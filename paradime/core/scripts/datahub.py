import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Final, Iterator, List, Optional, Tuple

import yaml  # type: ignore[import-untyped]

from paradime.cli import console

PARADIME_RESOURCES_DIRECTORY_ENV_VAR: Final = "PARADIME_RESOURCES_DIRECTORY"
DATAHUB_SERVER_ENV_VAR: Final = "DATAHUB_GMS_URL"
DATAHUB_TOKEN_ENV_VAR: Final = "DATAHUB_GMS_TOKEN"
DATAHUB_TARGET_PLATFORM_ENV_VAR: Final = "DATAHUB_TARGET_PLATFORM"
DATAHUB_DOMAIN_ENV_VAR: Final = "DATAHUB_DOMAIN"
DATAHUB_GLOSSARY_PATH_ENV_VAR: Final = "DATAHUB_GLOSSARY_PATH"

DEFAULT_GLOSSARY_GLOBS: Final = ("**/datahub_glossary*.yml", "**/datahub_glossary*.yaml")

# dbt resource types that DataHub's dbt source emits as datasets. Only these can
# carry a domain, so only these participate in the fallback-domain logic below.
_DATASET_RESOURCE_TYPES: Final = frozenset({"model", "seed", "snapshot", "source"})

# DataHub's dbt source only runs its column-level meta processor when
# `column_meta_mapping` is non-empty. The processor natively understands the
# special `datahub` meta property (tags/terms/owners) independently of the
# configured directives, so a directive that can never match is enough to
# unlock `meta: datahub: {...}` on columns without mapping anything itself.
_COLUMN_META_MAPPING_ACTIVATOR: Final = {
    "paradime__enable_column_meta": {
        "match": "a^",
        "operation": "add_tag",
        "config": {"tag": "unused"},
    }
}


def build_datahub_recipe(
    *,
    manifest_path: Path,
    catalog_path: Path,
    datahub_server: str,
    datahub_token: Optional[str] = None,
    target_platform: str,
    domain: Optional[str] = None,
    has_per_entity_domains: bool = False,
) -> Dict[str, Any]:
    """
    Build an acryl-datahub ingestion recipe that reads dbt artifacts and emits to
    DataHub's REST sink (GMS). This reuses DataHub's native ``dbt`` source so the
    output matches what a dbt Cloud / dbt Core ingestion produces.

    ``datahub_token`` is optional: DataHub Cloud (and any deployment with metadata
    service auth enabled) requires a personal access token, but a local/self-hosted
    instance with auth disabled accepts writes without one.

    Models, sources and columns can carry DataHub metadata in their dbt ``meta``
    under the ``datahub`` key (DataHub's native convention), e.g.
    ``meta: {datahub: {domain: "urn:li:domain:x", terms: [Account], tags: [pii]}}``.

    When ``domain`` is provided and no entity sets its own ``meta.datahub.domain``
    (``has_per_entity_domains=False``), a ``simple_add_dataset_domain`` transformer
    is added so every emitted dataset is associated with that DataHub domain URN.
    When entities do set their own domains, the transformer must be omitted — it
    runs after the source and would overwrite the per-entity domains. The
    workspace domain is then applied as a per-entity fallback by rewriting the
    manifest instead (see ``inject_fallback_domain``).
    """
    sink_config: Dict[str, Any] = {"server": datahub_server}
    if datahub_token:
        sink_config["token"] = datahub_token

    recipe: Dict[str, Any] = {
        "source": {
            "type": "dbt",
            "config": {
                "manifest_path": str(manifest_path),
                "catalog_path": str(catalog_path),
                "target_platform": target_platform,
                "column_meta_mapping": _COLUMN_META_MAPPING_ACTIVATOR,
                # The default ("PATCH") merges tags/terms/owners with existing
                # server state, so associations removed from dbt yml would linger
                # in DataHub forever. OVERRIDE makes the yml the source of truth.
                "write_semantics": "OVERRIDE",
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": sink_config,
        },
    }

    if domain and not has_per_entity_domains:
        recipe["transformers"] = [
            {
                "type": "simple_add_dataset_domain",
                "config": {
                    "semantics": "OVERWRITE",
                    "domains": [domain],
                },
            }
        ]

    return recipe


def build_glossary_recipe(
    *,
    glossary_path: Path,
    datahub_server: str,
    datahub_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a recipe that ingests a business glossary YAML file (DataHub's native
    ``datahub-business-glossary`` source format: version/source/owners/nodes/terms).
    """
    sink_config: Dict[str, Any] = {"server": datahub_server}
    if datahub_token:
        sink_config["token"] = datahub_token

    return {
        "source": {
            "type": "datahub-business-glossary",
            "config": {"file": str(glossary_path)},
        },
        "sink": {
            "type": "datahub-rest",
            "config": sink_config,
        },
    }


def _dataset_entities(manifest: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    for entity in manifest.get("nodes", {}).values():
        if entity.get("resource_type") in _DATASET_RESOURCE_TYPES:
            yield entity
    for entity in manifest.get("sources", {}).values():
        yield entity


def _effective_meta(entity: Dict[str, Any]) -> Dict[str, Any]:
    # Mirrors DataHub's extraction: top-level meta wins; config.meta is only
    # read when top-level meta is empty (dbt >=1.10 writes `config: meta:`
    # there, though it also mirrors the merged value into node.meta).
    meta = entity.get("meta") or {}
    if not meta:
        meta = (entity.get("config") or {}).get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _has_own_domain(entity: Dict[str, Any]) -> bool:
    datahub_meta = _effective_meta(entity).get("datahub")
    return isinstance(datahub_meta, dict) and bool(datahub_meta.get("domain"))


def manifest_has_per_entity_domains(manifest_path: Path) -> bool:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return any(_has_own_domain(entity) for entity in _dataset_entities(manifest))


def inject_fallback_domain(manifest: Dict[str, Any], domain: str) -> int:
    """
    Set ``meta.datahub.domain = domain`` on every dataset entity that doesn't
    declare its own, so the workspace-level domain acts as a per-entity fallback
    flowing through the same native meta handling as author-set domains.

    Mutates ``manifest`` in place and returns the number of entities updated.
    """
    updated = 0
    for entity in _dataset_entities(manifest):
        if _has_own_domain(entity):
            continue
        # Start from the effective meta so injecting into top-level meta doesn't
        # mask a `config: meta:` block that DataHub would otherwise fall back to.
        meta = dict(_effective_meta(entity))
        datahub_meta = meta.get("datahub")
        if datahub_meta is not None and not isinstance(datahub_meta, dict):
            continue
        meta["datahub"] = {**(datahub_meta or {}), "domain": domain}
        entity["meta"] = meta
        updated += 1
    return updated


def find_glossary_files(
    resources_directory: str,
    glossary_path: Optional[str] = None,
) -> List[Path]:
    """
    Find business glossary YAML files under the resources directory.

    ``glossary_path`` is a comma-separated list of globs relative to the
    resources directory (e.g. ``metadata/glossary_terms/**/*.yaml``); when not
    provided, any file matching ``datahub_glossary*.yml``/``.yaml`` anywhere in
    the tree is picked up.
    """
    base = Path(resources_directory)
    if glossary_path:
        patterns = [p.strip() for p in glossary_path.split(",") if p.strip()]
    else:
        patterns = list(DEFAULT_GLOSSARY_GLOBS)

    return sorted({f for pattern in patterns for f in base.glob(pattern) if f.is_file()})


def push_artifacts_to_datahub(
    *,
    paradime_resources_directory: str,
    datahub_server: str,
    datahub_token: Optional[str] = None,
    target_platform: str,
    domain: Optional[str] = None,
    glossary_path: Optional[str] = None,
) -> Tuple[bool, bool]:
    """
    Search the resources directory for dbt artifacts (``target/manifest.json`` and
    ``target/catalog.json``) and push them to DataHub for each project found, then
    ingest any business glossary YAML files committed in the repo.

    Returns ``(success, found_files)`` mirroring the other artifact-based integrations.
    """
    success, found_files = True, False
    for root, _dirs, _files in os.walk(paradime_resources_directory):
        # DataHub's dbt source needs both the manifest and the catalog. The catalog is
        # only produced by `dbt docs generate`, so a manifest without a catalog is skipped.
        manifest_path = Path(root) / "target" / "manifest.json"
        catalog_path = Path(root) / "target" / "catalog.json"

        if not manifest_path.is_file():
            continue

        if not catalog_path.is_file():
            console.warning(
                f"Found {manifest_path} but no catalog.json alongside it. "
                "Run `dbt docs generate` in the schedule so target/catalog.json is produced. Skipping."
            )
            continue

        found_files = True

        try:
            _run_datahub_ingestion(
                manifest_path=manifest_path,
                catalog_path=catalog_path,
                datahub_server=datahub_server,
                datahub_token=datahub_token,
                target_platform=target_platform,
                domain=domain,
            )
        except Exception as e:
            console.error(f"Error pushing artifacts to DataHub: {e!r}")
            success = False

    glossary_files = find_glossary_files(paradime_resources_directory, glossary_path)
    for glossary_file in glossary_files:
        found_files = True
        try:
            _run_glossary_ingestion(
                glossary_path=glossary_file,
                datahub_server=datahub_server,
                datahub_token=datahub_token,
            )
        except Exception as e:
            console.error(f"Error pushing glossary {glossary_file} to DataHub: {e!r}")
            success = False

    return success, found_files


def _run_datahub_ingestion(
    *,
    manifest_path: Path,
    catalog_path: Path,
    datahub_server: str,
    datahub_token: Optional[str],
    target_platform: str,
    domain: Optional[str],
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        has_per_entity_domains = domain is not None and manifest_has_per_entity_domains(
            manifest_path
        )
        if domain and has_per_entity_domains:
            # Per-entity meta domains would be overwritten by the domain
            # transformer, so apply the workspace domain as a fallback by
            # rewriting a copy of the manifest instead.
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            updated = inject_fallback_domain(manifest, domain)
            console.debug(f"Applied fallback domain to {updated} entities")
            manifest_path = Path(tmpdir) / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        recipe = build_datahub_recipe(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            datahub_server=datahub_server,
            datahub_token=datahub_token,
            target_platform=target_platform,
            domain=domain,
            has_per_entity_domains=has_per_entity_domains,
        )
        _run_datahub_ingest_command(recipe, tmpdir)


def _run_glossary_ingestion(
    *,
    glossary_path: Path,
    datahub_server: str,
    datahub_token: Optional[str],
) -> None:
    recipe = build_glossary_recipe(
        glossary_path=glossary_path,
        datahub_server=datahub_server,
        datahub_token=datahub_token,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        _run_datahub_ingest_command(recipe, tmpdir)


def _run_datahub_ingest_command(recipe: Dict[str, Any], tmpdir: str) -> None:
    recipe_path = Path(tmpdir) / "datahub_recipe.yml"
    recipe_path.write_text(yaml.safe_dump(recipe, sort_keys=False))

    command = ["datahub", "ingest", "-c", str(recipe_path)]
    try:
        console.debug(f"Running datahub ingest command: {command!r}")
        result = subprocess.run(command, check=True, capture_output=True, text=True, env=os.environ)
        console.debug(f"datahub ingest result: {result.stdout} {result.stderr}")
    except FileNotFoundError:
        raise Exception(
            "The 'datahub' CLI was not found. acryl-datahub must be installed in the "
            "runtime environment (it is provided by the Paradime dbt base image)."
        )
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error running datahub ingest: {e.stdout!r} {e.stderr!r}")
