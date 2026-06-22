import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Final, Optional, Tuple

import yaml  # type: ignore[import-untyped]

from paradime.cli import console

PARADIME_RESOURCES_DIRECTORY_ENV_VAR: Final = "PARADIME_RESOURCES_DIRECTORY"
DATAHUB_SERVER_ENV_VAR: Final = "DATAHUB_GMS_URL"
DATAHUB_TOKEN_ENV_VAR: Final = "DATAHUB_GMS_TOKEN"
DATAHUB_TARGET_PLATFORM_ENV_VAR: Final = "DATAHUB_TARGET_PLATFORM"
DATAHUB_DOMAIN_ENV_VAR: Final = "DATAHUB_DOMAIN"


def build_datahub_recipe(
    *,
    manifest_path: Path,
    catalog_path: Path,
    datahub_server: str,
    datahub_token: Optional[str] = None,
    target_platform: str,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build an acryl-datahub ingestion recipe that reads dbt artifacts and emits to
    DataHub's REST sink (GMS). This reuses DataHub's native ``dbt`` source so the
    output matches what a dbt Cloud / dbt Core ingestion produces.

    ``datahub_token`` is optional: DataHub Cloud (and any deployment with metadata
    service auth enabled) requires a personal access token, but a local/self-hosted
    instance with auth disabled accepts writes without one.

    When ``domain`` is provided, a ``simple_add_dataset_domain`` transformer is added
    so every emitted dataset is associated with that DataHub domain URN.
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
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": sink_config,
        },
    }

    if domain:
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


def push_artifacts_to_datahub(
    *,
    paradime_resources_directory: str,
    datahub_server: str,
    datahub_token: Optional[str] = None,
    target_platform: str,
    domain: Optional[str] = None,
) -> Tuple[bool, bool]:
    """
    Search the resources directory for dbt artifacts (``target/manifest.json`` and
    ``target/catalog.json``) and push them to DataHub for each project found.

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
    recipe = build_datahub_recipe(
        manifest_path=manifest_path,
        catalog_path=catalog_path,
        datahub_server=datahub_server,
        datahub_token=datahub_token,
        target_platform=target_platform,
        domain=domain,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_path = Path(tmpdir) / "datahub_recipe.yml"
        recipe_path.write_text(yaml.safe_dump(recipe, sort_keys=False))

        command = ["datahub", "ingest", "-c", str(recipe_path)]
        try:
            console.debug(f"Running datahub ingest command: {command!r}")
            result = subprocess.run(
                command, check=True, capture_output=True, text=True, env=os.environ
            )
            console.debug(f"datahub ingest result: {result.stdout} {result.stderr}")
        except FileNotFoundError:
            raise Exception(
                "The 'datahub' CLI was not found. acryl-datahub must be installed in the "
                "runtime environment (it is provided by the Paradime dbt base image)."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Error running datahub ingest: {e.stdout!r} {e.stderr!r}")
