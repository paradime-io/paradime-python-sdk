import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

from paradime.core.scripts.datahub import (
    build_datahub_recipe,
    build_glossary_recipe,
    find_glossary_files,
    inject_fallback_domain,
    manifest_has_per_entity_domains,
    push_artifacts_to_datahub,
)

DOMAIN = "urn:li:domain:workspace"


def _manifest(nodes: Dict[str, Any], sources: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {"nodes": nodes, "sources": sources or {}}


def _model(name: str, meta: Dict[str, Any] | None = None, **kwargs: Any) -> Dict[str, Any]:
    return {"resource_type": "model", "name": name, "meta": meta or {}, **kwargs}


# --- build_datahub_recipe ---


def test_recipe_without_domain_has_no_transformers() -> None:
    recipe = build_datahub_recipe(
        manifest_path=Path("manifest.json"),
        catalog_path=Path("catalog.json"),
        datahub_server="https://gms.example.com",
        target_platform="snowflake",
    )

    assert "transformers" not in recipe
    assert recipe["source"]["config"]["target_platform"] == "snowflake"
    # PATCH is the non-destructive default (matches DataHub's own default and
    # today's production behavior); OVERRIDE is opt-in via DATAHUB_WRITE_SEMANTICS.
    assert recipe["source"]["config"]["write_semantics"] == "PATCH"
    assert "token" not in recipe["sink"]["config"]


def test_recipe_write_semantics_override() -> None:
    recipe = build_datahub_recipe(
        manifest_path=Path("manifest.json"),
        catalog_path=Path("catalog.json"),
        datahub_server="https://gms.example.com",
        target_platform="snowflake",
        write_semantics="OVERRIDE",
    )

    assert recipe["source"]["config"]["write_semantics"] == "OVERRIDE"


def test_push_rejects_invalid_write_semantics(tmp_path: Path) -> None:
    import pytest

    with pytest.raises(ValueError, match="PATCH or OVERRIDE"):
        push_artifacts_to_datahub(
            paradime_resources_directory=str(tmp_path),
            datahub_server="https://gms.example.com",
            target_platform="snowflake",
            write_semantics="MERGE",
        )


def test_recipe_enables_column_meta_mapping() -> None:
    recipe = build_datahub_recipe(
        manifest_path=Path("manifest.json"),
        catalog_path=Path("catalog.json"),
        datahub_server="https://gms.example.com",
        target_platform="snowflake",
    )

    # A non-empty column_meta_mapping is required for DataHub to process the
    # native `datahub` property in column meta; the directive itself must never
    # match a real meta key.
    column_mapping = recipe["source"]["config"]["column_meta_mapping"]
    assert column_mapping
    for directive in column_mapping.values():
        assert directive["match"] == "a^"


def test_recipe_with_domain_and_no_per_entity_domains_keeps_transformer() -> None:
    recipe = build_datahub_recipe(
        manifest_path=Path("manifest.json"),
        catalog_path=Path("catalog.json"),
        datahub_server="https://gms.example.com",
        datahub_token="secret",
        target_platform="snowflake",
        domain=DOMAIN,
    )

    assert recipe["transformers"] == [
        {
            "type": "simple_add_dataset_domain",
            "config": {"semantics": "OVERWRITE", "domains": [DOMAIN]},
        }
    ]
    assert recipe["sink"]["config"]["token"] == "secret"


def test_recipe_with_per_entity_domains_drops_transformer() -> None:
    recipe = build_datahub_recipe(
        manifest_path=Path("manifest.json"),
        catalog_path=Path("catalog.json"),
        datahub_server="https://gms.example.com",
        target_platform="snowflake",
        domain=DOMAIN,
        has_per_entity_domains=True,
    )

    assert "transformers" not in recipe


# --- manifest scanning ---


def test_manifest_scan_detects_meta_domain(tmp_path: Path) -> None:
    manifest = _manifest(
        {"model.p.a": _model("a", meta={"datahub": {"domain": "urn:li:domain:x"}})}
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    assert manifest_has_per_entity_domains(manifest_path)


def test_manifest_scan_detects_config_meta_domain(tmp_path: Path) -> None:
    manifest = _manifest(
        {"model.p.a": _model("a", config={"meta": {"datahub": {"domain": "urn:li:domain:x"}}})}
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    assert manifest_has_per_entity_domains(manifest_path)


def test_manifest_scan_ignores_tests_and_plain_meta(tmp_path: Path) -> None:
    manifest = _manifest(
        {
            "model.p.a": _model("a", meta={"owner": "someone"}),
            "test.p.t": {
                "resource_type": "test",
                "meta": {"datahub": {"domain": "urn:li:domain:x"}},
            },
        }
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    assert not manifest_has_per_entity_domains(manifest_path)


# --- fallback domain injection ---


def test_inject_fallback_domain_fills_only_entities_without_domain() -> None:
    manifest = _manifest(
        {
            "model.p.a": _model("a", meta={"datahub": {"domain": "urn:li:domain:own"}}),
            "model.p.b": _model("b"),
            "test.p.t": {"resource_type": "test", "meta": {}},
        },
        sources={"source.p.s.t": {"resource_type": "source", "name": "t", "meta": {}}},
    )

    updated = inject_fallback_domain(manifest, DOMAIN)

    assert updated == 2  # model b + the source; model a keeps its own, test skipped
    assert manifest["nodes"]["model.p.a"]["meta"]["datahub"]["domain"] == "urn:li:domain:own"
    assert manifest["nodes"]["model.p.b"]["meta"]["datahub"]["domain"] == DOMAIN
    assert manifest["sources"]["source.p.s.t"]["meta"]["datahub"]["domain"] == DOMAIN
    assert "datahub" not in manifest["nodes"]["test.p.t"]["meta"]


def test_inject_fallback_domain_preserves_existing_meta_keys() -> None:
    manifest = _manifest(
        {"model.p.a": _model("a", meta={"owner": "x", "datahub": {"terms": ["Account"]}})}
    )

    inject_fallback_domain(manifest, DOMAIN)

    meta = manifest["nodes"]["model.p.a"]["meta"]
    assert meta["owner"] == "x"
    assert meta["datahub"] == {"terms": ["Account"], "domain": DOMAIN}


def test_inject_fallback_domain_merges_config_meta() -> None:
    # DataHub only reads config.meta when top-level meta is empty. Injecting into
    # top-level meta must therefore carry the config.meta content along, or it
    # would be masked.
    manifest = _manifest(
        {"model.p.a": _model("a", meta={}, config={"meta": {"datahub": {"tags": ["pii"]}}})}
    )

    inject_fallback_domain(manifest, DOMAIN)

    meta = manifest["nodes"]["model.p.a"]["meta"]
    assert meta["datahub"] == {"tags": ["pii"], "domain": DOMAIN}


# --- glossary discovery ---


def test_find_glossary_files_default_convention(tmp_path: Path) -> None:
    (tmp_path / "datahub_glossary.yml").write_text("version: '1'")
    nested = tmp_path / "project" / "governance"
    nested.mkdir(parents=True)
    (nested / "datahub_glossary_billing.yaml").write_text("version: '1'")
    (tmp_path / "unrelated.yml").write_text("nope")

    files = find_glossary_files(str(tmp_path))

    assert [f.name for f in files] == ["datahub_glossary.yml", "datahub_glossary_billing.yaml"]


def test_find_glossary_files_custom_globs(tmp_path: Path) -> None:
    terms = tmp_path / "metadata" / "glossary_terms" / "consumption_domain"
    terms.mkdir(parents=True)
    (terms / "consumption_glossary_terms.yaml").write_text("version: '1'")
    other = tmp_path / "other"
    other.mkdir()
    (other / "extra.yml").write_text("version: '1'")

    files = find_glossary_files(str(tmp_path), "metadata/glossary_terms/**/*.yaml, other/*.yml")

    assert [f.name for f in files] == ["consumption_glossary_terms.yaml", "extra.yml"]


def test_build_glossary_recipe() -> None:
    recipe = build_glossary_recipe(
        glossary_path=Path("glossary.yml"),
        datahub_server="https://gms.example.com",
        datahub_token="secret",
    )

    assert recipe["source"] == {
        "type": "datahub-business-glossary",
        "config": {"file": "glossary.yml"},
    }
    assert recipe["sink"]["config"] == {"server": "https://gms.example.com", "token": "secret"}


# --- end to end (subprocess mocked) ---


def _write_project(tmp_path: Path, manifest: Dict[str, Any]) -> None:
    target = tmp_path / "project" / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "catalog.json").write_text("{}")


def _run_push(tmp_path: Path, **kwargs: Any) -> tuple[bool, bool, List[Dict[str, Any]]]:
    recipes: List[Dict[str, Any]] = []

    def fake_run(command: list[str], **_: Any) -> Any:
        import yaml

        recipe = yaml.safe_load(Path(command[3]).read_text())
        # Snapshot the manifest while it still exists (patched copies live in a
        # TemporaryDirectory that is cleaned up right after the run).
        manifest_path = recipe["source"]["config"].get("manifest_path")
        if manifest_path:
            recipe["_manifest"] = json.loads(Path(manifest_path).read_text())
        recipes.append(recipe)

        class Result:
            stdout = ""
            stderr = ""

        return Result()

    with patch("paradime.core.scripts.datahub.subprocess.run", side_effect=fake_run):
        success, found_files = push_artifacts_to_datahub(
            paradime_resources_directory=str(tmp_path),
            datahub_server="https://gms.example.com",
            target_platform="snowflake",
            **kwargs,
        )
    return success, found_files, recipes


def test_push_without_per_entity_domains_matches_legacy_behavior(tmp_path: Path) -> None:
    _write_project(tmp_path, _manifest({"model.p.a": _model("a")}))

    success, found_files, recipes = _run_push(tmp_path, domain=DOMAIN)

    assert success and found_files
    assert len(recipes) == 1
    assert recipes[0]["transformers"][0]["config"]["domains"] == [DOMAIN]
    # The original manifest is used untouched.
    assert recipes[0]["source"]["config"]["manifest_path"].endswith("project/target/manifest.json")


def test_push_with_per_entity_domains_uses_patched_manifest(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        _manifest(
            {
                "model.p.a": _model("a", meta={"datahub": {"domain": "urn:li:domain:own"}}),
                "model.p.b": _model("b"),
            }
        ),
    )

    success, _, recipes = _run_push(tmp_path, domain=DOMAIN)

    assert success
    assert "transformers" not in recipes[0]
    # The recipe points at a rewritten manifest with the fallback domain injected.
    patched_path = recipes[0]["source"]["config"]["manifest_path"]
    assert not patched_path.endswith("project/target/manifest.json")
    patched_nodes = recipes[0]["_manifest"]["nodes"]
    assert patched_nodes["model.p.a"]["meta"]["datahub"]["domain"] == "urn:li:domain:own"
    assert patched_nodes["model.p.b"]["meta"]["datahub"]["domain"] == DOMAIN


def test_push_ingests_glossary_files(tmp_path: Path) -> None:
    _write_project(tmp_path, _manifest({"model.p.a": _model("a")}))
    (tmp_path / "datahub_glossary.yml").write_text("version: '1'")

    success, found_files, recipes = _run_push(tmp_path)

    assert success and found_files
    assert len(recipes) == 2
    assert recipes[1]["source"]["type"] == "datahub-business-glossary"
    assert recipes[1]["source"]["config"]["file"].endswith("datahub_glossary.yml")


def test_push_glossary_only_counts_as_found(tmp_path: Path) -> None:
    (tmp_path / "datahub_glossary.yml").write_text("version: '1'")

    success, found_files, recipes = _run_push(tmp_path)

    assert success and found_files
    assert len(recipes) == 1
    assert recipes[0]["source"]["type"] == "datahub-business-glossary"


def test_push_finds_nothing(tmp_path: Path) -> None:
    success, found_files, recipes = _run_push(tmp_path)

    assert success
    assert not found_files
    assert recipes == []
