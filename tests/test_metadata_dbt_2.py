import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock

import pytest

from paradime.apis.metadata.client import MetadataClient
from paradime.apis.metadata.utils import (
    canonicalize_run_results,
    canonicalize_sources,
    normalize_freshness_status,
)

# The same DuckDB project built by dbt 1.11.15 and by dbt 2.0.5 (Paradime's `2.0-latest`):
# `raw_orders` warns on freshness and `raw_customers` passes.
FIXTURES = Path(__file__).parent / "fixtures" / "metadata"


def _artifacts(version: str) -> Dict[str, Any]:
    return {
        name: json.loads((FIXTURES / f"{name}_{version}.json").read_text())
        for name in ("manifest", "run_results_build", "sources")
    }


def _client(version: str) -> MetadataClient:
    artifacts = _artifacts(version)
    bolt_client = Mock()
    bolt_client._get_all_latest_artifacts.return_value = {
        "manifest": artifacts["manifest"],
        "run_results": artifacts["run_results_build"],
        "sources": artifacts["sources"],
    }
    return MetadataClient(bolt_client)


@pytest.mark.parametrize(
    "status, expected",
    [
        ("pass", "pass"),
        ("Pass", "pass"),
        ("Warn", "warn"),
        ("Error", "error"),
        ("RuntimeError", "runtime error"),
        ("runtime error", "runtime error"),
        ("", None),
        (None, None),
    ],
)
def test_normalize_freshness_status(status: Any, expected: Any) -> None:
    assert normalize_freshness_status(status) == expected


def test_dbt_2_sources_json_reads_like_dbt_1() -> None:
    sources = canonicalize_sources(_artifacts("dbt_2_0_5")["sources"])

    assert sorted((r["unique_id"], r["status"]) for r in sources["results"]) == [
        ("source.probe.raw.raw_customers", "pass"),
        ("source.probe.raw.raw_orders", "warn"),
    ]


def test_a_model_built_with_a_warning_reads_as_success_but_a_warning_test_stays() -> None:
    run_results = canonicalize_run_results(
        {
            "results": [
                {"unique_id": "model.p.orders", "status": "warn"},
                {"unique_id": "seed.p.raw", "status": "warn"},
                {"unique_id": "test.p.positive.1a", "status": "warn"},
                {"unique_id": "unit_test.p.orders.t", "status": "warn"},
            ]
        }
    )

    assert [r["status"] for r in run_results["results"]] == ["success", "success", "warn", "warn"]


def test_health_dashboard_gives_the_same_answer_on_dbt_1_and_dbt_2() -> None:
    dbt_1 = _client("dbt_1_11_15").get_health_dashboard("probe")
    dbt_2 = _client("dbt_2_0_5").get_health_dashboard("probe")

    assert (dbt_2.sources_checked, dbt_2.stale_sources) == (2, 1)
    assert (dbt_1.sources_checked, dbt_1.stale_sources) == (2, 1)


def test_source_freshness_statuses_are_lowercase_on_dbt_2() -> None:
    sources = _client("dbt_2_0_5").get_source_freshness("probe")

    assert sorted((s.unique_id, s.freshness_status.value) for s in sources) == [
        ("source.probe.raw.raw_customers", "pass"),
        ("source.probe.raw.raw_orders", "warn"),
    ]
