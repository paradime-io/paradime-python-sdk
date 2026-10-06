import re
from typing import Any, List, Optional

# dbt-core writes freshness statuses `pass` / `warn` / `error` / `runtime error`. dbt 2.x writes
# its Rust enum names, `Pass` / `Warn` / `Error`, to sources.json (dbt-labs/dbt#16400) and, by
# contract, to freshness.json. The Paradime backend canonicalises them the same way.
_CAMEL_CASE_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])")
# unique_id prefixes whose run status is a test outcome (pass / warn / fail / error)
_TEST_UNIQUE_ID_PREFIXES = ("test.", "unit_test.")


def merge_run_results(run_results_list: List[dict]) -> dict:
    """
    Merges multiple run_results.json files into a single comprehensive result.
    This is useful when a schedule has multiple commands that generate run_results.json
    (e.g., dbt run and dbt test).

    Args:
        run_results_list (List[dict]): List of run_results.json dictionaries to merge.

    Returns:
        dict: Merged run_results.json with all results combined.
    """
    if not run_results_list:
        return {}

    if len(run_results_list) == 1:
        return run_results_list[0]

    # Use the metadata from the most recent run_results
    merged = {
        "metadata": run_results_list[0].get("metadata", {}),
        "results": [],
        "elapsed_time": sum(r.get("elapsed_time", 0) for r in run_results_list),
        "args": run_results_list[0].get("args", {}),
    }

    # Collect all results
    seen_results: dict[str, dict[str, Any]] = {}
    for run_results in run_results_list:
        for result in run_results.get("results", []):
            unique_id = result.get("unique_id")
            if unique_id:
                # If we've seen this result before, keep the one with latest timing
                if unique_id in seen_results:
                    existing_result = seen_results[unique_id]
                    existing_timing = existing_result.get("timing", [])
                    new_timing = result.get("timing", [])

                    # Compare completed_at timestamps if available
                    if (
                        new_timing
                        and existing_timing
                        and len(new_timing) > 0
                        and len(existing_timing) > 0
                        and new_timing[-1].get("completed_at", "")
                        > existing_timing[-1].get("completed_at", "")
                    ):
                        seen_results[unique_id] = result
                else:
                    seen_results[unique_id] = result

    merged["results"] = list(seen_results.values())
    return merged


def merge_sources(sources_list: List[dict]) -> dict:
    """
    Merges multiple sources.json files into a single comprehensive result.
    This is useful when a schedule has multiple commands that generate sources.json
    (e.g., multiple dbt runs with different source tables).

    Args:
        sources_list (List[dict]): List of sources.json dictionaries to merge.

    Returns:
        dict: Merged sources.json with all sources combined.
    """
    if not sources_list:
        return {}

    if len(sources_list) == 1:
        return sources_list[0]

    # Use the metadata from the most recent sources
    merged = {
        "metadata": sources_list[0].get("metadata", {}),
        "results": [],
        "elapsed_time": sum(s.get("elapsed_time", 0) for s in sources_list),
    }

    # Collect all sources, avoiding duplicates by unique_id
    seen_sources: dict[str, dict[str, Any]] = {}
    for sources in sources_list:
        for source in sources.get("results", []):
            unique_id = source.get("unique_id")
            if unique_id:
                # If we've seen this source before, keep the most recent one
                if unique_id in seen_sources:
                    existing_source = seen_sources[unique_id]
                    existing_timing = existing_source.get("timing", [])
                    new_timing = source.get("timing", [])

                    # Compare completed_at timestamps if available
                    if (
                        new_timing
                        and existing_timing
                        and len(new_timing) > 0
                        and len(existing_timing) > 0
                        and new_timing[-1].get("completed_at", "")
                        > existing_timing[-1].get("completed_at", "")
                    ):
                        seen_sources[unique_id] = source
                else:
                    seen_sources[unique_id] = source

    merged["results"] = list(seen_sources.values())
    return merged


def normalize_freshness_status(status: Optional[str]) -> Optional[str]:
    """`Pass` -> `pass`; `RuntimeError` / `runtime_error` -> `runtime error`; blank -> None."""
    if status is None:
        return None
    words = _CAMEL_CASE_BOUNDARY.sub(" ", status).replace("_", " ").replace("-", " ").split()
    return " ".join(words).lower() or None


def canonicalize_sources(sources: Any) -> Any:
    """A sources.json (or freshness.json) with dbt-core's lowercase statuses."""
    if not isinstance(sources, dict):
        return sources
    results = [
        (
            {**result, "status": normalize_freshness_status(result.get("status"))}
            if isinstance(result, dict)
            else result
        )
        for result in sources.get("results") or []
    ]
    return {**sources, "results": results}


def canonicalize_run_results(run_results: Any) -> Any:
    """A run_results.json with dbt-core's statuses: dbt 2.x writes `warn` for a model, seed or
    snapshot that built with a warning, where dbt-core wrote `success`. A test's `warn` stays."""
    if not isinstance(run_results, dict):
        return run_results
    results = [
        (
            {**result, "status": "success"}
            if isinstance(result, dict)
            and result.get("status") == "warn"
            and not str(result.get("unique_id") or "").startswith(_TEST_UNIQUE_ID_PREFIXES)
            else result
        )
        for result in run_results.get("results") or []
    ]
    return {**run_results, "results": results}
