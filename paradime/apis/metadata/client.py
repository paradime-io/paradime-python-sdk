import time
from datetime import datetime
from typing import Any, List, Optional

import polars as pl

from paradime.apis.bolt.client import BoltClient
from paradime.apis.bolt.exception import BoltScheduleArtifactNotFoundException

from .database import MetadataDatabase
from .parsers import ArtifactParser
from .types import (
    DependencyImpact,
    HealthDashboard,
    HealthStatus,
    MetadataResponse,
    ModelDependency,
    ModelHealth,
    PerformanceMetrics,
    ResourceType,
    SourceFreshness,
    TestResult,
)


class MetadataClient:
    """
    Client for querying dbt metadata.
    Provides model health monitoring, test results, source freshness, and dependency analysis.
    """

    def __init__(
        self, bolt_client: BoltClient, db_connection: str = ":memory:", cache_ttl_seconds: int = 300
    ):
        """
        Initialize MetadataClient.

        Args:
            bolt_client: Authenticated BoltClient instance
            db_connection: DuckDB connection string. Defaults to in-memory database.
            cache_ttl_seconds: Cache time-to-live in seconds (default 5 minutes)
        """
        self.bolt_client = bolt_client
        self.db = MetadataDatabase(db_connection)
        self.parser = ArtifactParser()
        self._loaded_schedules: set[str] = set()  # Track which schedules have been loaded
        self._cache_ttl = cache_ttl_seconds
        self._cache: dict[str, dict[str, Any]] = {}  # Simple in-memory cache
        self._dependency_cache: dict[str, dict[str, Any]] = (
            {}
        )  # Cache for computed dependency graphs

    def _ensure_metadata_loaded(self, schedule_name: str, force_refresh: bool = False) -> None:
        """
        Ensure metadata for a schedule is loaded into the database.

        Args:
            schedule_name: Name of the schedule
            force_refresh: If True, refresh data even if already loaded
        """
        if not force_refresh and schedule_name in self._loaded_schedules:
            return

        try:
            # Fetch all artifacts using the enhanced Bolt client
            artifacts = self.bolt_client.get_all_latest_artifacts(schedule_name)

            # Parse artifacts
            parsed = self.parser.parse_artifacts(artifacts, schedule_name)

            # Extract data for database loading
            run_results_data = self.parser.extract_run_results_data(parsed)
            source_data = self.parser.extract_source_freshness_data(parsed)
            model_metadata = self.parser.extract_model_metadata(parsed)
            seed_data = self.parser.extract_seed_data(parsed)
            snapshot_data = self.parser.extract_snapshot_data(parsed)
            test_data = self.parser.extract_test_data(parsed)
            exposure_data = self.parser.extract_exposure_data(parsed)

            # Enrich run results with manifest data
            if run_results_data and model_metadata:
                run_results_data = self.parser.enrich_run_results_with_manifest(
                    run_results_data, model_metadata
                )

            # Load into database
            self.db.load_run_results(run_results_data, schedule_name)
            self.db.load_source_freshness(source_data, schedule_name)
            self.db.load_model_metadata(model_metadata, schedule_name)
            self.db.load_seed_data(seed_data, schedule_name)
            self.db.load_snapshot_data(snapshot_data, schedule_name)
            self.db.load_test_data(test_data, schedule_name)
            self.db.load_exposure_data(exposure_data, schedule_name)

            # Track that this schedule is loaded
            self._loaded_schedules.add(schedule_name)

        except BoltScheduleArtifactNotFoundException as e:
            raise ValueError(f"Could not load metadata for schedule '{schedule_name}': {e}")

    def _get_cache_key(self, method_name: str, *args: Any) -> str:
        """Generate cache key for method calls"""
        return f"{method_name}:{':'.join(str(arg) for arg in args)}"

    def _is_cache_valid(self, cache_entry: dict[str, Any]) -> bool:
        """Check if cache entry is still valid"""
        return time.time() - cache_entry["timestamp"] < self._cache_ttl

    def _get_from_cache(self, cache_key: str) -> Any:
        """Get value from cache if valid"""
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if self._is_cache_valid(entry):
                return entry["data"]
            else:
                # Remove expired entry
                del self._cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any) -> None:
        """Set value in cache with timestamp"""
        self._cache[cache_key] = {"data": data, "timestamp": time.time()}

    def clear_cache(self, schedule_name: Optional[str] = None) -> None:
        """Clear cache entries, optionally for a specific schedule"""
        if schedule_name is None:
            self._cache.clear()
            self._dependency_cache.clear()
        else:
            # Clear entries that contain the schedule name
            keys_to_remove = [k for k in self._cache.keys() if schedule_name in k]
            for key in keys_to_remove:
                del self._cache[key]

            if schedule_name in self._dependency_cache:
                del self._dependency_cache[schedule_name]

    def refresh_metadata(self, schedule_name: str) -> None:
        """
        Force refresh metadata for a schedule.

        Args:
            schedule_name: Name of the schedule to refresh
        """
        self._ensure_metadata_loaded(schedule_name, force_refresh=True)

    def query_sql(
        self, sql: str, schedule_name: str, parameters: Optional[List[Any]] = None
    ) -> pl.DataFrame:
        """
        Execute raw SQL query against the metadata database.

        Args:
            sql: SQL query to execute
            schedule_name: Schedule name (ensures metadata is loaded)
            parameters: Optional query parameters

        Returns:
            DataFrame with query results
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.query_sql(sql, parameters)

    def get_model_health(self, schedule_name: str) -> List[ModelHealth]:
        """
        Get model health status for all models in a schedule.

        Args:
            schedule_name: Name of the schedule

        Returns:
            List of ModelHealth objects with status and test results
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_model_health(schedule_name)

    def get_test_results(self, schedule_name: str, failed_only: bool = False) -> List[TestResult]:
        """
        Get test results for a schedule.

        Args:
            schedule_name: Name of the schedule
            failed_only: If True, only return failed tests

        Returns:
            List of TestResult objects
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_test_results(schedule_name, failed_only)

    def get_source_freshness(self, schedule_name: str) -> List[SourceFreshness]:
        """
        Get source freshness results for a schedule.

        Args:
            schedule_name: Name of the schedule

        Returns:
            List of SourceFreshness objects
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_source_freshness(schedule_name)

    def get_seed_data(self, schedule_name: str) -> List[Any]:
        """
        Get seed information for a schedule.

        Args:
            schedule_name: Name of the schedule to query

        Returns:
            List of SeedData objects with comprehensive seed metadata
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_seed_data(schedule_name)

    def get_snapshot_data(self, schedule_name: str) -> List[Any]:
        """
        Get snapshot information for a schedule.

        Args:
            schedule_name: Name of the schedule to query

        Returns:
            List of SnapshotData objects with comprehensive snapshot metadata
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_snapshot_data(schedule_name)

    def get_test_data(self, schedule_name: str) -> List[Any]:
        """
        Get test information for a schedule.

        Args:
            schedule_name: Name of the schedule to query

        Returns:
            List of TestData objects with comprehensive test metadata
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_test_data(schedule_name)

    def get_exposure_data(self, schedule_name: str) -> List[Any]:
        """
        Get exposure information for a schedule.

        Args:
            schedule_name: Name of the schedule to query

        Returns:
            List of ExposureData objects with comprehensive exposure metadata
        """
        self._ensure_metadata_loaded(schedule_name)
        return self.db.get_exposure_data(schedule_name)

    def get_upstream_health(
        self, model_name: str, schedule_name: str, max_depth: int = 10
    ) -> List[ModelDependency]:
        """
        Get health status of all upstream dependencies for a model.

        Args:
            model_name: Name of the model to analyze
            schedule_name: Name of the schedule
            max_depth: Maximum depth to traverse dependencies

        Returns:
            List of ModelDependency objects representing upstream models and their health
        """
        self._ensure_metadata_loaded(schedule_name)

        df = self.db.get_upstream_dependencies(model_name, schedule_name, max_depth)

        dependencies = []
        for row in df.to_pandas().itertuples():
            # Determine health status
            health_status = None
            if row.health_status:
                health_status = HealthStatus(row.health_status)
            elif row.status:
                if row.status in ["error", "fail"]:
                    health_status = HealthStatus.CRITICAL
                elif row.status == "warn":
                    health_status = HealthStatus.WARNING
                else:
                    health_status = HealthStatus.HEALTHY

            dependencies.append(
                ModelDependency(
                    unique_id=row.unique_id,
                    name=row.name,
                    level=row.level,
                    resource_type=ResourceType(row.resource_type),
                    status=row.status,
                    execution_time=row.execution_time,
                    executed_at=row.executed_at,
                    health_status=health_status,
                )
            )

        return dependencies

    def get_downstream_impact(
        self, model_name: str, schedule_name: str, max_depth: int = 10
    ) -> DependencyImpact:
        """
        Analyze the downstream impact of a failed model.

        Args:
            model_name: Name of the failed model
            schedule_name: Name of the schedule
            max_depth: Maximum depth to traverse dependencies

        Returns:
            DependencyImpact object with affected models categorized by severity
        """
        self._ensure_metadata_loaded(schedule_name)

        df = self.db.get_downstream_impact(model_name, schedule_name, max_depth)
        results = [tuple(row) for row in df.to_pandas().itertuples(index=False)]

        return DependencyImpact.from_results(results, model_name)

    def get_health_dashboard(self, schedule_name: str) -> HealthDashboard:
        """
        Get comprehensive health dashboard metrics for a schedule using optimized SQL.

        Args:
            schedule_name: Name of the schedule

        Returns:
            HealthDashboard with aggregated health metrics
        """
        # Check cache first
        cache_key = self._get_cache_key("health_dashboard", schedule_name)
        cached_result = self._get_from_cache(cache_key)
        if cached_result is not None:
            return cached_result

        self._ensure_metadata_loaded(schedule_name)

        # Use optimized SQL aggregation for model metrics
        model_metrics = self.db.get_dashboard_metrics_optimized(schedule_name)

        # Get source freshness data (lighter operation)
        sources = self.get_source_freshness(schedule_name)
        sources_checked = len(sources)
        stale_sources = len([s for s in sources if s.freshness_status != "pass"])

        dashboard = HealthDashboard(
            schedule_name=schedule_name,
            total_models=model_metrics["total_models"],
            healthy_models=model_metrics["healthy_models"],
            warning_models=model_metrics["warning_models"],
            critical_models=model_metrics["critical_models"],
            avg_execution_time=model_metrics["avg_execution_time"],
            test_success_rate=model_metrics["test_success_rate"],
            total_tests=model_metrics["total_tests"],
            failed_tests=model_metrics["failed_tests"],
            sources_checked=sources_checked,
            stale_sources=stale_sources,
            last_updated=datetime.utcnow(),
        )

        # Cache the result
        self._set_cache(cache_key, dashboard)
        return dashboard

    def get_failing_models(self, schedule_name: str) -> List[ModelHealth]:
        """
        Get models that have failing tests.

        Args:
            schedule_name: Name of the schedule

        Returns:
            List of ModelHealth objects for models with failed tests
        """
        models = self.get_model_health(schedule_name)
        return [m for m in models if m.failed_tests > 0]

    # Keep backward compatibility
    def get_models_with_failing_tests(self, schedule_name: str) -> List[ModelHealth]:
        """Deprecated: Use get_failing_models instead"""
        return self.get_failing_models(schedule_name)

    def get_slowest_models(self, schedule_name: str, limit: int = 10) -> List[ModelHealth]:
        """
        Get the slowest running models.

        Args:
            schedule_name: Name of the schedule
            limit: Maximum number of models to return

        Returns:
            List of ModelHealth objects sorted by execution time (descending)
        """
        models = self.get_model_health(schedule_name)
        # Filter out models without execution time and sort by execution time
        models_with_time = [m for m in models if m.execution_time is not None]
        models_with_time.sort(key=lambda x: x.execution_time or 0.0, reverse=True)
        return models_with_time[:limit]

    def get_performance_summary(self, schedule_name: str, days: int = 7) -> PerformanceMetrics:
        """
        Get performance metrics for a schedule over a time period.
        Note: This is a basic implementation. For historical trends, you'd need
        to store multiple runs of data.

        Args:
            schedule_name: Name of the schedule
            days: Number of days to analyze (placeholder for future enhancement)

        Returns:
            PerformanceMetrics object
        """
        models = self.get_model_health(schedule_name)

        # Calculate current metrics (could be enhanced with historical data)
        slowest_models = [
            {"name": m.name, "execution_time": m.execution_time, "status": m.status}
            for m in self.get_slowest_models(schedule_name, 10)
        ]

        total_models = len(models)
        successful_models = len([m for m in models if m.status == "success"])
        models_with_time = [m for m in models if m.execution_time]
        avg_execution_time = (
            sum(m.execution_time or 0.0 for m in models_with_time) / len(models_with_time)
            if models_with_time
            else 0.0
        )
        success_rate = (successful_models * 100.0 / total_models) if total_models > 0 else 0.0

        return PerformanceMetrics(
            schedule_name=schedule_name,
            time_period_days=days,
            slowest_models=slowest_models,
            average_execution_time=avg_execution_time,
            total_runs=total_models,  # Using models as proxy for runs
            success_rate=success_rate,
            performance_trend=[],  # Would need historical data
        )

    # Keep backward compatibility
    def get_performance_metrics(self, schedule_name: str, days: int = 7) -> PerformanceMetrics:
        """Deprecated: Use get_performance_summary instead"""
        return self.get_performance_summary(schedule_name, days)

    # Keep backward compatibility
    def get_upstream_model_health(
        self, model_name: str, schedule_name: str, max_depth: int = 10
    ) -> List[ModelDependency]:
        """Deprecated: Use get_upstream_health instead"""
        return self.get_upstream_health(model_name, schedule_name, max_depth)

    def query(
        self,
        schedule_name: str,
        include_models: bool = True,
        include_tests: bool = True,
        include_sources: bool = True,
    ) -> MetadataResponse:
        """
        Get comprehensive metadata response for a schedule.

        Args:
            schedule_name: Name of the schedule
            include_models: Include model health data
            include_tests: Include test results
            include_sources: Include source freshness data

        Returns:
            MetadataResponse with requested data
        """
        response = MetadataResponse(schedule_name=schedule_name)

        if include_models:
            response.models = self.get_model_health(schedule_name)

        if include_tests:
            response.tests = self.get_test_results(schedule_name)  # type: ignore

        if include_sources:
            response.sources = self.get_source_freshness(schedule_name)

        return response

    def get_model_health_stream(self, schedule_name: str, batch_size: int = 100) -> Any:
        """
        Stream model health data in batches to handle large datasets efficiently.

        Args:
            schedule_name: Name of the schedule
            batch_size: Number of records per batch

        Yields:
            List of ModelHealth objects in batches
        """
        self._ensure_metadata_loaded(schedule_name)

        # Use SQL pagination for memory-efficient streaming
        offset = 0
        while True:
            sql = """
                SELECT * FROM latest_model_results
                WHERE schedule_name = ?
                ORDER BY executed_at DESC, unique_id
                LIMIT ? OFFSET ?
            """

            df = pl.from_pandas(self.db.conn.execute(sql, [schedule_name, batch_size, offset]).df())
            if df.is_empty():
                break

            # Convert to ModelHealth objects
            batch_models = []
            for row in df.to_pandas().itertuples():
                model_health = ModelHealth(
                    unique_id=row["unique_id"],
                    name=row["name"],
                    resource_type=ResourceType(row["resource_type"]),
                    status=row["status"],
                    execution_time=row["execution_time"],
                    executed_at=row["executed_at"],
                    health_status=HealthStatus(row["health_status"]),
                    total_tests=row["total_tests"],
                    failed_tests=row["failed_tests"],
                    depends_on=row["depends_on"] or [],
                    schema_name=row["schema_name"],
                    database_name=row["database_name"],
                    error_message=row["error_message"],
                    alias=row["alias"],
                    materialized_type=row["materialized_type"],
                    description=row["description"],
                    meta=row["meta"] if row["meta"] else {},
                    tags=row["tags"] if row["tags"] else [],
                    owner=row["owner"],
                    package_name=row["package_name"],
                    language=row["language"],
                    access=row["access"],
                    compiled_sql=row["compiled_sql"],
                    raw_sql=row["raw_sql"],
                    columns=row["columns"] if row["columns"] else {},
                    children_l1=row["children_l1"] if row["children_l1"] else [],
                    parents_models=row["parents_models"] if row["parents_models"] else [],
                    parents_sources=row["parents_sources"] if row["parents_sources"] else [],
                )
                batch_models.append(model_health)

            yield batch_models

            offset += batch_size
            if len(batch_models) < batch_size:
                break

    @property
    def dependency_graph(self) -> dict[str, Any]:
        """Lazy-loaded dependency graph property"""
        if not hasattr(self, "_dependency_graph"):
            self._dependency_graph: dict[str, Any] = {}
        return self._dependency_graph

    def get_dependency_graph_cached(self, schedule_name: str) -> dict[str, dict[str, Any]]:
        """Get or compute dependency graph with caching"""
        if schedule_name not in self._dependency_cache:
            self._ensure_metadata_loaded(schedule_name)

            # Build dependency graph from database
            sql = """
                SELECT unique_id, name, depends_on, children_l1 as children
                FROM model_metadata
                WHERE schedule_name = ?
            """
            df = pl.from_pandas(self.db.conn.execute(sql, [schedule_name]).df())

            graph = {}
            for row in df.to_pandas().itertuples():
                graph[row["unique_id"]] = {
                    "name": row["name"],
                    "parents": row["depends_on"] if row["depends_on"] else [],
                    "children": row["children"] if row["children"] else [],
                }

            self._dependency_cache[schedule_name] = graph

        return self._dependency_cache[schedule_name]

    def close(self) -> None:
        """Close database connection"""
        self.db.close()
