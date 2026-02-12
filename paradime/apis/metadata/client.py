from typing import List, Optional, Dict, Any
import pandas as pd
from datetime import datetime

from paradime.apis.bolt.client import BoltClient
from paradime.apis.bolt.exception import BoltScheduleArtifactNotFoundException

from .database import MetadataDatabase
from .parsers import ArtifactParser
from .types import (
    ModelHealth,
    TestResult,
    SourceFreshness,
    ModelDependency,
    HealthDashboard,
    DependencyImpact,
    PerformanceMetrics,
    ParsedArtifacts,
    MetadataResponse,
    ResourceType,
    HealthStatus
)


class MetadataClient:
    """
    Client for querying dbt metadata with Discovery API feature parity.
    Provides model health monitoring, test results, source freshness, and dependency analysis.
    """
    
    def __init__(self, bolt_client: BoltClient, db_connection: str = ":memory:"):
        """
        Initialize MetadataClient.
        
        Args:
            bolt_client: Authenticated BoltClient instance
            db_connection: DuckDB connection string. Defaults to in-memory database.
        """
        self.bolt_client = bolt_client
        self.db = MetadataDatabase(db_connection)
        self.parser = ArtifactParser()
        self._loaded_schedules = set()  # Track which schedules have been loaded
    
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
            
            # Enrich run results with manifest data
            if run_results_data and model_metadata:
                run_results_data = self.parser.enrich_run_results_with_manifest(
                    run_results_data, model_metadata
                )
            
            # Load into database
            self.db.load_run_results(run_results_data, schedule_name)
            self.db.load_source_freshness(source_data, schedule_name)
            self.db.load_model_metadata(model_metadata, schedule_name)
            
            # Track that this schedule is loaded
            self._loaded_schedules.add(schedule_name)
            
        except BoltScheduleArtifactNotFoundException as e:
            raise ValueError(f"Could not load metadata for schedule '{schedule_name}': {e}")
    
    def refresh_metadata(self, schedule_name: str) -> None:
        """
        Force refresh metadata for a schedule.
        
        Args:
            schedule_name: Name of the schedule to refresh
        """
        self._ensure_metadata_loaded(schedule_name, force_refresh=True)
    
    def query_sql(self, sql: str, schedule_name: str, parameters: Optional[List[Any]] = None) -> pd.DataFrame:
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
    
    def get_upstream_model_health(self, model_name: str, schedule_name: str, max_depth: int = 10) -> List[ModelDependency]:
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
        for row in df.itertuples(index=False):
            # Determine health status
            health_status = None
            if row.health_status:
                health_status = HealthStatus(row.health_status)
            elif row.status:
                if row.status in ['error', 'fail']:
                    health_status = HealthStatus.CRITICAL
                elif row.status == 'warn':
                    health_status = HealthStatus.WARNING
                else:
                    health_status = HealthStatus.HEALTHY
            
            dependencies.append(ModelDependency(
                unique_id=row.unique_id,
                name=row.name,
                level=row.level,
                resource_type=ResourceType(row.resource_type),
                status=row.status,
                execution_time=row.execution_time,
                executed_at=row.executed_at,
                health_status=health_status
            ))
        
        return dependencies
    
    def get_downstream_impact(self, model_name: str, schedule_name: str, max_depth: int = 10) -> DependencyImpact:
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
        results = [tuple(row) for row in df.itertuples(index=False)]
        
        return DependencyImpact.from_results(results, model_name)
    
    def get_health_dashboard(self, schedule_name: str) -> HealthDashboard:
        """
        Get comprehensive health dashboard metrics for a schedule.
        
        Args:
            schedule_name: Name of the schedule
            
        Returns:
            HealthDashboard with aggregated health metrics
        """
        self._ensure_metadata_loaded(schedule_name)
        
        # Get model health data
        models = self.get_model_health(schedule_name)
        
        # Get source freshness data
        sources = self.get_source_freshness(schedule_name)
        
        # Calculate metrics
        total_models = len(models)
        healthy_models = len([m for m in models if m.health_status == HealthStatus.HEALTHY])
        warning_models = len([m for m in models if m.health_status == HealthStatus.WARNING])
        critical_models = len([m for m in models if m.health_status == HealthStatus.CRITICAL])
        
        total_tests = sum(m.total_tests for m in models)
        failed_tests = sum(m.failed_tests for m in models)
        test_success_rate = 100.0 if total_tests == 0 else ((total_tests - failed_tests) * 100.0 / total_tests)
        
        avg_execution_time = sum(m.execution_time for m in models if m.execution_time) / len([m for m in models if m.execution_time]) if models else 0.0
        
        sources_checked = len(sources)
        stale_sources = len([s for s in sources if s.freshness_status != 'pass'])
        
        return HealthDashboard(
            schedule_name=schedule_name,
            total_models=total_models,
            healthy_models=healthy_models,
            warning_models=warning_models,
            critical_models=critical_models,
            avg_execution_time=avg_execution_time,
            test_success_rate=test_success_rate,
            total_tests=total_tests,
            failed_tests=failed_tests,
            sources_checked=sources_checked,
            stale_sources=stale_sources,
            last_updated=datetime.utcnow()
        )
    
    def get_models_with_failing_tests(self, schedule_name: str) -> List[ModelHealth]:
        """
        Get models that have failing tests.
        
        Args:
            schedule_name: Name of the schedule
            
        Returns:
            List of ModelHealth objects for models with failed tests
        """
        models = self.get_model_health(schedule_name)
        return [m for m in models if m.failed_tests > 0]
    
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
        models_with_time.sort(key=lambda x: x.execution_time, reverse=True)
        return models_with_time[:limit]
    
    def get_performance_metrics(self, schedule_name: str, days: int = 7) -> PerformanceMetrics:
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
            {
                'name': m.name,
                'execution_time': m.execution_time,
                'status': m.status
            }
            for m in self.get_slowest_models(schedule_name, 10)
        ]
        
        total_models = len(models)
        successful_models = len([m for m in models if m.status == 'success'])
        avg_execution_time = sum(m.execution_time for m in models if m.execution_time) / len([m for m in models if m.execution_time]) if models else 0.0
        success_rate = (successful_models * 100.0 / total_models) if total_models > 0 else 0.0
        
        return PerformanceMetrics(
            schedule_name=schedule_name,
            time_period_days=days,
            slowest_models=slowest_models,
            average_execution_time=avg_execution_time,
            total_runs=total_models,  # Using models as proxy for runs
            success_rate=success_rate,
            performance_trend=[]  # Would need historical data
        )
    
    def query(self, 
              schedule_name: str,
              include_models: bool = True,
              include_tests: bool = True,
              include_sources: bool = True) -> MetadataResponse:
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
            response.tests = self.get_test_results(schedule_name)
        
        if include_sources:
            response.sources = self.get_source_freshness(schedule_name)
        
        return response
    
    def close(self) -> None:
        """Close database connection"""
        self.db.close()