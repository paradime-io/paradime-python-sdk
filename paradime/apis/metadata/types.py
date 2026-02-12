from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any

from paradime.tools.pydantic import BaseModel


class ResourceType(str, Enum):
    MODEL = "model"
    TEST = "test"
    SOURCE = "source"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    ANALYSIS = "analysis"
    MACRO = "macro"
    EXPOSURE = "exposure"
    METRIC = "metric"


class RunStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    FAIL = "fail"
    PASS = "pass"
    WARN = "warn"
    SKIPPED = "skipped"


class HealthStatus(str, Enum):
    HEALTHY = "Healthy"
    WARNING = "Warning"
    CRITICAL = "Critical"


class FreshnessStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    ERROR = "error"


class ModelHealth(BaseModel):
    unique_id: str
    name: str
    resource_type: ResourceType
    status: RunStatus
    execution_time: Optional[float]
    executed_at: Optional[datetime]
    health_status: HealthStatus
    total_tests: int = 0
    failed_tests: int = 0
    depends_on: List[str] = []
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    error_message: Optional[str] = None

    @classmethod
    def from_row(cls, row: Any) -> "ModelHealth":
        """Create ModelHealth from database row or tuple"""
        if hasattr(row, '_asdict'):
            data = row._asdict()
        elif isinstance(row, dict):
            data = row
        else:
            # Assume it's a tuple/list with specific order
            data = {
                'unique_id': row[0],
                'name': row[1], 
                'resource_type': row[2],
                'status': row[3],
                'execution_time': row[4],
                'executed_at': row[5],
                'health_status': row[6],
                'total_tests': row[7] if len(row) > 7 else 0,
                'failed_tests': row[8] if len(row) > 8 else 0,
            }
        
        return cls(**data)


class TestResult(BaseModel):
    unique_id: str
    test_name: str
    status: RunStatus
    executed_at: Optional[datetime]
    depends_on_nodes: List[str] = []
    tested_models: List[str] = []
    test_type: Optional[str] = None
    severity: Optional[str] = None
    error_message: Optional[str] = None


class SourceFreshness(BaseModel):
    source_name: str
    table_name: str
    freshness_status: FreshnessStatus
    max_loaded_at: Optional[datetime]
    snapshotted_at: Optional[datetime]
    hours_since_load: Optional[float] = None
    error_after_hours: Optional[int] = None
    warn_after_hours: Optional[int] = None
    alert_level: Optional[str] = None


class ModelDependency(BaseModel):
    unique_id: str
    name: str
    level: int
    resource_type: ResourceType
    status: Optional[RunStatus] = None
    execution_time: Optional[float] = None
    executed_at: Optional[datetime] = None
    health_status: Optional[HealthStatus] = None


class HealthDashboard(BaseModel):
    schedule_name: str
    total_models: int
    healthy_models: int
    warning_models: int
    critical_models: int
    avg_execution_time: float
    test_success_rate: float
    total_tests: int
    failed_tests: int
    sources_checked: int
    stale_sources: int
    last_updated: datetime

    @classmethod
    def from_dataframe(cls, df: Any, schedule_name: str) -> "HealthDashboard":
        """Create HealthDashboard from pandas DataFrame with aggregated data"""
        return cls(
            schedule_name=schedule_name,
            total_models=len(df),
            healthy_models=len(df[df['health_status'] == 'Healthy']),
            warning_models=len(df[df['health_status'] == 'Warning']),
            critical_models=len(df[df['health_status'] == 'Critical']),
            avg_execution_time=df['execution_time'].mean() if 'execution_time' in df.columns else 0.0,
            test_success_rate=100.0 if len(df) == 0 else (df['failed_tests'] == 0).sum() * 100.0 / len(df),
            total_tests=df['total_tests'].sum() if 'total_tests' in df.columns else 0,
            failed_tests=df['failed_tests'].sum() if 'failed_tests' in df.columns else 0,
            sources_checked=0,  # Will be populated separately
            stale_sources=0,    # Will be populated separately
            last_updated=datetime.utcnow()
        )


class DependencyImpact(BaseModel):
    failed_model: str
    critical_models: List[str] = []
    warning_models: List[str] = []
    potentially_affected: List[str] = []
    total_affected: int = 0

    @classmethod
    def from_results(cls, results: List[Any], failed_model: str) -> "DependencyImpact":
        """Create DependencyImpact from query results"""
        critical = []
        warning = []
        potentially_affected = []
        
        for row in results:
            name = row[0] if isinstance(row, (tuple, list)) else row.name
            status = row[2] if isinstance(row, (tuple, list)) else getattr(row, 'status', None)
            
            if status in ['error', 'fail']:
                critical.append(name)
            elif status in ['warn']:
                warning.append(name)
            else:
                potentially_affected.append(name)
        
        return cls(
            failed_model=failed_model,
            critical_models=critical,
            warning_models=warning,
            potentially_affected=potentially_affected,
            total_affected=len(critical) + len(warning) + len(potentially_affected)
        )


class PerformanceMetrics(BaseModel):
    schedule_name: str
    time_period_days: int
    slowest_models: List[Dict[str, Any]] = []
    average_execution_time: float = 0.0
    total_runs: int = 0
    success_rate: float = 0.0
    performance_trend: List[Dict[str, Any]] = []  # Daily performance data


class ParsedArtifacts(BaseModel):
    """Container for parsed dbt artifacts"""
    manifest: Optional[Any] = None
    run_results: Optional[Any] = None
    sources: Optional[Any] = None
    schedule_name: str
    
    class Config:
        arbitrary_types_allowed = True


class MetadataResponse(BaseModel):
    """Generic response container for metadata queries"""
    models: List[ModelHealth] = []
    tests: List[TestResult] = []
    sources: List[SourceFreshness] = []
    dependencies: List[ModelDependency] = []
    schedule_name: str
    query_timestamp: datetime = datetime.utcnow()