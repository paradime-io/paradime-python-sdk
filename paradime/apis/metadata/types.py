from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from paradime.tools.pydantic import BaseModel, Field


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

    alias: Optional[str] = None
    materialized_type: Optional[str] = None
    description: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []
    owner: Optional[str] = None
    package_name: Optional[str] = None
    language: Optional[str] = None
    access: Optional[str] = None
    compiled_sql: Optional[str] = None
    raw_sql: Optional[str] = None
    columns: Optional[Any] = None
    children_l1: List[str] = []  # Direct children
    parents_models: List[str] = []  # Parent models
    parents_sources: List[str] = []  # Parent sources

    @classmethod
    def from_row(cls, row: Any) -> "ModelHealth":
        """Create ModelHealth from database row or tuple"""
        if hasattr(row, "_asdict"):
            data = row._asdict()
        elif isinstance(row, dict):
            data = row
        else:
            # Assume it's a tuple/list with specific order
            data = {
                "unique_id": row[0],
                "name": row[1],
                "resource_type": row[2],
                "status": row[3],
                "execution_time": row[4],
                "executed_at": row[5],
                "health_status": row[6],
                "total_tests": row[7] if len(row) > 7 else 0,
                "failed_tests": row[8] if len(row) > 8 else 0,
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


class SeedData(BaseModel):
    """Seed metadata"""

    # Core identification
    unique_id: str
    name: str
    resource_type: ResourceType

    # Database location
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    alias: Optional[str] = None

    # Execution information
    status: Optional[RunStatus] = None
    execution_time: Optional[float] = None  # executionTime
    run_elapsed_time: Optional[float] = None  # runElapsedTime

    # Timing information
    compile_started_at: Optional[datetime] = None  # compileStartedAt
    compile_completed_at: Optional[datetime] = None  # compileCompletedAt
    execute_started_at: Optional[datetime] = None  # executeStartedAt
    execute_completed_at: Optional[datetime] = None  # executeCompletedAt
    run_generated_at: Optional[datetime] = None  # runGeneratedAt

    # Code and SQL
    compiled_code: Optional[str] = None  # compiledCode
    compiled_sql: Optional[str] = None  # compiledSql
    raw_code: Optional[str] = None  # rawCode
    raw_sql: Optional[str] = None  # rawSql

    # Metadata and documentation
    description: Optional[str] = None
    comment: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []
    owner: Optional[str] = None
    package_name: Optional[str] = None  # packageName

    # Execution details
    error: Optional[str] = None
    skip: Optional[bool] = None
    thread_id: Optional[str] = None
    type: Optional[str] = None

    # Lineage
    children_l1: List[str] = []  # childrenL1 - nodes that depend on this seed

    # Statistics and columns
    columns: Optional[Any] = None
    stats: Optional[Any] = None

    # Legacy/additional fields
    depends_on: List[str] = []  # For consistency with other resource types


class SnapshotData(BaseModel):
    """Snapshot metadata"""

    # Core identification
    unique_id: str
    name: str
    resource_type: ResourceType

    # Database location
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    alias: Optional[str] = None

    # Execution information
    status: Optional[RunStatus] = None
    execution_time: Optional[float] = None  # executionTime
    run_elapsed_time: Optional[float] = None  # runElapsedTime

    # Timing information
    compile_started_at: Optional[datetime] = None  # compileStartedAt
    compile_completed_at: Optional[datetime] = None  # compileCompletedAt
    execute_started_at: Optional[datetime] = None  # executeStartedAt
    execute_completed_at: Optional[datetime] = None  # executeCompletedAt
    run_generated_at: Optional[datetime] = None  # runGeneratedAt

    # Code and SQL
    compiled_code: Optional[str] = None  # compiledCode
    compiled_sql: Optional[str] = None  # compiledSql
    raw_code: Optional[str] = None  # rawCode
    raw_sql: Optional[str] = None  # rawSql

    # Metadata and documentation
    description: Optional[str] = None
    comment: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []
    owner: Optional[str] = None
    package_name: Optional[str] = None  # packageName

    # Execution details
    error: Optional[str] = None
    skip: Optional[bool] = None
    thread_id: Optional[str] = None  # threadId
    type: Optional[str] = None

    # Lineage
    children_l1: List[str] = []  # childrenL1 - nodes that depend on this snapshot
    parents_models: List[str] = []  # parentsModels - parent model dependencies
    parents_sources: List[str] = []  # parentsSources - parent source dependencies

    # Statistics and columns
    columns: Optional[Any] = None
    stats: Optional[Any] = None

    # Legacy/additional fields
    depends_on: List[str] = []  # For consistency with other resource types


class SourceFreshness(BaseModel):
    # Core identification
    unique_id: str
    source_name: str  # sourceName in dbt Discovery API
    name: str  # table name / identifier
    table_name: str  # For backwards compatibility

    # Freshness information
    freshness_status: FreshnessStatus  # state in dbt Discovery API
    freshness_checked: Optional[bool] = None
    max_loaded_at: Optional[datetime]  # maxLoadedAt
    snapshotted_at: Optional[datetime]  # snapshottedAt
    max_loaded_at_time_ago_in_s: Optional[float] = None  # maxLoadedAtTimeAgoInS
    hours_since_load: Optional[float] = None  # Calculated field for backwards compatibility

    # Criteria and thresholds
    error_after_hours: Optional[int] = None
    warn_after_hours: Optional[int] = None
    criteria: Optional[Any] = None  # Full criteria object

    # Database location
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")  # schema field in Discovery API
    identifier: Optional[str] = None

    # Metadata and documentation
    description: Optional[str] = None
    source_description: Optional[str] = None  # sourceDescription
    comment: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []
    owner: Optional[str] = None
    loader: Optional[str] = None
    type: Optional[str] = None

    # Run information
    run_elapsed_time: Optional[float] = None  # runElapsedTime
    run_generated_at: Optional[datetime] = None  # runGeneratedAt

    # Lineage
    children_l1: List[str] = []  # childrenL1 - nodes that depend on this source

    # Statistics and columns (for full Discovery API parity)
    columns: Optional[Any] = None  # Can be dict or list depending on source
    stats: Optional[Any] = None  # Can be dict or list depending on source
    tests: List[str] = []  # Test unique IDs

    # Legacy fields for backwards compatibility
    alert_level: Optional[str] = None


class TestData(BaseModel):
    """Test metadata"""

    # Core identification
    unique_id: str
    name: Optional[str] = None
    resource_type: ResourceType

    # Run identification
    run_id: Optional[int] = None  # runId
    invocation_id: Optional[str] = None  # invocationId

    # Test-specific information
    column_name: Optional[str] = None  # columnName - test column
    state: Optional[str] = None  # state: error, fail, warn, pass
    status: Optional[str] = None  # status: ERROR or number of rows
    fail: Optional[bool] = None  # fail result
    warn: Optional[bool] = None  # warn result
    skip: Optional[bool] = None  # skip result

    # Execution information
    execution_time: Optional[float] = None  # executionTime
    run_elapsed_time: Optional[float] = None  # runElapsedTime

    # Timing information
    compile_started_at: Optional[datetime] = None  # compileStartedAt
    compile_completed_at: Optional[datetime] = None  # compileCompletedAt
    execute_started_at: Optional[datetime] = None  # executeStartedAt
    execute_completed_at: Optional[datetime] = None  # executeCompletedAt
    run_generated_at: Optional[datetime] = None  # runGeneratedAt

    # Code and SQL
    compiled_code: Optional[str] = None  # compiledCode
    compiled_sql: Optional[str] = None  # compiledSql
    raw_code: Optional[str] = None  # rawCode
    raw_sql: Optional[str] = None  # rawSql

    # Metadata and documentation
    description: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []

    # Technical details
    language: Optional[str] = None
    dbt_version: Optional[str] = None  # dbtVersion
    thread_id: Optional[str] = None  # threadId
    error: Optional[str] = None  # error message

    # Dependencies
    depends_on: List[str] = []  # dependsOn


class ExposureData(BaseModel):
    """Exposure metadata"""

    # Core identification
    unique_id: str
    name: Optional[str] = None
    resource_type: ResourceType

    # Run identification
    run_id: Optional[int] = None  # runId

    # Exposure-specific information
    exposure_type: Optional[str] = None  # exposureType
    maturity: Optional[str] = None
    owner_name: Optional[str] = None  # ownerName
    owner_email: Optional[str] = None  # ownerEmail
    url: Optional[str] = None
    package_name: Optional[str] = None  # packageName

    # Execution information
    status: Optional[str] = None
    execution_time: Optional[float] = None  # executionTime
    thread_id: Optional[str] = None  # threadId

    # Timing information
    compile_started_at: Optional[datetime] = None  # compileStartedAt
    compile_completed_at: Optional[datetime] = None  # compileCompletedAt
    execute_started_at: Optional[datetime] = None  # executeStartedAt
    execute_completed_at: Optional[datetime] = None  # executeCompletedAt
    manifest_generated_at: Optional[datetime] = None  # manifestGeneratedAt

    # Metadata and documentation
    description: Optional[str] = None
    meta: Optional[Any] = None
    tags: List[str] = []

    # Technical details
    dbt_version: Optional[str] = None  # dbtVersion

    # Dependencies and lineage
    depends_on: List[str] = []  # dependsOn
    parents: List[str] = []  # parents - all parent resources
    parents_models: List[str] = []  # parentsModels
    parents_sources: List[str] = []  # parentsSources


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
            healthy_models=len(df[df["health_status"] == "Healthy"]),
            warning_models=len(df[df["health_status"] == "Warning"]),
            critical_models=len(df[df["health_status"] == "Critical"]),
            avg_execution_time=(
                df["execution_time"].mean() if "execution_time" in df.columns else 0.0
            ),
            test_success_rate=(
                100.0 if len(df) == 0 else (df["failed_tests"] == 0).sum() * 100.0 / len(df)
            ),
            total_tests=df["total_tests"].sum() if "total_tests" in df.columns else 0,
            failed_tests=df["failed_tests"].sum() if "failed_tests" in df.columns else 0,
            sources_checked=0,  # Will be populated separately
            stale_sources=0,  # Will be populated separately
            last_updated=datetime.utcnow(),
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
            status = row[2] if isinstance(row, (tuple, list)) else getattr(row, "status", None)

            if status in ["error", "fail"]:
                critical.append(name)
            elif status in ["warn"]:
                warning.append(name)
            else:
                potentially_affected.append(name)

        return cls(
            failed_model=failed_model,
            critical_models=critical,
            warning_models=warning,
            potentially_affected=potentially_affected,
            total_affected=len(critical) + len(warning) + len(potentially_affected),
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
    tests: List[TestData] = []
    sources: List[SourceFreshness] = []
    seeds: List[SeedData] = []
    snapshots: List[SnapshotData] = []
    exposures: List[ExposureData] = []
    dependencies: List[ModelDependency] = []
    schedule_name: str
    query_timestamp: datetime = datetime.utcnow()
