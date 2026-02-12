from typing import List, Dict, Any, Optional
import duckdb
import pandas as pd
from datetime import datetime

from .types import ModelHealth, TestResult, SourceFreshness


class MetadataDatabase:
    """DuckDB-based metadata storage and querying"""

    def __init__(self, connection_string: str = ":memory:"):
        """
        Initialize MetadataDatabase with DuckDB connection.
        
        Args:
            connection_string: DuckDB connection string. Defaults to in-memory database.
                               Use a file path for persistent storage.
        """
        self.conn = duckdb.connect(connection_string)
        self._create_schema()
    
    def _create_schema(self) -> None:
        """Create database schema matching backend BigQuery structure"""
        
        # Main table for dbt run results (models, tests, etc.)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_run_results (
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                status VARCHAR,
                execution_time DOUBLE,
                executed_at TIMESTAMP,
                schedule_name VARCHAR,
                depends_on VARCHAR[],
                error_message TEXT,
                -- Model-specific fields
                schema_name VARCHAR,
                database_name VARCHAR,
                model_type VARCHAR,
                config JSON,
                tags VARCHAR[],
                meta JSON,
                -- Timing information
                compile_started_at TIMESTAMP,
                compile_completed_at TIMESTAMP,
                execute_started_at TIMESTAMP,
                execute_completed_at TIMESTAMP,
                -- Additional metadata
                thread_id VARCHAR,
                adapter_response JSON
            )
        """)
        
        # Source freshness results
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_source_freshness_results (
                source_name VARCHAR,
                table_name VARCHAR,
                freshness_status VARCHAR,
                max_loaded_at TIMESTAMP,
                snapshotted_at TIMESTAMP,
                schedule_name VARCHAR,
                error_after_hours INTEGER,
                warn_after_hours INTEGER,
                error_message TEXT,
                -- Additional metadata
                unique_id VARCHAR,
                criteria JSON
            )
        """)
        
        # Model metadata and relationships
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS model_metadata (
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                depends_on VARCHAR[],
                config JSON,
                tags VARCHAR[],
                meta JSON,
                schedule_name VARCHAR,
                description TEXT,
                columns JSON,
                -- Lineage information
                parents VARCHAR[],
                children VARCHAR[],
                -- File information
                original_file_path VARCHAR,
                root_path VARCHAR
            )
        """)
        
        # Create views for common queries
        self._create_views()
    
    def _create_views(self) -> None:
        """Create views for common metadata queries"""
        
        # Simple view without complex joins - we'll calculate test counts in the application layer
        self.conn.execute("""
            CREATE OR REPLACE VIEW models_with_tests AS
            SELECT 
                unique_id,
                name,
                resource_type,
                status,
                execution_time,
                executed_at,
                schedule_name,
                depends_on,
                schema_name,
                database_name,
                error_message,
                0 as total_tests,
                0 as failed_tests,
                CASE 
                    WHEN status IN ('error', 'fail') THEN 'Critical'
                    ELSE 'Healthy'
                END as health_status
            FROM dbt_run_results
            WHERE resource_type = 'model'
        """)
        
        # Latest run results per model
        self.conn.execute("""
            CREATE OR REPLACE VIEW latest_model_results AS
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY unique_id, schedule_name ORDER BY executed_at DESC) as rn
                FROM models_with_tests
            ) ranked
            WHERE rn = 1
        """)
    
    def load_run_results(self, run_results_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load run results data into dbt_run_results table"""
        if not run_results_data:
            return
        
        # Insert data row by row to handle NULL values properly
        self.conn.execute("DELETE FROM dbt_run_results WHERE schedule_name = ?", [schedule_name])
        
        for row in run_results_data:
            # Ensure all required columns are present with proper defaults
            row_data = {
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'status': row.get('status'),
                'execution_time': row.get('execution_time'),
                'executed_at': row.get('executed_at'),
                'schedule_name': schedule_name,
                'depends_on': row.get('depends_on', []),
                'error_message': row.get('error_message'),
                'schema_name': row.get('schema_name'),
                'database_name': row.get('database_name'),
                'model_type': row.get('model_type'),
                'config': row.get('config', {}),
                'tags': row.get('tags', []),
                'meta': row.get('meta', {}),
                'compile_started_at': row.get('compile_started_at'),
                'compile_completed_at': row.get('compile_completed_at'),
                'execute_started_at': row.get('execute_started_at'),
                'execute_completed_at': row.get('execute_completed_at'),
                'thread_id': row.get('thread_id'),
                'adapter_response': row.get('adapter_response', {})
            }
            
            self.conn.execute("""
                INSERT INTO dbt_run_results (
                    unique_id, name, resource_type, status, execution_time, executed_at,
                    schedule_name, depends_on, error_message, schema_name, database_name,
                    model_type, config, tags, meta, compile_started_at, compile_completed_at,
                    execute_started_at, execute_completed_at, thread_id, adapter_response
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'],
                row_data['status'], row_data['execution_time'], row_data['executed_at'],
                row_data['schedule_name'], row_data['depends_on'], row_data['error_message'],
                row_data['schema_name'], row_data['database_name'], row_data['model_type'],
                row_data['config'], row_data['tags'], row_data['meta'],
                row_data['compile_started_at'], row_data['compile_completed_at'],
                row_data['execute_started_at'], row_data['execute_completed_at'],
                row_data['thread_id'], row_data['adapter_response']
            ])
    
    def load_source_freshness(self, source_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load source freshness data into dbt_source_freshness_results table"""
        if not source_data:
            return
        
        self.conn.execute("DELETE FROM dbt_source_freshness_results WHERE schedule_name = ?", [schedule_name])
        
        for row in source_data:
            row_data = {
                'source_name': row.get('source_name'),
                'table_name': row.get('table_name'),
                'freshness_status': row.get('freshness_status'),
                'max_loaded_at': row.get('max_loaded_at'),
                'snapshotted_at': row.get('snapshotted_at'),
                'schedule_name': schedule_name,
                'error_after_hours': row.get('error_after_hours'),
                'warn_after_hours': row.get('warn_after_hours'),
                'error_message': row.get('error_message'),
                'unique_id': row.get('unique_id'),
                'criteria': row.get('criteria', {})
            }
            
            self.conn.execute("""
                INSERT INTO dbt_source_freshness_results (
                    source_name, table_name, freshness_status, max_loaded_at, snapshotted_at,
                    schedule_name, error_after_hours, warn_after_hours, error_message,
                    unique_id, criteria
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['source_name'], row_data['table_name'], row_data['freshness_status'],
                row_data['max_loaded_at'], row_data['snapshotted_at'], row_data['schedule_name'],
                row_data['error_after_hours'], row_data['warn_after_hours'], row_data['error_message'],
                row_data['unique_id'], row_data['criteria']
            ])
    
    def load_model_metadata(self, metadata: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load model metadata into model_metadata table"""
        if not metadata:
            return
        
        self.conn.execute("DELETE FROM model_metadata WHERE schedule_name = ?", [schedule_name])
        
        for row in metadata:
            row_data = {
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'depends_on': row.get('depends_on', []),
                'config': row.get('config', {}),
                'tags': row.get('tags', []),
                'meta': row.get('meta', {}),
                'schedule_name': schedule_name,
                'description': row.get('description', ''),
                'columns': row.get('columns', {}),
                'parents': row.get('parents', []),
                'children': row.get('children', []),
                'original_file_path': row.get('original_file_path'),
                'root_path': row.get('root_path')
            }
            
            self.conn.execute("""
                INSERT INTO model_metadata (
                    unique_id, name, resource_type, depends_on, config, tags, meta,
                    schedule_name, description, columns, parents, children,
                    original_file_path, root_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'],
                row_data['depends_on'], row_data['config'], row_data['tags'], row_data['meta'],
                row_data['schedule_name'], row_data['description'], row_data['columns'],
                row_data['parents'], row_data['children'], row_data['original_file_path'],
                row_data['root_path']
            ])
    
    def query_sql(self, sql: str, parameters: Optional[List[Any]] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        if parameters:
            return self.conn.execute(sql, parameters).df()
        else:
            return self.conn.execute(sql).df()
    
    def get_model_health(self, schedule_name: str) -> List[ModelHealth]:
        """Get model health status for a schedule"""
        # Get models
        models_sql = """
            SELECT 
                unique_id,
                name,
                resource_type,
                status,
                execution_time,
                executed_at,
                depends_on,
                schema_name,
                database_name,
                error_message
            FROM dbt_run_results
            WHERE schedule_name = ? AND resource_type = 'model'
            ORDER BY executed_at DESC
        """
        
        models_df = self.conn.execute(models_sql, [schedule_name]).df()
        
        # Get test counts for each model
        tests_sql = """
            SELECT 
                depends_on,
                COUNT(*) as total_tests,
                COUNT(CASE WHEN status IN ('fail', 'error') THEN 1 END) as failed_tests
            FROM dbt_run_results
            WHERE schedule_name = ? AND resource_type = 'test'
            GROUP BY depends_on
        """
        
        tests_df = self.conn.execute(tests_sql, [schedule_name]).df()
        
        # Create a mapping of model_id -> test counts
        test_counts = {}
        for _, test_row in tests_df.iterrows():
            if test_row['depends_on'] and len(test_row['depends_on']) > 0:
                for model_id in test_row['depends_on']:
                    if model_id not in test_counts:
                        test_counts[model_id] = {'total': 0, 'failed': 0}
                    test_counts[model_id]['total'] += test_row['total_tests']
                    test_counts[model_id]['failed'] += test_row['failed_tests']
        
        # Build ModelHealth objects
        results = []
        for _, model_row in models_df.iterrows():
            model_id = model_row['unique_id']
            test_info = test_counts.get(model_id, {'total': 0, 'failed': 0})
            
            # Determine health status
            if model_row['status'] in ['error', 'fail']:
                health_status = 'Critical'
            elif test_info['failed'] > 0:
                health_status = 'Warning'
            else:
                health_status = 'Healthy'
            
            results.append(ModelHealth(
                unique_id=model_id,
                name=model_row['name'],
                resource_type=model_row['resource_type'],
                status=model_row['status'],
                execution_time=model_row['execution_time'],
                executed_at=model_row['executed_at'],
                health_status=health_status,
                total_tests=test_info['total'],
                failed_tests=test_info['failed'],
                depends_on=model_row['depends_on'] or [],
                schema_name=model_row['schema_name'],
                database_name=model_row['database_name'],
                error_message=model_row['error_message']
            ))
        
        # Sort by health status and execution time
        results.sort(key=lambda x: (
            1 if x.health_status == 'Critical' else 2 if x.health_status == 'Warning' else 3,
            -(x.execution_time or 0)
        ))
        
        return results
    
    def get_test_results(self, schedule_name: str, failed_only: bool = False) -> List[TestResult]:
        """Get test results for a schedule"""
        sql = """
            SELECT 
                unique_id,
                name as test_name,
                status,
                executed_at,
                depends_on,
                error_message
            FROM dbt_run_results
            WHERE schedule_name = ? AND resource_type = 'test'
        """
        
        if failed_only:
            sql += " AND status IN ('fail', 'error')"
        
        sql += " ORDER BY executed_at DESC"
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            results.append(TestResult(
                unique_id=row.unique_id,
                test_name=row.test_name,
                status=row.status,
                executed_at=row.executed_at,
                depends_on_nodes=row.depends_on if row.depends_on else [],
                error_message=row.error_message
            ))
        
        return results
    
    def get_source_freshness(self, schedule_name: str) -> List[SourceFreshness]:
        """Get source freshness results for a schedule"""
        sql = """
            SELECT 
                source_name,
                table_name,
                freshness_status,
                max_loaded_at,
                snapshotted_at,
                error_after_hours,
                warn_after_hours,
                error_message,
                CASE 
                    WHEN max_loaded_at IS NOT NULL 
                    THEN CAST((EPOCH(NOW()) - EPOCH(max_loaded_at)) / 3600 AS DOUBLE)
                    ELSE NULL 
                END as hours_since_load
            FROM dbt_source_freshness_results
            WHERE schedule_name = ?
            ORDER BY 
                CASE freshness_status 
                    WHEN 'error' THEN 1 
                    WHEN 'warn' THEN 2 
                    ELSE 3 
                END,
                hours_since_load DESC
        """
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            alert_level = None
            if row.freshness_status == 'error':
                alert_level = 'Critical - Data is stale'
            elif row.freshness_status == 'warn':
                alert_level = 'Warning - Data aging'
            else:
                alert_level = 'Fresh'
            
            results.append(SourceFreshness(
                source_name=row.source_name,
                table_name=row.table_name,
                freshness_status=row.freshness_status,
                max_loaded_at=row.max_loaded_at,
                snapshotted_at=row.snapshotted_at,
                error_after_hours=row.error_after_hours,
                warn_after_hours=row.warn_after_hours,
                hours_since_load=row.hours_since_load,
                alert_level=alert_level
            ))
        
        return results
    
    def get_upstream_dependencies(self, model_name: str, schedule_name: str, max_depth: int = 10) -> pd.DataFrame:
        """Get upstream dependencies for a model - simplified approach"""
        # For now, return a simple upstream lookup without recursion
        # This can be enhanced later with proper recursive support
        
        # Get the target model's dependencies
        model_sql = """
            SELECT depends_on
            FROM model_metadata
            WHERE name = ? AND schedule_name = ?
        """
        
        model_result = self.conn.execute(model_sql, [model_name, schedule_name]).df()
        if model_result.empty:
            return pd.DataFrame(columns=['unique_id', 'name', 'level', 'resource_type', 'status', 'execution_time', 'executed_at', 'health_status'])
        
        depends_on = model_result.iloc[0]['depends_on']
        if not depends_on:
            return pd.DataFrame(columns=['unique_id', 'name', 'level', 'resource_type', 'status', 'execution_time', 'executed_at', 'health_status'])
        
        # Get direct dependencies (level 1)
        placeholders = ','.join(['?' for _ in depends_on])
        sql = f"""
            SELECT 
                m.unique_id,
                m.name,
                1 as level,
                m.resource_type,
                r.status,
                r.execution_time,
                r.executed_at,
                CASE 
                    WHEN r.status IN ('error', 'fail') THEN 'Critical'
                    ELSE 'Healthy'
                END as health_status
            FROM model_metadata m
            LEFT JOIN dbt_run_results r ON m.unique_id = r.unique_id AND r.schedule_name = ?
            WHERE m.unique_id IN ({placeholders}) AND m.schedule_name = ?
            ORDER BY m.name
        """
        
        params = [schedule_name] + depends_on + [schedule_name]
        return self.conn.execute(sql, params).df()
    
    def get_downstream_impact(self, model_name: str, schedule_name: str, max_depth: int = 10) -> pd.DataFrame:
        """Get downstream impact of a model - simplified approach"""
        # Get the target model's unique_id first
        target_sql = """
            SELECT unique_id
            FROM model_metadata
            WHERE name = ? AND schedule_name = ?
        """
        
        target_result = self.conn.execute(target_sql, [model_name, schedule_name]).df()
        if target_result.empty:
            return pd.DataFrame(columns=['name', 'downstream_level', 'current_status', 'executed_at', 'impact_status'])
        
        target_id = target_result.iloc[0]['unique_id']
        
        # Find models that depend on this target model (level 1 downstream)
        sql = """
            SELECT 
                m.name,
                1 as downstream_level,
                r.status as current_status,
                r.executed_at,
                CASE 
                    WHEN r.status IN ('error', 'fail') THEN 'Already Impacted'
                    ELSE 'May Be Affected'
                END as impact_status
            FROM model_metadata m
            LEFT JOIN dbt_run_results r ON m.unique_id = r.unique_id AND r.schedule_name = ?
            WHERE ? = ANY(m.depends_on) AND m.schedule_name = ?
            ORDER BY m.name
        """
        
        return self.conn.execute(sql, [schedule_name, target_id, schedule_name]).df()
    
    def close(self) -> None:
        """Close database connection"""
        self.conn.close()