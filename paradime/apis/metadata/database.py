from typing import List, Dict, Any, Optional
import duckdb
import pandas as pd
import json
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
                adapter_response JSON,
                alias VARCHAR,
                materialized_type VARCHAR,
                description TEXT,
                access VARCHAR,
                language VARCHAR,
                package_name VARCHAR,
                owner VARCHAR,
                compiled_sql TEXT,
                raw_sql TEXT,
                columns JSON,
                children_l1 VARCHAR[],
                parents_models VARCHAR[],
                parents_sources VARCHAR[],
                original_file_path VARCHAR,
                root_path VARCHAR
            )
        """)
        
        # Source freshness
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_source_freshness_results (
                -- Core identification
                unique_id VARCHAR,
                source_name VARCHAR,
                name VARCHAR,
                table_name VARCHAR,
                schedule_name VARCHAR,
                
                -- Freshness information
                freshness_status VARCHAR,
                freshness_checked BOOLEAN,
                max_loaded_at TIMESTAMP,
                snapshotted_at TIMESTAMP,
                max_loaded_at_time_ago_in_s DOUBLE,
                hours_since_load DOUBLE,
                
                -- Criteria and thresholds
                error_after_hours INTEGER,
                warn_after_hours INTEGER,
                criteria JSON,
                
                -- Database location
                database VARCHAR,
                schema_name VARCHAR,
                identifier VARCHAR,
                
                -- Metadata and documentation
                description TEXT,
                source_description TEXT,
                comment TEXT,
                meta JSON,
                tags VARCHAR[],
                owner VARCHAR,
                loader VARCHAR,
                type VARCHAR,
                
                -- Run information
                run_elapsed_time DOUBLE,
                run_generated_at TIMESTAMP,
                
                -- Lineage
                children_l1 VARCHAR[],
                
                -- Statistics and columns
                columns JSON,
                stats JSON,
                tests VARCHAR[],
                
                -- Legacy fields
                error_message TEXT,
                alert_level VARCHAR
            )
        """)
        
        # Snapshot data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_snapshot_data (
                -- Core identification
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                schedule_name VARCHAR,
                
                -- Database location
                database VARCHAR,
                schema_name VARCHAR,
                alias VARCHAR,
                
                -- Execution information
                status VARCHAR,
                execution_time DOUBLE,
                run_elapsed_time DOUBLE,
                
                -- Timing information
                compile_started_at TIMESTAMP,
                compile_completed_at TIMESTAMP,
                execute_started_at TIMESTAMP,
                execute_completed_at TIMESTAMP,
                run_generated_at TIMESTAMP,
                
                -- Code and SQL
                compiled_code TEXT,
                compiled_sql TEXT,
                raw_code TEXT,
                raw_sql TEXT,
                
                -- Metadata and documentation
                description TEXT,
                comment TEXT,
                meta JSON,
                tags VARCHAR[],
                owner VARCHAR,
                package_name VARCHAR,
                
                -- Execution details
                error TEXT,
                skip BOOLEAN,
                thread_id VARCHAR,
                type VARCHAR,
                
                -- Lineage
                children_l1 VARCHAR[],
                parents_models VARCHAR[],
                parents_sources VARCHAR[],
                
                -- Statistics and columns
                columns JSON,
                stats JSON,
                
                -- Legacy/additional fields
                depends_on VARCHAR[]
            )
        """)
        
        # Seed data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_seed_data (
                -- Core identification
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                schedule_name VARCHAR,
                
                -- Database location
                database VARCHAR,
                schema_name VARCHAR,
                alias VARCHAR,
                
                -- Execution information
                status VARCHAR,
                execution_time DOUBLE,
                run_elapsed_time DOUBLE,
                
                -- Timing information
                compile_started_at TIMESTAMP,
                compile_completed_at TIMESTAMP,
                execute_started_at TIMESTAMP,
                execute_completed_at TIMESTAMP,
                run_generated_at TIMESTAMP,
                
                -- Code and SQL
                compiled_code TEXT,
                compiled_sql TEXT,
                raw_code TEXT,
                raw_sql TEXT,
                
                -- Metadata and documentation
                description TEXT,
                comment TEXT,
                meta JSON,
                tags VARCHAR[],
                owner VARCHAR,
                package_name VARCHAR,
                
                -- Execution details
                error TEXT,
                skip BOOLEAN,
                thread_id VARCHAR,
                type VARCHAR,
                
                -- Lineage
                children_l1 VARCHAR[],
                
                -- Statistics and columns
                columns JSON,
                stats JSON,
                
                -- Legacy/additional fields
                depends_on VARCHAR[]
            )
        """)
        
        # Test data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_test_data (
                -- Core identification
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                schedule_name VARCHAR,
                
                -- Run identification
                run_id BIGINT,
                invocation_id VARCHAR,
                
                -- Test-specific information
                column_name VARCHAR,
                state VARCHAR,
                status VARCHAR,
                fail BOOLEAN,
                warn BOOLEAN,
                skip BOOLEAN,
                
                -- Execution information
                execution_time DOUBLE,
                run_elapsed_time DOUBLE,
                
                -- Timing information
                compile_started_at TIMESTAMP,
                compile_completed_at TIMESTAMP,
                execute_started_at TIMESTAMP,
                execute_completed_at TIMESTAMP,
                run_generated_at TIMESTAMP,
                
                -- Code and SQL
                compiled_code TEXT,
                compiled_sql TEXT,
                raw_code TEXT,
                raw_sql TEXT,
                
                -- Metadata and documentation
                description TEXT,
                meta JSON,
                tags VARCHAR[],
                
                -- Technical details
                language VARCHAR,
                dbt_version VARCHAR,
                thread_id VARCHAR,
                error TEXT,
                
                -- Dependencies
                depends_on VARCHAR[]
            )
        """)
        
        # Exposure data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dbt_exposure_data (
                -- Core identification
                unique_id VARCHAR,
                name VARCHAR,
                resource_type VARCHAR,
                schedule_name VARCHAR,
                
                -- Run identification
                run_id BIGINT,
                
                -- Exposure-specific information
                exposure_type VARCHAR,
                maturity VARCHAR,
                owner_name VARCHAR,
                owner_email VARCHAR,
                url VARCHAR,
                package_name VARCHAR,
                
                -- Execution information
                status VARCHAR,
                execution_time DOUBLE,
                thread_id VARCHAR,
                
                -- Timing information
                compile_started_at TIMESTAMP,
                compile_completed_at TIMESTAMP,
                execute_started_at TIMESTAMP,
                execute_completed_at TIMESTAMP,
                manifest_generated_at TIMESTAMP,
                
                -- Metadata and documentation
                description TEXT,
                meta JSON,
                tags VARCHAR[],
                
                -- Technical details
                dbt_version VARCHAR,
                
                -- Dependencies and lineage
                depends_on VARCHAR[],
                parents VARCHAR[],
                parents_models VARCHAR[],
                parents_sources VARCHAR[]
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
                'adapter_response': row.get('adapter_response', {}),
                'alias': row.get('alias'),
                'materialized_type': row.get('materialized_type'),
                'description': row.get('description', ''),
                'access': row.get('access'),
                'language': row.get('language'),
                'package_name': row.get('package_name'),
                'owner': row.get('owner'),
                'compiled_sql': row.get('compiled_sql'),
                'raw_sql': row.get('raw_sql'),
                'columns': row.get('columns', {}),
                'children_l1': row.get('children', []),
                'parents_models': row.get('parents_models', []),
                'parents_sources': row.get('parents_sources', []),
                'original_file_path': row.get('original_file_path'),
                'root_path': row.get('root_path')
            }
            
            self.conn.execute("""
                INSERT INTO dbt_run_results (
                    unique_id, name, resource_type, status, execution_time, executed_at,
                    schedule_name, depends_on, error_message, schema_name, database_name,
                    model_type, config, tags, meta, compile_started_at, compile_completed_at,
                    execute_started_at, execute_completed_at, thread_id, adapter_response,
                    alias, materialized_type, description, access, language, package_name,
                    owner, compiled_sql, raw_sql, columns, children_l1, parents_models,
                    parents_sources, original_file_path, root_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'],
                row_data['status'], row_data['execution_time'], row_data['executed_at'],
                row_data['schedule_name'], row_data['depends_on'], row_data['error_message'],
                row_data['schema_name'], row_data['database_name'], row_data['model_type'],
                row_data['config'], row_data['tags'], row_data['meta'],
                row_data['compile_started_at'], row_data['compile_completed_at'],
                row_data['execute_started_at'], row_data['execute_completed_at'],
                row_data['thread_id'], row_data['adapter_response'],
                row_data['alias'], row_data['materialized_type'], row_data['description'],
                row_data['access'], row_data['language'], row_data['package_name'],
                row_data['owner'], row_data['compiled_sql'], row_data['raw_sql'],
                row_data['columns'], row_data['children_l1'], row_data['parents_models'],
                row_data['parents_sources'], row_data['original_file_path'], row_data['root_path']
            ])
    
    def load_source_freshness(self, source_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load source freshness data into dbt_source_freshness_results table"""
        if not source_data:
            return
        
        self.conn.execute("DELETE FROM dbt_source_freshness_results WHERE schedule_name = ?", [schedule_name])
        
        for row in source_data:
            row_data = {
                # Core identification
                'unique_id': row.get('unique_id'),
                'source_name': row.get('source_name'),
                'name': row.get('table_name'),  # Use table_name as name
                'table_name': row.get('table_name'),
                'schedule_name': schedule_name,
                
                # Freshness information
                'freshness_status': row.get('freshness_status'),
                'freshness_checked': row.get('freshness_checked', True),
                'max_loaded_at': row.get('max_loaded_at'),
                'snapshotted_at': row.get('snapshotted_at'),
                'max_loaded_at_time_ago_in_s': row.get('max_loaded_at_time_ago_in_s'),
                'hours_since_load': row.get('hours_since_load'),
                
                # Criteria and thresholds
                'error_after_hours': row.get('error_after_hours'),
                'warn_after_hours': row.get('warn_after_hours'),
                'criteria': row.get('criteria', {}),
                
                # Database location
                'database': row.get('database'),
                'schema_name': row.get('schema'),
                'identifier': row.get('identifier'),
                
                # Metadata and documentation
                'description': row.get('description', ''),
                'source_description': row.get('source_description', ''),
                'comment': row.get('comment'),
                'meta': row.get('meta', {}),
                'tags': row.get('tags', []),
                'owner': row.get('owner'),
                'loader': row.get('loader'),
                'type': row.get('type'),
                
                # Run information
                'run_elapsed_time': row.get('run_elapsed_time'),
                'run_generated_at': row.get('run_generated_at'),
                
                # Lineage
                'children_l1': row.get('children_l1', []),
                
                # Statistics and columns
                'columns': row.get('columns', []),
                'stats': row.get('stats', []),
                'tests': row.get('tests', []),
                
                # Legacy fields
                'error_message': row.get('error_message'),
                'alert_level': row.get('alert_level')
            }
            
            self.conn.execute("""
                INSERT INTO dbt_source_freshness_results (
                    unique_id, source_name, name, table_name, schedule_name,
                    freshness_status, freshness_checked, max_loaded_at, snapshotted_at,
                    max_loaded_at_time_ago_in_s, hours_since_load,
                    error_after_hours, warn_after_hours, criteria,
                    database, schema_name, identifier,
                    description, source_description, comment, meta, tags, owner, loader, type,
                    run_elapsed_time, run_generated_at,
                    children_l1, columns, stats, tests,
                    error_message, alert_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['source_name'], row_data['name'], row_data['table_name'], row_data['schedule_name'],
                row_data['freshness_status'], row_data['freshness_checked'], row_data['max_loaded_at'], row_data['snapshotted_at'],
                row_data['max_loaded_at_time_ago_in_s'], row_data['hours_since_load'],
                row_data['error_after_hours'], row_data['warn_after_hours'], row_data['criteria'],
                row_data['database'], row_data['schema_name'], row_data['identifier'],
                row_data['description'], row_data['source_description'], row_data['comment'], row_data['meta'], 
                row_data['tags'], row_data['owner'], row_data['loader'], row_data['type'],
                row_data['run_elapsed_time'], row_data['run_generated_at'],
                row_data['children_l1'], row_data['columns'], row_data['stats'], row_data['tests'],
                row_data['error_message'], row_data['alert_level']
            ])
    
    def load_seed_data(self, seed_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load seed data into dbt_seed_data table"""
        if not seed_data:
            return
        
        # Insert data row by row to handle NULL values properly
        self.conn.execute("DELETE FROM dbt_seed_data WHERE schedule_name = ?", [schedule_name])
        
        for row in seed_data:
            # Ensure all required columns are present with proper defaults
            row_data = {
                # Core identification
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'schedule_name': schedule_name,
                
                # Database location
                'database': row.get('database'),
                'schema_name': row.get('schema_name'),
                'alias': row.get('alias'),
                
                # Execution information
                'status': row.get('status'),
                'execution_time': row.get('execution_time'),
                'run_elapsed_time': row.get('run_elapsed_time'),
                
                # Timing information
                'compile_started_at': row.get('compile_started_at'),
                'compile_completed_at': row.get('compile_completed_at'),
                'execute_started_at': row.get('execute_started_at'),
                'execute_completed_at': row.get('execute_completed_at'),
                'run_generated_at': row.get('run_generated_at'),
                
                # Code and SQL
                'compiled_code': row.get('compiled_code'),
                'compiled_sql': row.get('compiled_sql'),
                'raw_code': row.get('raw_code'),
                'raw_sql': row.get('raw_sql'),
                
                # Metadata and documentation
                'description': row.get('description', ''),
                'comment': row.get('comment'),
                'meta': row.get('meta', {}),
                'tags': row.get('tags', []),
                'owner': row.get('owner'),
                'package_name': row.get('package_name'),
                
                # Execution details
                'error': row.get('error'),
                'skip': row.get('skip', False),
                'thread_id': row.get('thread_id'),
                'type': row.get('type'),
                
                # Lineage
                'children_l1': row.get('children_l1', []),
                
                # Statistics and columns
                'columns': row.get('columns', {}),
                'stats': row.get('stats', {}),
                
                # Legacy/additional fields
                'depends_on': row.get('depends_on', [])
            }
            
            self.conn.execute("""
                INSERT INTO dbt_seed_data (
                    unique_id, name, resource_type, schedule_name,
                    database, schema_name, alias,
                    status, execution_time, run_elapsed_time,
                    compile_started_at, compile_completed_at, execute_started_at, 
                    execute_completed_at, run_generated_at,
                    compiled_code, compiled_sql, raw_code, raw_sql,
                    description, comment, meta, tags, owner, package_name,
                    error, skip, thread_id, type,
                    children_l1, columns, stats, depends_on
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'], row_data['schedule_name'],
                row_data['database'], row_data['schema_name'], row_data['alias'],
                row_data['status'], row_data['execution_time'], row_data['run_elapsed_time'],
                row_data['compile_started_at'], row_data['compile_completed_at'], row_data['execute_started_at'],
                row_data['execute_completed_at'], row_data['run_generated_at'],
                row_data['compiled_code'], row_data['compiled_sql'], row_data['raw_code'], row_data['raw_sql'],
                row_data['description'], row_data['comment'], row_data['meta'], row_data['tags'], 
                row_data['owner'], row_data['package_name'],
                row_data['error'], row_data['skip'], row_data['thread_id'], row_data['type'],
                row_data['children_l1'], row_data['columns'], row_data['stats'], row_data['depends_on']
            ])
    
    def load_snapshot_data(self, snapshot_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load snapshot data into dbt_snapshot_data table"""
        if not snapshot_data:
            return
        
        # Insert data row by row to handle NULL values properly
        self.conn.execute("DELETE FROM dbt_snapshot_data WHERE schedule_name = ?", [schedule_name])
        
        for row in snapshot_data:
            # Ensure all required columns are present with proper defaults
            row_data = {
                # Core identification
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'schedule_name': schedule_name,
                
                # Database location
                'database': row.get('database'),
                'schema_name': row.get('schema_name'),
                'alias': row.get('alias'),
                
                # Execution information
                'status': row.get('status'),
                'execution_time': row.get('execution_time'),
                'run_elapsed_time': row.get('run_elapsed_time'),
                
                # Timing information
                'compile_started_at': row.get('compile_started_at'),
                'compile_completed_at': row.get('compile_completed_at'),
                'execute_started_at': row.get('execute_started_at'),
                'execute_completed_at': row.get('execute_completed_at'),
                'run_generated_at': row.get('run_generated_at'),
                
                # Code and SQL
                'compiled_code': row.get('compiled_code'),
                'compiled_sql': row.get('compiled_sql'),
                'raw_code': row.get('raw_code'),
                'raw_sql': row.get('raw_sql'),
                
                # Metadata and documentation
                'description': row.get('description', ''),
                'comment': row.get('comment'),
                'meta': row.get('meta', {}),
                'tags': row.get('tags', []),
                'owner': row.get('owner'),
                'package_name': row.get('package_name'),
                
                # Execution details
                'error': row.get('error'),
                'skip': row.get('skip', False),
                'thread_id': row.get('thread_id'),
                'type': row.get('type'),
                
                # Lineage
                'children_l1': row.get('children_l1', []),
                'parents_models': row.get('parents_models', []),
                'parents_sources': row.get('parents_sources', []),
                
                # Statistics and columns
                'columns': row.get('columns', {}),
                'stats': row.get('stats', {}),
                
                # Legacy/additional fields
                'depends_on': row.get('depends_on', [])
            }
            
            self.conn.execute("""
                INSERT INTO dbt_snapshot_data (
                    unique_id, name, resource_type, schedule_name,
                    database, schema_name, alias,
                    status, execution_time, run_elapsed_time,
                    compile_started_at, compile_completed_at, execute_started_at, 
                    execute_completed_at, run_generated_at,
                    compiled_code, compiled_sql, raw_code, raw_sql,
                    description, comment, meta, tags, owner, package_name,
                    error, skip, thread_id, type,
                    children_l1, parents_models, parents_sources,
                    columns, stats, depends_on
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'], row_data['schedule_name'],
                row_data['database'], row_data['schema_name'], row_data['alias'],
                row_data['status'], row_data['execution_time'], row_data['run_elapsed_time'],
                row_data['compile_started_at'], row_data['compile_completed_at'], row_data['execute_started_at'],
                row_data['execute_completed_at'], row_data['run_generated_at'],
                row_data['compiled_code'], row_data['compiled_sql'], row_data['raw_code'], row_data['raw_sql'],
                row_data['description'], row_data['comment'], row_data['meta'], row_data['tags'], 
                row_data['owner'], row_data['package_name'],
                row_data['error'], row_data['skip'], row_data['thread_id'], row_data['type'],
                row_data['children_l1'], row_data['parents_models'], row_data['parents_sources'],
                row_data['columns'], row_data['stats'], row_data['depends_on']
            ])
    
    def load_test_data(self, test_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load test data into dbt_test_data table"""
        if not test_data:
            return
        
        # Insert data row by row to handle NULL values properly
        self.conn.execute("DELETE FROM dbt_test_data WHERE schedule_name = ?", [schedule_name])
        
        for row in test_data:
            # Ensure all required columns are present with proper defaults
            row_data = {
                # Core identification
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'schedule_name': schedule_name,
                
                # Run identification
                'run_id': row.get('run_id'),
                'invocation_id': row.get('invocation_id'),
                
                # Test-specific information
                'column_name': row.get('column_name'),
                'state': row.get('state'),
                'status': row.get('status'),
                'fail': row.get('fail'),
                'warn': row.get('warn'),
                'skip': row.get('skip'),
                
                # Execution information
                'execution_time': row.get('execution_time'),
                'run_elapsed_time': row.get('run_elapsed_time'),
                
                # Timing information
                'compile_started_at': row.get('compile_started_at'),
                'compile_completed_at': row.get('compile_completed_at'),
                'execute_started_at': row.get('execute_started_at'),
                'execute_completed_at': row.get('execute_completed_at'),
                'run_generated_at': row.get('run_generated_at'),
                
                # Code and SQL
                'compiled_code': row.get('compiled_code'),
                'compiled_sql': row.get('compiled_sql'),
                'raw_code': row.get('raw_code'),
                'raw_sql': row.get('raw_sql'),
                
                # Metadata and documentation
                'description': row.get('description'),
                'meta': row.get('meta', {}),
                'tags': row.get('tags', []),
                
                # Technical details
                'language': row.get('language'),
                'dbt_version': row.get('dbt_version'),
                'thread_id': row.get('thread_id'),
                'error': row.get('error'),
                
                # Dependencies
                'depends_on': row.get('depends_on', [])
            }
            
            # Convert JSON-serializable fields
            if row_data['meta'] is not None:
                row_data['meta'] = json.dumps(row_data['meta'])
            if row_data['tags'] is not None:
                row_data['tags'] = json.dumps(row_data['tags'])
            if row_data['depends_on'] is not None:
                row_data['depends_on'] = json.dumps(row_data['depends_on'])
            
            self.conn.execute("""
                INSERT INTO dbt_test_data (
                    unique_id, name, resource_type, schedule_name,
                    run_id, invocation_id,
                    column_name, state, status, fail, warn, skip,
                    execution_time, run_elapsed_time,
                    compile_started_at, compile_completed_at, execute_started_at,
                    execute_completed_at, run_generated_at,
                    compiled_code, compiled_sql, raw_code, raw_sql,
                    description, meta, tags,
                    language, dbt_version, thread_id, error,
                    depends_on
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'], row_data['schedule_name'],
                row_data['run_id'], row_data['invocation_id'],
                row_data['column_name'], row_data['state'], row_data['status'], row_data['fail'], 
                row_data['warn'], row_data['skip'],
                row_data['execution_time'], row_data['run_elapsed_time'],
                row_data['compile_started_at'], row_data['compile_completed_at'], row_data['execute_started_at'],
                row_data['execute_completed_at'], row_data['run_generated_at'],
                row_data['compiled_code'], row_data['compiled_sql'], row_data['raw_code'], row_data['raw_sql'],
                row_data['description'], row_data['meta'], row_data['tags'],
                row_data['language'], row_data['dbt_version'], row_data['thread_id'], row_data['error'],
                row_data['depends_on']
            ])
    
    def load_exposure_data(self, exposure_data: List[Dict[str, Any]], schedule_name: str) -> None:
        """Load exposure data into dbt_exposure_data table"""
        if not exposure_data:
            return
        
        # Insert data row by row to handle NULL values properly
        self.conn.execute("DELETE FROM dbt_exposure_data WHERE schedule_name = ?", [schedule_name])
        
        for row in exposure_data:
            # Ensure all required columns are present with proper defaults
            row_data = {
                # Core identification
                'unique_id': row.get('unique_id'),
                'name': row.get('name'),
                'resource_type': row.get('resource_type'),
                'schedule_name': schedule_name,
                
                # Run identification
                'run_id': row.get('run_id'),
                
                # Exposure-specific information
                'exposure_type': row.get('exposure_type'),
                'maturity': row.get('maturity'),
                'owner_name': row.get('owner_name'),
                'owner_email': row.get('owner_email'),
                'url': row.get('url'),
                'package_name': row.get('package_name'),
                
                # Execution information
                'status': row.get('status'),
                'execution_time': row.get('execution_time'),
                'thread_id': row.get('thread_id'),
                
                # Timing information
                'compile_started_at': row.get('compile_started_at'),
                'compile_completed_at': row.get('compile_completed_at'),
                'execute_started_at': row.get('execute_started_at'),
                'execute_completed_at': row.get('execute_completed_at'),
                'manifest_generated_at': row.get('manifest_generated_at'),
                
                # Metadata and documentation
                'description': row.get('description'),
                'meta': row.get('meta', {}),
                'tags': row.get('tags', []),
                
                # Technical details
                'dbt_version': row.get('dbt_version'),
                
                # Dependencies and lineage
                'depends_on': row.get('depends_on', []),
                'parents': row.get('parents', []),
                'parents_models': row.get('parents_models', []),
                'parents_sources': row.get('parents_sources', [])
            }
            
            # Convert JSON-serializable fields
            if row_data['meta'] is not None:
                row_data['meta'] = json.dumps(row_data['meta'])
            if row_data['tags'] is not None:
                row_data['tags'] = json.dumps(row_data['tags'])
            if row_data['depends_on'] is not None:
                row_data['depends_on'] = json.dumps(row_data['depends_on'])
            if row_data['parents'] is not None:
                row_data['parents'] = json.dumps(row_data['parents'])
            if row_data['parents_models'] is not None:
                row_data['parents_models'] = json.dumps(row_data['parents_models'])
            if row_data['parents_sources'] is not None:
                row_data['parents_sources'] = json.dumps(row_data['parents_sources'])
            
            self.conn.execute("""
                INSERT INTO dbt_exposure_data (
                    unique_id, name, resource_type, schedule_name,
                    run_id,
                    exposure_type, maturity, owner_name, owner_email, url, package_name,
                    status, execution_time, thread_id,
                    compile_started_at, compile_completed_at, execute_started_at,
                    execute_completed_at, manifest_generated_at,
                    description, meta, tags,
                    dbt_version,
                    depends_on, parents, parents_models, parents_sources
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row_data['unique_id'], row_data['name'], row_data['resource_type'], row_data['schedule_name'],
                row_data['run_id'],
                row_data['exposure_type'], row_data['maturity'], row_data['owner_name'], row_data['owner_email'],
                row_data['url'], row_data['package_name'],
                row_data['status'], row_data['execution_time'], row_data['thread_id'],
                row_data['compile_started_at'], row_data['compile_completed_at'], row_data['execute_started_at'],
                row_data['execute_completed_at'], row_data['manifest_generated_at'],
                row_data['description'], row_data['meta'], row_data['tags'],
                row_data['dbt_version'],
                row_data['depends_on'], row_data['parents'], row_data['parents_models'], row_data['parents_sources']
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
                error_message,
                alias,
                materialized_type,
                description,
                meta,
                tags,
                owner,
                package_name,
                language,
                access,
                compiled_sql,
                raw_sql,
                columns,
                children_l1,
                parents_models,
                parents_sources
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
                error_message=model_row['error_message'],
                alias=model_row['alias'],
                materialized_type=model_row['materialized_type'],
                description=model_row['description'] or '',
                meta=json.loads(model_row['meta']) if model_row['meta'] and isinstance(model_row['meta'], str) else (model_row['meta'] if model_row['meta'] is not None else {}),
                tags=model_row['tags'] if model_row['tags'] is not None else [],
                owner=model_row['owner'],
                package_name=model_row['package_name'],
                language=model_row['language'],
                access=model_row['access'],
                compiled_sql=model_row['compiled_sql'],
                raw_sql=model_row['raw_sql'],
                columns=json.loads(model_row['columns']) if model_row['columns'] and isinstance(model_row['columns'], str) else (model_row['columns'] if model_row['columns'] is not None else {}),
                children_l1=model_row['children_l1'] if model_row['children_l1'] is not None else [],
                parents_models=model_row['parents_models'] if model_row['parents_models'] is not None else [],
                parents_sources=model_row['parents_sources'] if model_row['parents_sources'] is not None else []
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
        """Get source freshness results for a schedule with full Discovery API parity"""
        sql = """
            SELECT 
                -- Core identification
                unique_id,
                source_name,
                name,
                table_name,
                
                -- Freshness information
                freshness_status,
                freshness_checked,
                max_loaded_at,
                snapshotted_at,
                max_loaded_at_time_ago_in_s,
                COALESCE(hours_since_load, 
                    CASE 
                        WHEN max_loaded_at IS NOT NULL 
                        THEN CAST((EPOCH(NOW()) - EPOCH(max_loaded_at)) / 3600 AS DOUBLE)
                        ELSE NULL 
                    END) as hours_since_load,
                
                -- Criteria and thresholds
                error_after_hours,
                warn_after_hours,
                criteria,
                
                -- Database location
                database,
                schema_name,
                identifier,
                
                -- Metadata and documentation
                description,
                source_description,
                comment,
                meta,
                tags,
                owner,
                loader,
                type,
                
                -- Run information
                run_elapsed_time,
                run_generated_at,
                
                -- Lineage
                children_l1,
                
                -- Statistics and columns
                columns,
                stats,
                tests,
                
                -- Legacy fields
                error_message,
                alert_level
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
            # Calculate alert level for backwards compatibility
            if hasattr(row, 'alert_level') and row.alert_level:
                alert_level = row.alert_level
            elif row.freshness_status == 'error':
                alert_level = 'Critical - Data is stale'
            elif row.freshness_status == 'warn':
                alert_level = 'Warning - Data aging'
            else:
                alert_level = 'Fresh'
            
            results.append(SourceFreshness(
                # Core identification
                unique_id=row.unique_id,
                source_name=row.source_name,
                name=row.name,
                table_name=row.table_name,
                
                # Freshness information
                freshness_status=row.freshness_status,
                freshness_checked=getattr(row, 'freshness_checked', None),
                max_loaded_at=row.max_loaded_at,
                snapshotted_at=row.snapshotted_at,
                max_loaded_at_time_ago_in_s=getattr(row, 'max_loaded_at_time_ago_in_s', None),
                hours_since_load=row.hours_since_load,
                
                # Criteria and thresholds
                error_after_hours=row.error_after_hours,
                warn_after_hours=row.warn_after_hours,
                criteria=json.loads(row.criteria) if row.criteria and isinstance(row.criteria, str) else (row.criteria or {}),
                
                # Database location
                database=getattr(row, 'database', None),
                schema_name=getattr(row, 'schema_name', None),
                identifier=getattr(row, 'identifier', None),
                
                # Metadata and documentation
                description=getattr(row, 'description', '') or '',
                source_description=getattr(row, 'source_description', '') or '',
                comment=getattr(row, 'comment', None),
                meta=json.loads(row.meta) if hasattr(row, 'meta') and row.meta and isinstance(row.meta, str) else (getattr(row, 'meta', {}) or {}),
                tags=getattr(row, 'tags', []) or [],
                owner=getattr(row, 'owner', None),
                loader=getattr(row, 'loader', None),
                type=getattr(row, 'type', None),
                
                # Run information
                run_elapsed_time=getattr(row, 'run_elapsed_time', None),
                run_generated_at=getattr(row, 'run_generated_at', None),
                
                # Lineage
                children_l1=getattr(row, 'children_l1', []) or [],
                
                # Statistics and columns
                columns=json.loads(row.columns) if hasattr(row, 'columns') and row.columns and isinstance(row.columns, str) else (getattr(row, 'columns', None)),
                stats=json.loads(row.stats) if hasattr(row, 'stats') and row.stats and isinstance(row.stats, str) else (getattr(row, 'stats', None)),
                tests=getattr(row, 'tests', []) or [],
                
                # Legacy fields
                alert_level=alert_level
            ))
        
        return results
    
    def get_seed_data(self, schedule_name: str) -> List[Any]:
        """Get seed data for a schedule with full Discovery API parity"""
        from .types import SeedData
        
        sql = """
            SELECT 
                -- Core identification
                unique_id,
                name,
                resource_type,
                
                -- Database location
                database,
                schema_name,
                alias,
                
                -- Execution information
                status,
                execution_time,
                run_elapsed_time,
                
                -- Timing information
                compile_started_at,
                compile_completed_at,
                execute_started_at,
                execute_completed_at,
                run_generated_at,
                
                -- Code and SQL
                compiled_code,
                compiled_sql,
                raw_code,
                raw_sql,
                
                -- Metadata and documentation
                description,
                comment,
                meta,
                tags,
                owner,
                package_name,
                
                -- Execution details
                error,
                skip,
                thread_id,
                type,
                
                -- Lineage
                children_l1,
                
                -- Statistics and columns
                columns,
                stats,
                
                -- Legacy/additional fields
                depends_on
            FROM dbt_seed_data
            WHERE schedule_name = ?
            ORDER BY name
        """
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            results.append(SeedData(
                # Core identification
                unique_id=row.unique_id,
                name=row.name,
                resource_type=row.resource_type,
                
                # Database location
                database=getattr(row, 'database', None),
                schema_name=getattr(row, 'schema_name', None),
                alias=getattr(row, 'alias', None),
                
                # Execution information
                status=getattr(row, 'status', None),
                execution_time=getattr(row, 'execution_time', None),
                run_elapsed_time=getattr(row, 'run_elapsed_time', None),
                
                # Timing information
                compile_started_at=getattr(row, 'compile_started_at', None),
                compile_completed_at=getattr(row, 'compile_completed_at', None),
                execute_started_at=getattr(row, 'execute_started_at', None),
                execute_completed_at=getattr(row, 'execute_completed_at', None),
                run_generated_at=getattr(row, 'run_generated_at', None),
                
                # Code and SQL
                compiled_code=getattr(row, 'compiled_code', None),
                compiled_sql=getattr(row, 'compiled_sql', None),
                raw_code=getattr(row, 'raw_code', None),
                raw_sql=getattr(row, 'raw_sql', None),
                
                # Metadata and documentation
                description=getattr(row, 'description', '') or '',
                comment=getattr(row, 'comment', None),
                meta=json.loads(row.meta) if hasattr(row, 'meta') and row.meta and isinstance(row.meta, str) else (getattr(row, 'meta', {}) or {}),
                tags=getattr(row, 'tags', []) or [],
                owner=getattr(row, 'owner', None),
                package_name=getattr(row, 'package_name', None),
                
                # Execution details
                error=getattr(row, 'error', None),
                skip=getattr(row, 'skip', False),
                thread_id=getattr(row, 'thread_id', None),
                type=getattr(row, 'type', None),
                
                # Lineage
                children_l1=getattr(row, 'children_l1', []) or [],
                
                # Statistics and columns
                columns=json.loads(row.columns) if hasattr(row, 'columns') and row.columns and isinstance(row.columns, str) else (getattr(row, 'columns', None)),
                stats=json.loads(row.stats) if hasattr(row, 'stats') and row.stats and isinstance(row.stats, str) else (getattr(row, 'stats', None)),
                
                # Legacy/additional fields
                depends_on=getattr(row, 'depends_on', []) or []
            ))
        
        return results
    
    def get_snapshot_data(self, schedule_name: str) -> List[Any]:
        """Get snapshot data for a schedule with full Discovery API parity"""
        from .types import SnapshotData
        
        sql = """
            SELECT 
                -- Core identification
                unique_id,
                name,
                resource_type,
                
                -- Database location
                database,
                schema_name,
                alias,
                
                -- Execution information
                status,
                execution_time,
                run_elapsed_time,
                
                -- Timing information
                compile_started_at,
                compile_completed_at,
                execute_started_at,
                execute_completed_at,
                run_generated_at,
                
                -- Code and SQL
                compiled_code,
                compiled_sql,
                raw_code,
                raw_sql,
                
                -- Metadata and documentation
                description,
                comment,
                meta,
                tags,
                owner,
                package_name,
                
                -- Execution details
                error,
                skip,
                thread_id,
                type,
                
                -- Lineage
                children_l1,
                parents_models,
                parents_sources,
                
                -- Statistics and columns
                columns,
                stats,
                
                -- Legacy/additional fields
                depends_on
            FROM dbt_snapshot_data
            WHERE schedule_name = ?
            ORDER BY name
        """
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            results.append(SnapshotData(
                # Core identification
                unique_id=row.unique_id,
                name=row.name,
                resource_type=row.resource_type,
                
                # Database location
                database=getattr(row, 'database', None),
                schema_name=getattr(row, 'schema_name', None),
                alias=getattr(row, 'alias', None),
                
                # Execution information
                status=getattr(row, 'status', None),
                execution_time=getattr(row, 'execution_time', None),
                run_elapsed_time=getattr(row, 'run_elapsed_time', None),
                
                # Timing information
                compile_started_at=getattr(row, 'compile_started_at', None),
                compile_completed_at=getattr(row, 'compile_completed_at', None),
                execute_started_at=getattr(row, 'execute_started_at', None),
                execute_completed_at=getattr(row, 'execute_completed_at', None),
                run_generated_at=getattr(row, 'run_generated_at', None),
                
                # Code and SQL
                compiled_code=getattr(row, 'compiled_code', None),
                compiled_sql=getattr(row, 'compiled_sql', None),
                raw_code=getattr(row, 'raw_code', None),
                raw_sql=getattr(row, 'raw_sql', None),
                
                # Metadata and documentation
                description=getattr(row, 'description', '') or '',
                comment=getattr(row, 'comment', None),
                meta=json.loads(row.meta) if hasattr(row, 'meta') and row.meta and isinstance(row.meta, str) else (getattr(row, 'meta', {}) or {}),
                tags=getattr(row, 'tags', []) or [],
                owner=getattr(row, 'owner', None),
                package_name=getattr(row, 'package_name', None),
                
                # Execution details
                error=getattr(row, 'error', None),
                skip=getattr(row, 'skip', False),
                thread_id=getattr(row, 'thread_id', None),
                type=getattr(row, 'type', None),
                
                # Lineage
                children_l1=getattr(row, 'children_l1', []) or [],
                parents_models=getattr(row, 'parents_models', []) or [],
                parents_sources=getattr(row, 'parents_sources', []) or [],
                
                # Statistics and columns
                columns=json.loads(row.columns) if hasattr(row, 'columns') and row.columns and isinstance(row.columns, str) else (getattr(row, 'columns', None)),
                stats=json.loads(row.stats) if hasattr(row, 'stats') and row.stats and isinstance(row.stats, str) else (getattr(row, 'stats', None)),
                
                # Legacy/additional fields
                depends_on=getattr(row, 'depends_on', []) or []
            ))
        
        return results
    
    def get_test_data(self, schedule_name: str) -> List[Any]:
        """Get test data for a schedule with full Discovery API parity"""
        from .types import TestData
        
        sql = """
            SELECT 
                -- Core identification
                unique_id,
                name,
                resource_type,
                
                -- Run identification
                run_id,
                invocation_id,
                
                -- Test-specific information
                column_name,
                state,
                status,
                fail,
                warn,
                skip,
                
                -- Execution information
                execution_time,
                run_elapsed_time,
                
                -- Timing information
                compile_started_at,
                compile_completed_at,
                execute_started_at,
                execute_completed_at,
                run_generated_at,
                
                -- Code and SQL
                compiled_code,
                compiled_sql,
                raw_code,
                raw_sql,
                
                -- Metadata and documentation
                description,
                meta,
                tags,
                
                -- Technical details
                language,
                dbt_version,
                thread_id,
                error,
                
                -- Dependencies
                depends_on
            FROM dbt_test_data
            WHERE schedule_name = ?
            ORDER BY name
        """
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            results.append(TestData(
                # Core identification
                unique_id=row.unique_id,
                name=getattr(row, 'name', None),
                resource_type=getattr(row, 'resource_type', 'test'),
                
                # Run identification
                run_id=int(row.run_id) if hasattr(row, 'run_id') and row.run_id is not None and not pd.isna(row.run_id) else None,
                invocation_id=getattr(row, 'invocation_id', None),
                
                # Test-specific information
                column_name=getattr(row, 'column_name', None),
                state=getattr(row, 'state', None),
                status=getattr(row, 'status', None),
                fail=bool(row.fail) if hasattr(row, 'fail') and row.fail is not None else None,
                warn=bool(row.warn) if hasattr(row, 'warn') and row.warn is not None else None,
                skip=bool(row.skip) if hasattr(row, 'skip') and row.skip is not None else None,
                
                # Execution information
                execution_time=getattr(row, 'execution_time', None),
                run_elapsed_time=getattr(row, 'run_elapsed_time', None),
                
                # Timing information
                compile_started_at=getattr(row, 'compile_started_at', None),
                compile_completed_at=getattr(row, 'compile_completed_at', None),
                execute_started_at=getattr(row, 'execute_started_at', None),
                execute_completed_at=getattr(row, 'execute_completed_at', None),
                run_generated_at=getattr(row, 'run_generated_at', None),
                
                # Code and SQL
                compiled_code=getattr(row, 'compiled_code', None),
                compiled_sql=getattr(row, 'compiled_sql', None),
                raw_code=getattr(row, 'raw_code', None),
                raw_sql=getattr(row, 'raw_sql', None),
                
                # Metadata and documentation
                description=getattr(row, 'description', None),
                meta=json.loads(row.meta) if hasattr(row, 'meta') and row.meta and isinstance(row.meta, str) else (getattr(row, 'meta', {}) or {}),
                tags=json.loads(row.tags) if hasattr(row, 'tags') and row.tags and isinstance(row.tags, str) else (getattr(row, 'tags', []) or []),
                
                # Technical details
                language=getattr(row, 'language', None),
                dbt_version=getattr(row, 'dbt_version', None),
                thread_id=getattr(row, 'thread_id', None),
                error=getattr(row, 'error', None),
                
                # Dependencies
                depends_on=json.loads(row.depends_on) if hasattr(row, 'depends_on') and row.depends_on and isinstance(row.depends_on, str) else (getattr(row, 'depends_on', []) or [])
            ))
        
        return results
    
    def get_exposure_data(self, schedule_name: str) -> List[Any]:
        """Get exposure data for a schedule with full Discovery API parity"""
        from .types import ExposureData
        
        sql = """
            SELECT 
                -- Core identification
                unique_id,
                name,
                resource_type,
                
                -- Run identification
                run_id,
                
                -- Exposure-specific information
                exposure_type,
                maturity,
                owner_name,
                owner_email,
                url,
                package_name,
                
                -- Execution information
                status,
                execution_time,
                thread_id,
                
                -- Timing information
                compile_started_at,
                compile_completed_at,
                execute_started_at,
                execute_completed_at,
                manifest_generated_at,
                
                -- Metadata and documentation
                description,
                meta,
                tags,
                
                -- Technical details
                dbt_version,
                
                -- Dependencies and lineage
                depends_on,
                parents,
                parents_models,
                parents_sources
            FROM dbt_exposure_data
            WHERE schedule_name = ?
            ORDER BY name
        """
        
        df = self.conn.execute(sql, [schedule_name]).df()
        
        results = []
        for row in df.itertuples(index=False):
            results.append(ExposureData(
                # Core identification
                unique_id=row.unique_id,
                name=getattr(row, 'name', None),
                resource_type=getattr(row, 'resource_type', 'exposure'),
                
                # Run identification
                run_id=int(row.run_id) if hasattr(row, 'run_id') and row.run_id is not None and not pd.isna(row.run_id) else None,
                
                # Exposure-specific information
                exposure_type=getattr(row, 'exposure_type', None),
                maturity=getattr(row, 'maturity', None),
                owner_name=getattr(row, 'owner_name', None),
                owner_email=getattr(row, 'owner_email', None),
                url=getattr(row, 'url', None),
                package_name=getattr(row, 'package_name', None),
                
                # Execution information
                status=getattr(row, 'status', None),
                execution_time=getattr(row, 'execution_time', None),
                thread_id=getattr(row, 'thread_id', None),
                
                # Timing information
                compile_started_at=getattr(row, 'compile_started_at', None),
                compile_completed_at=getattr(row, 'compile_completed_at', None),
                execute_started_at=getattr(row, 'execute_started_at', None),
                execute_completed_at=getattr(row, 'execute_completed_at', None),
                manifest_generated_at=getattr(row, 'manifest_generated_at', None),
                
                # Metadata and documentation
                description=getattr(row, 'description', None),
                meta=json.loads(row.meta) if hasattr(row, 'meta') and row.meta and isinstance(row.meta, str) else (getattr(row, 'meta', {}) or {}),
                tags=json.loads(row.tags) if hasattr(row, 'tags') and row.tags and isinstance(row.tags, str) else (getattr(row, 'tags', []) or []),
                
                # Technical details
                dbt_version=getattr(row, 'dbt_version', None),
                
                # Dependencies and lineage
                depends_on=json.loads(row.depends_on) if hasattr(row, 'depends_on') and row.depends_on and isinstance(row.depends_on, str) else (getattr(row, 'depends_on', []) or []),
                parents=json.loads(row.parents) if hasattr(row, 'parents') and row.parents and isinstance(row.parents, str) else (getattr(row, 'parents', []) or []),
                parents_models=json.loads(row.parents_models) if hasattr(row, 'parents_models') and row.parents_models and isinstance(row.parents_models, str) else (getattr(row, 'parents_models', []) or []),
                parents_sources=json.loads(row.parents_sources) if hasattr(row, 'parents_sources') and row.parents_sources and isinstance(row.parents_sources, str) else (getattr(row, 'parents_sources', []) or [])
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