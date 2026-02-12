from typing import Dict, List, Any, Optional
import json
from datetime import datetime

try:
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import ManifestV12
    from dbt_artifacts_parser.parsers.run_results.run_results_v6 import RunResultsV6
    from dbt_artifacts_parser.parsers.sources.sources_v3 import SourcesV3
    from dbt_artifacts_parser import parse_manifest, parse_run_results, parse_sources
except ImportError:
    # Fallback for different versions or if package not available
    ManifestV12 = None
    RunResultsV6 = None
    SourcesV3 = None
    parse_manifest = None
    parse_run_results = None
    parse_sources = None

from .types import ParsedArtifacts


class ArtifactParser:
    """Parser for dbt artifacts using dbt-artifacts-parser and backend-inspired transformation logic"""
    
    def __init__(self):
        self.use_dbt_parser = parse_manifest is not None
    
    def parse_artifacts(self, artifacts: Dict[str, dict], schedule_name: str) -> ParsedArtifacts:
        """
        Parse raw artifact dictionaries into structured objects.
        
        Args:
            artifacts: Dictionary with keys 'manifest', 'run_results', 'sources' containing raw JSON
            schedule_name: Name of the schedule these artifacts belong to
            
        Returns:
            ParsedArtifacts object with parsed components
        """
        parsed = ParsedArtifacts(schedule_name=schedule_name)
        
        if self.use_dbt_parser and parse_manifest and parse_run_results and parse_sources:
            # Use dbt-artifacts-parser for type-safe parsing
            if 'manifest' in artifacts:
                try:
                    parsed.manifest = parse_manifest(artifacts['manifest'])
                except Exception as e:
                    # Fallback to raw dict if parsing fails
                    print(f"Warning: Failed to parse manifest with dbt-artifacts-parser: {e}")
                    parsed.manifest = artifacts['manifest']
            
            if 'run_results' in artifacts:
                try:
                    parsed.run_results = parse_run_results(artifacts['run_results'])
                except Exception as e:
                    print(f"Warning: Failed to parse run_results with dbt-artifacts-parser: {e}")
                    parsed.run_results = artifacts['run_results']
            
            if 'sources' in artifacts:
                try:
                    parsed.sources = parse_sources(artifacts['sources'])
                except Exception as e:
                    print(f"Warning: Failed to parse sources with dbt-artifacts-parser: {e}")
                    parsed.sources = artifacts['sources']
        else:
            # Use raw dictionaries if dbt-artifacts-parser not available
            parsed.manifest = artifacts.get('manifest')
            parsed.run_results = artifacts.get('run_results')
            parsed.sources = artifacts.get('sources')
        
        return parsed
    
    def extract_run_results_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """
        Extract run results data for database loading.
        Adapts logic from backend analytics processing.
        """
        if not parsed_artifacts.run_results:
            return []
        
        results = []
        run_results = parsed_artifacts.run_results
        
        # Handle both parsed objects and raw dictionaries
        if hasattr(run_results, 'results'):
            # Parsed object
            results_list = run_results.results
            metadata = run_results.metadata if hasattr(run_results, 'metadata') else {}
        else:
            # Raw dictionary
            results_list = run_results.get('results', [])
            metadata = run_results.get('metadata', {})
        
        extracted_data = []
        
        for result in results_list:
            # Handle both parsed objects and raw dictionaries
            if hasattr(result, 'unique_id'):
                # Parsed object
                unique_id = result.unique_id
                status = result.status
                execution_time = getattr(result, 'execution_time', None)
                message = getattr(result, 'message', None)
                timing = getattr(result, 'timing', [])
            else:
                # Raw dictionary
                unique_id = result.get('unique_id')
                status = result.get('status')
                execution_time = result.get('execution_time')
                message = result.get('message')
                timing = result.get('timing', [])
            
            if not unique_id:
                continue
            
            # Extract resource type and name from unique_id
            parts = unique_id.split('.')
            resource_type = parts[0] if len(parts) > 0 else 'unknown'
            name = parts[-1] if len(parts) > 0 else unique_id
            
            # Extract timing information
            executed_at = None
            compile_started_at = None
            compile_completed_at = None
            execute_started_at = None
            execute_completed_at = None
            
            for time_entry in timing:
                if hasattr(time_entry, 'name'):
                    # Parsed object
                    timing_name = time_entry.name
                    started_at = getattr(time_entry, 'started_at', None)
                    completed_at = getattr(time_entry, 'completed_at', None)
                else:
                    # Raw dictionary
                    timing_name = time_entry.get('name')
                    started_at = time_entry.get('started_at')
                    completed_at = time_entry.get('completed_at')
                
                if timing_name == 'compile':
                    compile_started_at = started_at
                    compile_completed_at = completed_at
                elif timing_name == 'execute':
                    execute_started_at = started_at
                    execute_completed_at = completed_at
                    executed_at = completed_at  # Use execute completion as main timestamp
            
            # If no execute timing, use the last completed timestamp
            if not executed_at and timing:
                last_timing = timing[-1]
                if hasattr(last_timing, 'completed_at'):
                    executed_at = last_timing.completed_at
                else:
                    executed_at = last_timing.get('completed_at')
            
            # Convert string timestamps to datetime objects
            if isinstance(executed_at, str):
                try:
                    executed_at = datetime.fromisoformat(executed_at.replace('Z', '+00:00'))
                except:
                    executed_at = None
            
            extracted_data.append({
                'unique_id': unique_id,
                'name': name,
                'resource_type': resource_type,
                'status': status,
                'execution_time': execution_time,
                'executed_at': executed_at,
                'depends_on': [],  # Will be filled from manifest
                'error_message': message if status in ['error', 'fail'] else None,
                'compile_started_at': compile_started_at,
                'compile_completed_at': compile_completed_at,
                'execute_started_at': execute_started_at,
                'execute_completed_at': execute_completed_at,
                'thread_id': getattr(result, 'thread_id', None) if hasattr(result, 'thread_id') else result.get('thread_id'),
                'adapter_response': getattr(result, 'adapter_response', None) if hasattr(result, 'adapter_response') else result.get('adapter_response')
            })
        
        return extracted_data
    
    def extract_source_freshness_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract source freshness data for database loading"""
        if not parsed_artifacts.sources:
            return []
        
        sources = parsed_artifacts.sources
        extracted_data = []
        
        # Handle both parsed objects and raw dictionaries
        if hasattr(sources, 'results'):
            # Parsed object
            results_list = sources.results
        else:
            # Raw dictionary
            results_list = sources.get('results', [])
        
        for result in results_list:
            if hasattr(result, 'unique_id'):
                # Parsed object
                unique_id = result.unique_id
                status = result.status
                max_loaded_at = getattr(result, 'max_loaded_at', None)
                snapshotted_at = getattr(result, 'snapshotted_at', None)
                criteria = getattr(result, 'criteria', {})
                error_message = getattr(result, 'error', None)
            else:
                # Raw dictionary
                unique_id = result.get('unique_id')
                status = result.get('status')
                max_loaded_at = result.get('max_loaded_at')
                snapshotted_at = result.get('snapshotted_at')
                criteria = result.get('criteria', {})
                error_message = result.get('error')
            
            if not unique_id:
                continue
            
            # Extract source and table name from unique_id
            parts = unique_id.split('.')
            source_name = parts[1] if len(parts) > 1 else 'unknown'
            table_name = parts[2] if len(parts) > 2 else 'unknown'
            
            # Extract criteria information
            error_after_hours = None
            warn_after_hours = None
            
            if criteria:
                if hasattr(criteria, 'error_after'):
                    error_after = criteria.error_after
                    if hasattr(error_after, 'count'):
                        error_after_hours = error_after.count
                elif 'error_after' in criteria:
                    error_after = criteria['error_after']
                    error_after_hours = error_after.get('count') if isinstance(error_after, dict) else None
                
                if hasattr(criteria, 'warn_after'):
                    warn_after = criteria.warn_after
                    if hasattr(warn_after, 'count'):
                        warn_after_hours = warn_after.count
                elif 'warn_after' in criteria:
                    warn_after = criteria['warn_after']
                    warn_after_hours = warn_after.get('count') if isinstance(warn_after, dict) else None
            
            # Convert timestamps
            if isinstance(max_loaded_at, str):
                try:
                    max_loaded_at = datetime.fromisoformat(max_loaded_at.replace('Z', '+00:00'))
                except:
                    max_loaded_at = None
            
            if isinstance(snapshotted_at, str):
                try:
                    snapshotted_at = datetime.fromisoformat(snapshotted_at.replace('Z', '+00:00'))
                except:
                    snapshotted_at = None
            
            extracted_data.append({
                'source_name': source_name,
                'table_name': table_name,
                'unique_id': unique_id,
                'freshness_status': status,
                'max_loaded_at': max_loaded_at,
                'snapshotted_at': snapshotted_at,
                'error_after_hours': error_after_hours,
                'warn_after_hours': warn_after_hours,
                'error_message': str(error_message) if error_message else None,
                'criteria': criteria
            })
        
        return extracted_data
    
    def extract_model_metadata(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract model metadata and dependencies from manifest"""
        if not parsed_artifacts.manifest:
            return []
        
        manifest = parsed_artifacts.manifest
        extracted_data = []
        
        # Handle both parsed objects and raw dictionaries
        if hasattr(manifest, 'nodes'):
            # Parsed object
            nodes = manifest.nodes
        else:
            # Raw dictionary
            nodes = manifest.get('nodes', {})
        
        for unique_id, node in nodes.items():
            if hasattr(node, 'resource_type'):
                # Parsed object
                resource_type = node.resource_type
                name = node.name
                depends_on = list(getattr(node, 'depends_on', {}).get('nodes', [])) if hasattr(getattr(node, 'depends_on', {}), 'get') else []
                config = getattr(node, 'config', {})
                tags = list(getattr(node, 'tags', []))
                meta = getattr(node, 'meta', {})
                description = getattr(node, 'description', '')
                schema_name = getattr(node, 'schema', None)
                database_name = getattr(node, 'database', None)
            else:
                # Raw dictionary
                resource_type = node.get('resource_type')
                name = node.get('name')
                depends_on_data = node.get('depends_on', {})
                depends_on = depends_on_data.get('nodes', []) if isinstance(depends_on_data, dict) else []
                config = node.get('config', {})
                tags = node.get('tags', [])
                meta = node.get('meta', {})
                description = node.get('description', '')
                schema_name = node.get('schema')
                database_name = node.get('database')
            
            if not resource_type:
                continue
            
            extracted_data.append({
                'unique_id': unique_id,
                'name': name,
                'resource_type': resource_type,
                'depends_on': depends_on,
                'config': config,
                'tags': tags,
                'meta': meta,
                'description': description,
                'schema_name': schema_name,
                'database_name': database_name,
                'columns': {},  # Could extract column info if needed
                'parents': depends_on,  # Same as depends_on for now
                'children': [],  # Would need reverse lookup
                'original_file_path': getattr(node, 'original_file_path', None) if hasattr(node, 'original_file_path') else node.get('original_file_path'),
                'root_path': getattr(node, 'root_path', None) if hasattr(node, 'root_path') else node.get('root_path')
            })
        
        return extracted_data
    
    def enrich_run_results_with_manifest(self, run_results_data: List[Dict[str, Any]], 
                                        model_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich run results with metadata from manifest"""
        metadata_by_id = {m['unique_id']: m for m in model_metadata}
        
        for result in run_results_data:
            unique_id = result['unique_id']
            if unique_id in metadata_by_id:
                metadata = metadata_by_id[unique_id]
                result.update({
                    'depends_on': metadata.get('depends_on', []),
                    'schema_name': metadata.get('schema_name'),
                    'database_name': metadata.get('database_name'),
                    'config': metadata.get('config', {}),
                    'tags': metadata.get('tags', []),
                    'meta': metadata.get('meta', {}),
                    'model_type': metadata.get('config', {}).get('materialized', 'unknown') if isinstance(metadata.get('config', {}), dict) else 'unknown'
                })
        
        return run_results_data