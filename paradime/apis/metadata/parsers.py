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

    def __init__(self, enable_streaming: bool = True):
        self.use_dbt_parser = parse_manifest is not None
        self.enable_streaming = enable_streaming
        self._batch_size = 1000  # Default batch size for streaming

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
        """Extract source freshness"""
        if not parsed_artifacts.sources:
            return []

        sources = parsed_artifacts.sources
        manifest = parsed_artifacts.manifest
        extracted_data = []

        # Build a mapping of source metadata from manifest
        source_metadata_map = {}
        if manifest:
            manifest_sources = manifest.sources if hasattr(manifest, 'sources') else manifest.get('sources', {})
            for source_id, source_node in manifest_sources.items():
                if hasattr(source_node, 'unique_id'):
                    # Parsed object
                    metadata = {
                        'database': getattr(source_node, 'database', None),
                        'schema_name': getattr(source_node, 'schema', None),
                        'identifier': getattr(source_node, 'identifier', None),
                        'description': getattr(source_node, 'description', ''),
                        'source_description': getattr(source_node, 'source_description', ''),
                        'comment': getattr(source_node, 'comment', None),
                        'meta': getattr(source_node, 'meta', {}),
                        'tags': list(getattr(source_node, 'tags', [])),
                        'owner': getattr(source_node, 'meta', {}).get('owner') if isinstance(getattr(source_node, 'meta', {}), dict) else None,
                        'loader': getattr(source_node, 'loader', None),
                        'source_name': getattr(source_node, 'source_name', None),
                        'columns': dict(getattr(source_node, 'columns', {})),
                    }
                else:
                    # Raw dictionary
                    metadata = {
                        'database': source_node.get('database'),
                        'schema_name': source_node.get('schema'),
                        'identifier': source_node.get('identifier'),
                        'description': source_node.get('description', ''),
                        'source_description': source_node.get('source_description', ''),
                        'comment': source_node.get('comment'),
                        'meta': source_node.get('meta', {}),
                        'tags': source_node.get('tags', []),
                        'owner': source_node.get('meta', {}).get('owner') if isinstance(source_node.get('meta', {}), dict) else None,
                        'loader': source_node.get('loader'),
                        'source_name': source_node.get('source_name'),
                        'columns': source_node.get('columns', {}),
                    }
                source_metadata_map[source_id] = metadata

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

            # Calculate additional dbt Discovery API fields
            max_loaded_at_time_ago_in_s = None
            hours_since_load = None
            freshness_checked = status is not None

            if max_loaded_at and snapshotted_at:
                # Calculate time difference in seconds
                time_diff = snapshotted_at - max_loaded_at
                max_loaded_at_time_ago_in_s = time_diff.total_seconds()
                hours_since_load = max_loaded_at_time_ago_in_s / 3600.0

            # Get additional metadata from manifest
            source_metadata = source_metadata_map.get(unique_id, {})

            extracted_data.append({
                # Core identification
                'unique_id': unique_id,
                'source_name': source_name,
                'table_name': table_name,

                # Freshness information
                'freshness_status': status,
                'freshness_checked': freshness_checked,
                'max_loaded_at': max_loaded_at,
                'snapshotted_at': snapshotted_at,
                'max_loaded_at_time_ago_in_s': max_loaded_at_time_ago_in_s,
                'hours_since_load': hours_since_load,

                # Criteria and thresholds
                'error_after_hours': error_after_hours,
                'warn_after_hours': warn_after_hours,
                'criteria': criteria,

                # Database location (from manifest)
                'database': source_metadata.get('database'),
                'schema_name': source_metadata.get('schema_name'),
                'identifier': source_metadata.get('identifier'),

                # Metadata and documentation (from manifest)
                'description': source_metadata.get('description', ''),
                'source_description': source_metadata.get('source_description', ''),
                'comment': source_metadata.get('comment'),
                'meta': source_metadata.get('meta', {}),
                'tags': source_metadata.get('tags', []),
                'owner': source_metadata.get('owner'),
                'loader': source_metadata.get('loader'),
                'type': 'table',  # Default type for sources

                # Statistics and columns (from manifest)
                'columns': source_metadata.get('columns', {}),
                'stats': [],  # Will be populated if available from catalog
                'tests': [],  # Will be populated from test results

                # Lineage (children who depend on this source)
                'children_l1': [],  # Will be computed from dependencies

                # Legacy fields
                'error_message': str(error_message) if error_message else None
            })

        return extracted_data

    def extract_seed_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract seed metadata"""
        if not parsed_artifacts.manifest and not parsed_artifacts.run_results:
            return []

        extracted_data = []

        # Get seed nodes from manifest
        manifest = parsed_artifacts.manifest
        run_results = parsed_artifacts.run_results

        # Build run results lookup by unique_id
        run_results_map = {}
        if run_results:
            results_list = run_results.results if hasattr(run_results, 'results') else run_results.get('results', [])
            for result in results_list:
                if hasattr(result, 'unique_id'):
                    # Parsed object
                    unique_id = result.unique_id
                    status = result.status
                    execution_time = getattr(result, 'execution_time', None)
                    error = getattr(result, 'message', None)
                    timing = getattr(result, 'timing', [])
                    thread_id = getattr(result, 'thread_id', None)
                else:
                    # Raw dictionary
                    unique_id = result.get('unique_id')
                    status = result.get('status')
                    execution_time = result.get('execution_time')
                    error = result.get('message')
                    timing = result.get('timing', [])
                    thread_id = result.get('thread_id')

                if not unique_id or not unique_id.startswith('seed.'):
                    continue

                # Extract timing information
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

                run_results_map[unique_id] = {
                    'status': status,
                    'execution_time': execution_time,
                    'error': error,
                    'thread_id': thread_id,
                    'compile_started_at': compile_started_at,
                    'compile_completed_at': compile_completed_at,
                    'execute_started_at': execute_started_at,
                    'execute_completed_at': execute_completed_at
                }

        # Extract seed metadata from manifest
        if manifest:
            manifest_nodes = manifest.nodes if hasattr(manifest, 'nodes') else manifest.get('nodes', {})
            for unique_id, node in manifest_nodes.items():
                # Only process seed nodes
                resource_type = node.resource_type if hasattr(node, 'resource_type') else node.get('resource_type')
                if resource_type != 'seed':
                    continue

                if hasattr(node, 'name'):
                    # Parsed object
                    name = node.name
                    database = getattr(node, 'database', None)
                    schema = getattr(node, 'schema', None)
                    alias = getattr(node, 'alias', None)
                    description = getattr(node, 'description', '')
                    comment = getattr(node, 'comment', None)
                    meta = getattr(node, 'meta', {})
                    tags = list(getattr(node, 'tags', []))
                    owner = meta.get('owner') if isinstance(meta, dict) else None
                    package_name = getattr(node, 'package_name', None)
                    compiled_code = getattr(node, 'compiled_code', None) or getattr(node, 'compiled_sql', None)
                    raw_code = getattr(node, 'raw_code', None) or getattr(node, 'raw_sql', None)
                    columns = dict(getattr(node, 'columns', {}))
                else:
                    # Raw dictionary
                    name = node.get('name')
                    database = node.get('database')
                    schema = node.get('schema')
                    alias = node.get('alias')
                    description = node.get('description', '')
                    comment = node.get('comment')
                    meta = node.get('meta', {})
                    tags = node.get('tags', [])
                    owner = meta.get('owner') if isinstance(meta, dict) else None
                    package_name = node.get('package_name')
                    compiled_code = node.get('compiled_code') or node.get('compiled_sql')
                    raw_code = node.get('raw_code') or node.get('raw_sql')
                    columns = node.get('columns', {})

                # Get run results data if available
                run_data = run_results_map.get(unique_id, {})

                extracted_data.append({
                    # Core identification
                    'unique_id': unique_id,
                    'name': name,
                    'resource_type': resource_type,

                    # Database location
                    'database': database,
                    'schema_name': schema,
                    'alias': alias,

                    # Execution information
                    'status': run_data.get('status'),
                    'execution_time': run_data.get('execution_time'),
                    'run_elapsed_time': run_data.get('execution_time'),  # Same as execution_time for seeds

                    # Timing information
                    'compile_started_at': run_data.get('compile_started_at'),
                    'compile_completed_at': run_data.get('compile_completed_at'),
                    'execute_started_at': run_data.get('execute_started_at'),
                    'execute_completed_at': run_data.get('execute_completed_at'),
                    'run_generated_at': run_data.get('execute_completed_at'),  # Use execute completion as run generation

                    # Code and SQL
                    'compiled_code': compiled_code,
                    'compiled_sql': compiled_code,  # Same as compiled_code
                    'raw_code': raw_code,
                    'raw_sql': raw_code,  # Same as raw_code

                    # Metadata and documentation
                    'description': description,
                    'comment': comment,
                    'meta': meta,
                    'tags': tags,
                    'owner': owner,
                    'package_name': package_name,

                    # Execution details
                    'error': run_data.get('error'),
                    'skip': run_data.get('status') == 'skipped',
                    'thread_id': run_data.get('thread_id'),
                    'type': 'seed',  # Type is always 'seed' for seeds

                    # Lineage (will be computed)
                    'children_l1': [],  # Will be computed from dependency graph

                    # Statistics and columns
                    'columns': columns,
                    'stats': {},  # Will be populated if available

                    # Legacy/additional fields
                    'depends_on': []  # Seeds typically don't depend on other models
                })

        return extracted_data

    def extract_snapshot_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract snapshot"""
        if not parsed_artifacts.manifest and not parsed_artifacts.run_results:
            return []

        extracted_data = []

        # Get snapshot nodes from manifest
        manifest = parsed_artifacts.manifest
        run_results = parsed_artifacts.run_results

        # Build run results lookup by unique_id
        run_results_map = {}
        if run_results:
            results_list = run_results.results if hasattr(run_results, 'results') else run_results.get('results', [])
            for result in results_list:
                if hasattr(result, 'unique_id'):
                    # Parsed object
                    unique_id = result.unique_id
                    status = result.status
                    execution_time = getattr(result, 'execution_time', None)
                    error = getattr(result, 'message', None)
                    timing = getattr(result, 'timing', [])
                    thread_id = getattr(result, 'thread_id', None)
                else:
                    # Raw dictionary
                    unique_id = result.get('unique_id')
                    status = result.get('status')
                    execution_time = result.get('execution_time')
                    error = result.get('message')
                    timing = result.get('timing', [])
                    thread_id = result.get('thread_id')

                if not unique_id or not unique_id.startswith('snapshot.'):
                    continue

                # Extract timing information
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

                run_results_map[unique_id] = {
                    'status': status,
                    'execution_time': execution_time,
                    'error': error,
                    'thread_id': thread_id,
                    'compile_started_at': compile_started_at,
                    'compile_completed_at': compile_completed_at,
                    'execute_started_at': execute_started_at,
                    'execute_completed_at': execute_completed_at
                }

        # Extract snapshot metadata from manifest
        if manifest:
            manifest_nodes = manifest.nodes if hasattr(manifest, 'nodes') else manifest.get('nodes', {})
            for unique_id, node in manifest_nodes.items():
                # Only process snapshot nodes
                resource_type = node.resource_type if hasattr(node, 'resource_type') else node.get('resource_type')
                if resource_type != 'snapshot':
                    continue

                if hasattr(node, 'name'):
                    # Parsed object
                    name = node.name
                    database = getattr(node, 'database', None)
                    schema = getattr(node, 'schema', None)
                    alias = getattr(node, 'alias', None)
                    description = getattr(node, 'description', '')
                    comment = getattr(node, 'comment', None)
                    meta = getattr(node, 'meta', {})
                    tags = list(getattr(node, 'tags', []))
                    owner = meta.get('owner') if isinstance(meta, dict) else None
                    package_name = getattr(node, 'package_name', None)
                    compiled_code = getattr(node, 'compiled_code', None) or getattr(node, 'compiled_sql', None)
                    raw_code = getattr(node, 'raw_code', None) or getattr(node, 'raw_sql', None)
                    columns = dict(getattr(node, 'columns', {}))
                    depends_on = list(getattr(node, 'depends_on', {}).get('nodes', [])) if hasattr(getattr(node, 'depends_on', {}), 'get') else []
                else:
                    # Raw dictionary
                    name = node.get('name')
                    database = node.get('database')
                    schema = node.get('schema')
                    alias = node.get('alias')
                    description = node.get('description', '')
                    comment = node.get('comment')
                    meta = node.get('meta', {})
                    tags = node.get('tags', [])
                    owner = meta.get('owner') if isinstance(meta, dict) else None
                    package_name = node.get('package_name')
                    compiled_code = node.get('compiled_code') or node.get('compiled_sql')
                    raw_code = node.get('raw_code') or node.get('raw_sql')
                    columns = node.get('columns', {})
                    depends_on_data = node.get('depends_on', {})
                    depends_on = depends_on_data.get('nodes', []) if isinstance(depends_on_data, dict) else []

                # Get run results data if available
                run_data = run_results_map.get(unique_id, {})

                # Separate parents by type
                parents_models = []
                parents_sources = []

                for parent_id in depends_on:
                    if parent_id.startswith('model.'):
                        parents_models.append(parent_id)
                    elif parent_id.startswith('source.'):
                        parents_sources.append(parent_id)
                    else:
                        # For other types, add to models by default
                        parents_models.append(parent_id)

                extracted_data.append({
                    # Core identification
                    'unique_id': unique_id,
                    'name': name,
                    'resource_type': resource_type,

                    # Database location
                    'database': database,
                    'schema_name': schema,
                    'alias': alias,

                    # Execution information
                    'status': run_data.get('status'),
                    'execution_time': run_data.get('execution_time'),
                    'run_elapsed_time': run_data.get('execution_time'),  # Same as execution_time for snapshots

                    # Timing information
                    'compile_started_at': run_data.get('compile_started_at'),
                    'compile_completed_at': run_data.get('compile_completed_at'),
                    'execute_started_at': run_data.get('execute_started_at'),
                    'execute_completed_at': run_data.get('execute_completed_at'),
                    'run_generated_at': run_data.get('execute_completed_at'),  # Use execute completion as run generation

                    # Code and SQL
                    'compiled_code': compiled_code,
                    'compiled_sql': compiled_code,  # Same as compiled_code
                    'raw_code': raw_code,
                    'raw_sql': raw_code,  # Same as raw_code

                    # Metadata and documentation
                    'description': description,
                    'comment': comment,
                    'meta': meta,
                    'tags': tags,
                    'owner': owner,
                    'package_name': package_name,

                    # Execution details
                    'error': run_data.get('error'),
                    'skip': run_data.get('status') == 'skipped',
                    'thread_id': run_data.get('thread_id'),
                    'type': 'snapshot',  # Type is always 'snapshot' for snapshots

                    # Lineage
                    'children_l1': [],  # Will be computed from dependency graph
                    'parents_models': parents_models,
                    'parents_sources': parents_sources,

                    # Statistics and columns
                    'columns': columns,
                    'stats': {},  # Will be populated if available

                    # Legacy/additional fields
                    'depends_on': depends_on
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
                alias = getattr(node, 'alias', None)
                materialized = config.get('materialized') if isinstance(config, dict) else getattr(config, 'materialized', None)
                access = getattr(node, 'access', None)
                language = getattr(node, 'language', 'sql')
                package_name = getattr(node, 'package_name', None)
                owner = meta.get('owner') if isinstance(meta, dict) else None
                compiled_code = getattr(node, 'compiled_code', None) or getattr(node, 'compiled_sql', None)
                raw_code = getattr(node, 'raw_code', None) or getattr(node, 'raw_sql', None)
                columns = dict(getattr(node, 'columns', {}))
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
                alias = node.get('alias')
                materialized = config.get('materialized') if isinstance(config, dict) else None
                access = node.get('access')
                language = node.get('language', 'sql')
                package_name = node.get('package_name')
                owner = meta.get('owner') if isinstance(meta, dict) else None
                compiled_code = node.get('compiled_code') or node.get('compiled_sql')
                raw_code = node.get('raw_code') or node.get('raw_sql')
                columns = node.get('columns', {})

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
                'alias': alias,
                'materialized_type': materialized,
                'access': access,
                'language': language,
                'package_name': package_name,
                'owner': owner,
                'compiled_sql': compiled_code,
                'raw_sql': raw_code,
                'columns': columns,
                'parents': depends_on,  # Will be split into models/sources
                'children': [],  # Will be computed from reverse lookup
                'original_file_path': getattr(node, 'original_file_path', None) if hasattr(node, 'original_file_path') else node.get('original_file_path'),
                'root_path': getattr(node, 'root_path', None) if hasattr(node, 'root_path') else node.get('root_path')
            })

        # Compute children relationships and separate parents by type
        extracted_data = self._compute_dependency_graph(extracted_data)

        return extracted_data

    def _compute_dependency_graph(self, model_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Compute children relationships and separate parents by type"""
        models_by_id = {item['unique_id']: item for item in model_data}

        # Build children mapping
        children_map = {}
        for item in model_data:
            unique_id = item['unique_id']
            children_map[unique_id] = []

        # For each model, add itself as a child to its dependencies
        for item in model_data:
            for parent_id in item['depends_on']:
                if parent_id in children_map:
                    children_map[parent_id].append(item['unique_id'])

        # Update each model with computed relationships
        for item in model_data:
            # Set children (only direct children for childrenL1)
            item['children'] = children_map.get(item['unique_id'], [])

            # Separate parents by type
            parents_models = []
            parents_sources = []

            for parent_id in item['depends_on']:
                if parent_id.startswith('model.'):
                    parents_models.append(parent_id)
                elif parent_id.startswith('source.'):
                    parents_sources.append(parent_id)
                else:
                    # For other types, add to models by default
                    parents_models.append(parent_id)

            item['parents_models'] = parents_models
            item['parents_sources'] = parents_sources

        return model_data

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
                    'model_type': metadata.get('config', {}).get('materialized', 'unknown') if isinstance(metadata.get('config', {}), dict) else 'unknown',
                    'alias': metadata.get('alias'),
                    'materialized_type': metadata.get('materialized_type'),
                    'description': metadata.get('description', ''),
                    'access': metadata.get('access'),
                    'language': metadata.get('language'),
                    'package_name': metadata.get('package_name'),
                    'owner': metadata.get('owner'),
                    'compiled_sql': metadata.get('compiled_sql'),
                    'raw_sql': metadata.get('raw_sql'),
                    'columns': metadata.get('columns', {}),
                    'children': metadata.get('children', []),
                    'parents_models': metadata.get('parents_models', []),
                    'parents_sources': metadata.get('parents_sources', []),
                    'original_file_path': metadata.get('original_file_path'),
                    'root_path': metadata.get('root_path')
                })

        return run_results_data

    def extract_test_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract test metadata from manifest.json and run_results.json with complete Discovery API parity"""
        test_data = []

        if not parsed_artifacts.run_results:
            return test_data

        # Build lookup for run results
        run_results_lookup = {}
        try:
            if hasattr(parsed_artifacts.run_results, 'results'):
                results = parsed_artifacts.run_results.results
            else:
                results = parsed_artifacts.run_results.get('results', [])

            for result in results:
                if hasattr(result, 'unique_id'):
                    unique_id = result.unique_id
                else:
                    unique_id = result.get('unique_id')

                if unique_id and unique_id.startswith('test.'):
                    run_results_lookup[unique_id] = result
        except Exception as e:
            print(f"Warning: Could not parse run results for tests: {e}")

        # Extract test metadata from manifest
        manifest_tests = {}
        if parsed_artifacts.manifest:
            try:
                if hasattr(parsed_artifacts.manifest, 'nodes'):
                    nodes = parsed_artifacts.manifest.nodes
                elif isinstance(parsed_artifacts.manifest, dict):
                    nodes = parsed_artifacts.manifest.get('nodes', {})
                else:
                    nodes = {}

                # Get test nodes from manifest
                for unique_id, node in nodes.items():
                    if unique_id.startswith('test.'):
                        manifest_tests[unique_id] = node

            except Exception as e:
                print(f"Warning: Could not parse manifest for tests: {e}")

        # Process each test from run results and merge with manifest data
        for unique_id, run_result in run_results_lookup.items():
            try:
                # Get manifest metadata for this test
                manifest_test = manifest_tests.get(unique_id, {})

                # Extract basic info from run result
                if hasattr(run_result, 'status'):
                    status = run_result.status
                    execution_time = getattr(run_result, 'execution_time', None)
                    thread_id = getattr(run_result, 'thread_id', None)
                    message = getattr(run_result, 'message', None)
                    timing = getattr(run_result, 'timing', [])
                    failures = getattr(run_result, 'failures', None)
                else:
                    # Raw dictionary format
                    status = run_result.get('status')
                    execution_time = run_result.get('execution_time')
                    thread_id = run_result.get('thread_id')
                    message = run_result.get('message')
                    timing = run_result.get('timing', [])
                    failures = run_result.get('failures')

                # Extract test name from unique_id
                parts = unique_id.split('.')
                name = parts[-1] if len(parts) > 0 else 'unknown'

                # Extract manifest metadata
                if hasattr(manifest_test, 'name'):
                    manifest_name = manifest_test.name
                    description = getattr(manifest_test, 'description', None)
                    meta = getattr(manifest_test, 'meta', {})
                    tags = getattr(manifest_test, 'tags', [])
                    depends_on = getattr(manifest_test, 'depends_on', {})
                    compiled_code = getattr(manifest_test, 'compiled_code', None)
                    compiled_sql = getattr(manifest_test, 'compiled_sql', None)
                    raw_code = getattr(manifest_test, 'raw_code', None)
                    raw_sql = getattr(manifest_test, 'raw_sql', None)
                    language = getattr(manifest_test, 'language', 'sql')
                    column_name = getattr(manifest_test, 'column_name', None)
                elif isinstance(manifest_test, dict):
                    manifest_name = manifest_test.get('name', name)
                    description = manifest_test.get('description')
                    meta = manifest_test.get('meta', {})
                    tags = manifest_test.get('tags', [])
                    depends_on = manifest_test.get('depends_on', {})
                    compiled_code = manifest_test.get('compiled_code')
                    compiled_sql = manifest_test.get('compiled_sql')
                    raw_code = manifest_test.get('raw_code')
                    raw_sql = manifest_test.get('raw_sql')
                    language = manifest_test.get('language', 'sql')
                    column_name = manifest_test.get('column_name')
                else:
                    # Fallback values
                    manifest_name = name
                    description = None
                    meta = {}
                    tags = []
                    depends_on = {}
                    compiled_code = None
                    compiled_sql = None
                    raw_code = None
                    raw_sql = None
                    language = 'sql'
                    column_name = None

                # Extract dependency list
                depends_on_list = []
                if isinstance(depends_on, dict):
                    depends_on_list.extend(depends_on.get('nodes', []))
                elif isinstance(depends_on, list):
                    depends_on_list = depends_on

                # Parse timing information for Discovery API fields
                compile_started_at = None
                compile_completed_at = None
                execute_started_at = None
                execute_completed_at = None

                for time_entry in timing:
                    if hasattr(time_entry, 'name'):
                        time_name = time_entry.name
                        started_at = getattr(time_entry, 'started_at', None)
                        completed_at = getattr(time_entry, 'completed_at', None)
                    elif isinstance(time_entry, dict):
                        time_name = time_entry.get('name')
                        started_at = time_entry.get('started_at')
                        completed_at = time_entry.get('completed_at')
                    else:
                        continue

                    # Convert timestamps
                    if started_at and isinstance(started_at, str):
                        try:
                            started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                        except:
                            started_at = None

                    if completed_at and isinstance(completed_at, str):
                        try:
                            completed_at = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                        except:
                            completed_at = None

                    if time_name == 'compile':
                        compile_started_at = started_at
                        compile_completed_at = completed_at
                    elif time_name == 'execute':
                        execute_started_at = started_at
                        execute_completed_at = completed_at

                # Determine test state based on status and failures
                state = None
                fail = None
                warn = None
                skip = None
                error_message = None

                if status == 'pass':
                    state = 'pass'
                    fail = False
                elif status == 'fail':
                    state = 'fail'
                    fail = True
                elif status == 'error':
                    state = 'error'
                    error_message = str(message) if message else None
                elif status == 'warn':
                    state = 'warn'
                    warn = True
                elif status == 'skip' or status == 'skipped':
                    state = 'skip'
                    skip = True
                else:
                    state = status

                # Extract status details (number of failed rows or ERROR)
                status_detail = None
                if failures and isinstance(failures, int):
                    status_detail = str(failures)
                elif status == 'error':
                    status_detail = 'ERROR'
                elif message:
                    status_detail = str(message)

                test_record = {
                    # Core identification
                    'unique_id': unique_id,
                    'name': manifest_name,
                    'resource_type': 'test',

                    # Test-specific information
                    'column_name': column_name,
                    'state': state,
                    'status': status_detail,
                    'fail': fail,
                    'warn': warn,
                    'skip': skip,

                    # Execution information
                    'execution_time': execution_time,
                    'run_elapsed_time': execution_time,  # Same value for tests

                    # Timing information
                    'compile_started_at': compile_started_at,
                    'compile_completed_at': compile_completed_at,
                    'execute_started_at': execute_started_at,
                    'execute_completed_at': execute_completed_at,
                    'run_generated_at': execute_completed_at,  # Use execute completion as run generated

                    # Code and SQL
                    'compiled_code': compiled_code,
                    'compiled_sql': compiled_sql,
                    'raw_code': raw_code,
                    'raw_sql': raw_sql,

                    # Metadata and documentation
                    'description': description,
                    'meta': meta,
                    'tags': tags,

                    # Technical details
                    'language': language,
                    'dbt_version': None,  # Not typically available in artifacts
                    'thread_id': thread_id,
                    'error': error_message,

                    # Dependencies
                    'depends_on': depends_on_list,

                    # Run identification (will be None for local runs)
                    'run_id': None,
                    'invocation_id': None
                }

                test_data.append(test_record)

            except Exception as e:
                print(f"Warning: Could not process test {unique_id}: {e}")
                continue

        return test_data

    def extract_exposure_data(self, parsed_artifacts: ParsedArtifacts) -> List[Dict[str, Any]]:
        """Extract exposure metadata from manifest.json with complete Discovery API parity"""
        exposure_data = []

        if not parsed_artifacts.manifest:
            return exposure_data

        # Extract exposure metadata from manifest
        manifest_exposures = {}
        try:
            if hasattr(parsed_artifacts.manifest, 'exposures'):
                exposures = parsed_artifacts.manifest.exposures
            elif isinstance(parsed_artifacts.manifest, dict):
                exposures = parsed_artifacts.manifest.get('exposures', {})
            else:
                exposures = {}

            # Get exposure nodes from manifest
            for unique_id, exposure in exposures.items():
                if unique_id.startswith('exposure.'):
                    manifest_exposures[unique_id] = exposure

        except Exception as e:
            print(f"Warning: Could not parse manifest for exposures: {e}")

        # Process each exposure from manifest
        for unique_id, exposure in manifest_exposures.items():
            try:
                # Extract basic info from exposure
                if hasattr(exposure, 'name'):
                    name = exposure.name
                    description = getattr(exposure, 'description', None)
                    meta = getattr(exposure, 'meta', {})
                    tags = getattr(exposure, 'tags', [])
                    depends_on = getattr(exposure, 'depends_on', {})
                    exposure_type = getattr(exposure, 'type', None)
                    maturity = getattr(exposure, 'maturity', None)
                    owner = getattr(exposure, 'owner', {})
                    url = getattr(exposure, 'url', None)
                    package_name = getattr(exposure, 'package_name', None)
                elif isinstance(exposure, dict):
                    name = exposure.get('name')
                    description = exposure.get('description')
                    meta = exposure.get('meta', {})
                    tags = exposure.get('tags', [])
                    depends_on = exposure.get('depends_on', {})
                    exposure_type = exposure.get('type')
                    maturity = exposure.get('maturity')
                    owner = exposure.get('owner', {})
                    url = exposure.get('url')
                    package_name = exposure.get('package_name')
                else:
                    # Fallback values
                    name = unique_id.split('.')[-1] if '.' in unique_id else 'unknown'
                    description = None
                    meta = {}
                    tags = []
                    depends_on = {}
                    exposure_type = None
                    maturity = None
                    owner = {}
                    url = None
                    package_name = None

                # Extract owner information
                owner_name = None
                owner_email = None
                if isinstance(owner, dict):
                    owner_name = owner.get('name')
                    owner_email = owner.get('email')
                elif hasattr(owner, 'name'):
                    owner_name = getattr(owner, 'name', None)
                    owner_email = getattr(owner, 'email', None)

                # Extract dependency lists
                depends_on_list = []
                parents_models = []
                parents_sources = []
                all_parents = []

                if isinstance(depends_on, dict):
                    nodes = depends_on.get('nodes', [])
                    depends_on_list.extend(nodes)
                    all_parents.extend(nodes)

                    # Separate parents by type for Discovery API
                    for parent_id in nodes:
                        if parent_id.startswith('model.'):
                            parents_models.append(parent_id)
                        elif parent_id.startswith('source.'):
                            parents_sources.append(parent_id)
                elif isinstance(depends_on, list):
                    depends_on_list = depends_on
                    all_parents = depends_on

                    # Separate parents by type
                    for parent_id in depends_on:
                        if parent_id.startswith('model.'):
                            parents_models.append(parent_id)
                        elif parent_id.startswith('source.'):
                            parents_sources.append(parent_id)

                exposure_record = {
                    # Core identification
                    'unique_id': unique_id,
                    'name': name,
                    'resource_type': 'exposure',

                    # Exposure-specific information
                    'exposure_type': exposure_type,
                    'maturity': maturity,
                    'owner_name': owner_name,
                    'owner_email': owner_email,
                    'url': url,
                    'package_name': package_name,

                    # Execution information (typically None for exposures since they don't execute)
                    'status': None,
                    'execution_time': None,
                    'thread_id': None,

                    # Timing information (typically None for exposures)
                    'compile_started_at': None,
                    'compile_completed_at': None,
                    'execute_started_at': None,
                    'execute_completed_at': None,
                    'manifest_generated_at': None,  # Could be populated from manifest metadata

                    # Metadata and documentation
                    'description': description,
                    'meta': meta,
                    'tags': tags,

                    # Technical details
                    'dbt_version': None,  # Not typically available in artifacts

                    # Dependencies and lineage
                    'depends_on': depends_on_list,
                    'parents': all_parents,
                    'parents_models': parents_models,
                    'parents_sources': parents_sources,

                    # Run identification (will be None for local runs)
                    'run_id': None
                }

                exposure_data.append(exposure_record)

            except Exception as e:
                print(f"Warning: Could not process exposure {unique_id}: {e}")
                continue

        return exposure_data
