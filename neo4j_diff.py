#!/usr/bin/env python3

import logging
from typing import Dict, List, Set, Any, Optional, Tuple
from neo4j import GraphDatabase
import json
import time
from datetime import datetime
from collections import defaultdict

# Optional tqdm import with fallback
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Simple fallback for tqdm with visual progress bars
    class tqdm:
        def __init__(self, iterable=None, desc="Processing", unit="item", total=None, leave=True, bar_format=None, **kwargs):
            self.iterable = iterable
            self.desc = desc
            self.total = total or (len(iterable) if hasattr(iterable, '__len__') else None)
            self.current = 0
            self.leave = leave
            self.last_update = 0
            
        def __iter__(self):
            if self.iterable:
                for i, item in enumerate(self.iterable):
                    self.current = i + 1
                    if i % max(1, len(self.iterable) // 20) == 0 or i == 0:
                        self._display_progress()
                    yield item
        
        def update(self, n=1):
            self.current += n
            # Update display every 5% or every 10 items, whichever is more frequent
            update_freq = max(1, min(10, self.total // 20)) if self.total else 10
            if self.current - self.last_update >= update_freq:
                self._display_progress()
                self.last_update = self.current
        
        def _display_progress(self):
            if self.total:
                progress_pct = (self.current / self.total) * 100
                bar_length = 40
                filled_length = int(bar_length * self.current // self.total)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                print(f"\r{self.desc}: {progress_pct:3.0f}%|{bar}| {self.current}/{self.total}", end='', flush=True)
            else:
                print(f"\r{self.desc}: {self.current} items processed", end='', flush=True)
        
        def set_description(self, desc):
            self.desc = desc
            
        def close(self):
            if self.total:
                bar = '█' * 40
                print(f"\r{self.desc}: 100%|{bar}| {self.total}/{self.total}")
            else:
                print(f"\n{self.desc}: Completed")
        
        def set_postfix(self, **kwargs):
            # Add postfix info to current display
            if kwargs:
                info = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                if self.total:
                    progress_pct = (self.current / self.total * 100) if self.total > 0 else 0
                    bar_length = 40
                    filled_length = int(bar_length * self.current // self.total)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    print(f"\r{self.desc}: {progress_pct:3.0f}%|{bar}| {self.current}/{self.total} | {info}", end='', flush=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jDiff:
    """Tool to compare data between two Neo4j instances"""
    
    # Define Papers with Code specific data types
    PWC_NODE_TYPES = ['Paper', 'Repository', 'Dataset', 'Task', 'Author']
    PWC_RELATIONSHIP_TYPES = ['AUTHORED', 'HAS_CODE', 'USES_DATASET', 'ADDRESSES_TASK']
    
    def __init__(self, 
                 source_uri: str, source_user: str, source_password: str,
                 target_uri: str, target_user: str, target_password: str,
                 pwc_only: bool = False):
        """Initialize connections to both Neo4j instances"""
        self.source_uri = source_uri
        self.target_uri = target_uri
        self.pwc_only = pwc_only  # If True, only compare PWC-specific data types
        
        # Connect to source instance
        try:
            self.source_driver = GraphDatabase.driver(source_uri, auth=(source_user, source_password))
            self.source_session = self.source_driver.session()
            logger.info(f"✅ Connected to source Neo4j: {source_uri}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to source Neo4j {source_uri}: {e}")
            raise
        
        # Connect to target instance
        try:
            self.target_driver = GraphDatabase.driver(target_uri, auth=(target_user, target_password))
            self.target_session = self.target_driver.session()
            logger.info(f"✅ Connected to target Neo4j: {target_uri}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to target Neo4j {target_uri}: {e}")
            raise
    
    def close(self):
        """Close all Neo4j connections"""
        if hasattr(self, 'source_session'):
            self.source_session.close()
        if hasattr(self, 'source_driver'):
            self.source_driver.close()
        if hasattr(self, 'target_session'):
            self.target_session.close()
        if hasattr(self, 'target_driver'):
            self.target_driver.close()
    
    def get_node_counts(self, session) -> Dict[str, int]:
        """Get count of each node type in the database using batch query"""
        counts = {}
        
        if self.pwc_only:
            # Only check PWC-specific node types, but first verify they exist
            result = session.run("CALL db.labels()")
            existing_labels = set(record["label"] for record in result)
            labels = [label for label in self.PWC_NODE_TYPES if label in existing_labels]
        else:
            # Get all labels
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
        
        # Use single batch query to count all labels at once
        if labels:
            # Build a single query that counts all node types in one go
            count_queries = []
            for i, label in enumerate(labels):
                count_queries.append(f"count(n{i}) AS count_{i}")
            
            match_queries = []
            for i, label in enumerate(labels):
                match_queries.append(f"OPTIONAL MATCH (n{i}:{label})")
            
            # Combine into single query
            query = f"""
            {' '.join(match_queries)}
            RETURN {', '.join(count_queries)}
            """
            
            try:
                result = session.run(query)
                record = result.single()
                if record:
                    for i, label in enumerate(labels):
                        counts[label] = record[f'count_{i}'] or 0
                else:
                    # Fallback to individual queries if batch fails
                    for label in labels:
                        counts[label] = 0
            except Exception as e:
                logger.debug(f"Batch count query failed, falling back to individual queries: {e}")
                # Fallback to individual queries
                for label in labels:
                    try:
                        result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                        record = result.single()
                        counts[label] = record['count'] if record else 0
                    except Exception as e:
                        logger.debug(f"Could not count {label} nodes: {e}")
                        counts[label] = 0
        
        # Add any missing PWC node types with 0 count if in PWC-only mode
        if self.pwc_only:
            for label in self.PWC_NODE_TYPES:
                if label not in counts:
                    counts[label] = 0
        
        return counts
    
    def get_relationship_counts(self, session) -> Dict[str, int]:
        """Get count of each relationship type in the database using batch query"""
        counts = {}
        
        if self.pwc_only:
            # Only check PWC-specific relationship types, but first verify they exist
            result = session.run("CALL db.relationshipTypes()")
            existing_rel_types = set(record["relationshipType"] for record in result)
            rel_types = [rt for rt in self.PWC_RELATIONSHIP_TYPES if rt in existing_rel_types]
        else:
            # Get all relationship types
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in result]
        
        # Use single batch query to count all relationship types at once
        if rel_types:
            # Build a single query that counts all relationship types in one go
            count_queries = []
            for i, rel_type in enumerate(rel_types):
                count_queries.append(f"count(r{i}) AS count_{i}")
            
            match_queries = []
            for i, rel_type in enumerate(rel_types):
                match_queries.append(f"OPTIONAL MATCH ()-[r{i}:{rel_type}]-()")
            
            # Combine into single query
            query = f"""
            {' '.join(match_queries)}
            RETURN {', '.join(count_queries)}
            """
            
            try:
                result = session.run(query)
                record = result.single()
                if record:
                    for i, rel_type in enumerate(rel_types):
                        counts[rel_type] = record[f'count_{i}'] or 0
                else:
                    # Fallback to individual queries if batch fails
                    for rel_type in rel_types:
                        counts[rel_type] = 0
            except Exception as e:
                logger.debug(f"Batch relationship count query failed, falling back to individual queries: {e}")
                # Fallback to individual queries
                for rel_type in rel_types:
                    try:
                        result = session.run(f"MATCH ()-[r:{rel_type}]-() RETURN count(r) as count")
                        record = result.single()
                        counts[rel_type] = record['count'] if record else 0
                    except Exception as e:
                        logger.debug(f"Could not count {rel_type} relationships: {e}")
                        counts[rel_type] = 0
        
        # Add any missing PWC relationship types with 0 count if in PWC-only mode
        if self.pwc_only:
            for rel_type in self.PWC_RELATIONSHIP_TYPES:
                if rel_type not in counts:
                    counts[rel_type] = 0
        
        return counts
    
    def get_node_ids_by_label(self, session, label: str, batch_size: int = 10000) -> Set[str]:
        """Get all node IDs for a specific label with efficient batching"""
        ids = set()
        
        # Use stable identifiers for Papers, avoiding hash-based IDs
        if label == 'Paper':
            # For Papers, prefer stable identifiers over generated hash IDs
            id_properties = ['arxiv_id', 'url', 'title']
        else:
            id_properties = ['id', 'arxiv_id', 'url', 'name']
        
        for id_prop in id_properties:
            try:
                # Use LIMIT for large datasets to avoid memory issues
                result = session.run(f"""
                    MATCH (n:{label}) 
                    WHERE n.{id_prop} IS NOT NULL 
                    RETURN n.{id_prop} as id 
                    LIMIT {batch_size}
                """)
                batch_ids = {str(record['id']) for record in result}
                if batch_ids:
                    ids.update(batch_ids)
                    break
            except:
                continue
        
        return ids
    
    def get_detailed_node_data(self, session, label: str, node_id: str) -> Optional[Dict]:
        """Get detailed data for a specific node"""
        # Try common ID properties
        id_properties = ['id', 'arxiv_id', 'url', 'name']
        
        for id_prop in id_properties:
            try:
                result = session.run(f"MATCH (n:{label}) WHERE n.{id_prop} = $id RETURN n", id=node_id)
                record = result.single()
                if record:
                    return dict(record['n'])
            except:
                continue
        
        return None
    
    def compare_node_counts(self) -> Dict[str, Any]:
        """Compare node counts between instances"""
        logger.info("📊 Comparing node counts...")
        
        source_counts = self.get_node_counts(self.source_session)
        target_counts = self.get_node_counts(self.target_session)
        
        all_labels = set(source_counts.keys()) | set(target_counts.keys())
        
        comparison = {
            'source_counts': source_counts,
            'target_counts': target_counts,
            'differences': {},
            'missing_in_target': [],
            'missing_in_source': [],
            'identical_counts': []
        }
        
        for label in all_labels:
            source_count = source_counts.get(label, 0)
            target_count = target_counts.get(label, 0)
            
            if source_count == target_count:
                comparison['identical_counts'].append(label)
            else:
                comparison['differences'][label] = {
                    'source': source_count,
                    'target': target_count,
                    'difference': target_count - source_count
                }
            
            if source_count > 0 and target_count == 0:
                comparison['missing_in_target'].append(label)
            elif source_count == 0 and target_count > 0:
                comparison['missing_in_source'].append(label)
        
        return comparison
    
    def compare_relationship_counts(self) -> Dict[str, Any]:
        """Compare relationship counts between instances"""
        logger.info("🔗 Comparing relationship counts...")
        
        source_counts = self.get_relationship_counts(self.source_session)
        target_counts = self.get_relationship_counts(self.target_session)
        
        all_rel_types = set(source_counts.keys()) | set(target_counts.keys())
        
        comparison = {
            'source_counts': source_counts,
            'target_counts': target_counts,
            'differences': {},
            'missing_in_target': [],
            'missing_in_source': [],
            'identical_counts': []
        }
        
        for rel_type in all_rel_types:
            source_count = source_counts.get(rel_type, 0)
            target_count = target_counts.get(rel_type, 0)
            
            if source_count == target_count:
                comparison['identical_counts'].append(rel_type)
            else:
                comparison['differences'][rel_type] = {
                    'source': source_count,
                    'target': target_count,
                    'difference': target_count - source_count
                }
            
            if source_count > 0 and target_count == 0:
                comparison['missing_in_target'].append(rel_type)
            elif source_count == 0 and target_count > 0:
                comparison['missing_in_source'].append(rel_type)
        
        return comparison
    
    def sample_node_comparison(self, label: str, sample_size: int = 10, batch_size: int = 50, progress_bar=None, full_diff: bool = False) -> Dict[str, Any]:
        """Compare nodes between instances with batch processing"""
        if full_diff:
            logger.info(f"🔍 Comparing ALL {label} nodes (full diff mode)...")
        else:
            logger.info(f"🔍 Sampling {sample_size} {label} nodes for detailed comparison...")
        
        # Get node IDs from source database only
        if full_diff:
            # Get ALL node IDs for full comparison
            source_ids = self.get_node_ids_by_label(self.source_session, label, batch_size=100000)  # Large batch for full diff
            
            # Use all source IDs for comparison
            compare_ids = list(source_ids)
            
            # For full diff, also get target IDs to find nodes only in target
            target_ids = self.get_node_ids_by_label(self.target_session, label, batch_size=100000)
            target_only_ids = target_ids - source_ids
            
        else:
            # Get node IDs from SOURCE ONLY for sampling
            source_ids = self.get_node_ids_by_label(self.source_session, label, batch_size=max(sample_size * 2, 1000))
            
            # Sample IDs from source for comparison
            compare_ids = list(source_ids)[:sample_size]
            target_only_ids = set()  # Don't check target-only for sampling
        
        comparison = {
            'label': label,
            'sample_size': len(compare_ids) if not full_diff else None,
            'total_compared': len(compare_ids),
            'full_diff': full_diff,
            'identical_nodes': 0,
            'different_nodes': 0,
            'missing_in_target': 0,
            'missing_in_source': len(target_only_ids) if full_diff else 0,
            'differences': []
        }
        
        # Process nodes in batches for better performance
        # Use smaller batches for node types that are slow to fetch
        if label in ['Repository', 'Author']:
            effective_batch_size = min(batch_size, 10)  # Much smaller batches for slow types
        else:
            effective_batch_size = batch_size
        
        num_batches = (len(compare_ids) + effective_batch_size - 1) // effective_batch_size
        
        # Create detailed progress bar for all node comparisons with reasonable thresholds
        detailed_progress = None
        start_time = time.time()
        show_detailed = len(compare_ids) > 50  # Show progress bar for any comparison with more than 50 nodes
        
        if show_detailed:
            mode_text = "Full diff" if full_diff else "Sampling"
            detailed_progress = tqdm(
                total=len(compare_ids),
                desc=f"{mode_text} {label}",
                unit="nodes",
                leave=True,  # Keep the bar visible after completion
                miniters=max(1, len(compare_ids) // 50),  # Update frequently for visual feedback
                mininterval=0.5,  # Update twice per second for responsiveness
                disable=False,
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
        
        for i in range(0, len(compare_ids), effective_batch_size):
            batch_ids = compare_ids[i:i + effective_batch_size]
            
            # Update progress during data fetching phases
            if detailed_progress:
                detailed_progress.set_description(f"{mode_text} {label} - Fetching data...")
            
            # Get batch data from both sources
            source_batch_data = self.get_batch_node_data(self.source_session, label, batch_ids)
            target_batch_data = self.get_batch_node_data(self.target_session, label, batch_ids)
            
            if detailed_progress:
                detailed_progress.set_description(f"{mode_text} {label} - Comparing...")
            
            # Compare nodes in this batch
            for idx, node_id in enumerate(batch_ids):
                source_data = source_batch_data.get(node_id)
                target_data = target_batch_data.get(node_id)
                
                if not target_data:
                    comparison['missing_in_target'] += 1
                    comparison['differences'].append({
                        'id': node_id,
                        'status': 'missing_in_target',
                        'source_data': source_data
                    })
                    # Debug logging for missing nodes
                    if len(comparison['differences']) <= 3:  # Only log first few
                        logger.debug(f"Missing in target - {label} node ID: {node_id}")
                elif self.nodes_are_equivalent(source_data, target_data):
                    comparison['identical_nodes'] += 1
                else:
                    comparison['different_nodes'] += 1
                    comparison['differences'].append({
                        'id': node_id,
                        'status': 'different',
                        'source_data': source_data,
                        'target_data': target_data
                    })
                    # Debug logging for different nodes - only log first few differences
                    if len([d for d in comparison['differences'] if d['status'] == 'different']) <= 3:
                        logger.debug(f"Different {label} node ID: {node_id}")
                        logger.debug(f"Source: {source_data}")
                        logger.debug(f"Target: {target_data}")
                        # Find specific differences
                        if source_data and target_data:
                            source_keys = set(source_data.keys()) if source_data else set()
                            target_keys = set(target_data.keys()) if target_data else set()
                            if source_keys != target_keys:
                                logger.debug(f"Key differences - Source keys: {source_keys}, Target keys: {target_keys}")
                            else:
                                for key in source_keys:
                                    if source_data.get(key) != target_data.get(key):
                                        logger.debug(f"Value difference in '{key}': '{source_data.get(key)}' vs '{target_data.get(key)}'")
                                        break
                
                # Update progress for each individual node processed 
                if detailed_progress:
                    detailed_progress.update(1)
                    # Update postfix with current results every few nodes for performance
                    if (idx + 1) % max(1, len(batch_ids) // 4) == 0 or idx == len(batch_ids) - 1:
                        elapsed = time.time() - start_time
                        rate = (i + idx + 1) / elapsed if elapsed > 0 else 0
                        detailed_progress.set_postfix({
                            'identical': comparison['identical_nodes'],
                            'different': comparison['different_nodes'],
                            'missing': comparison['missing_in_target'],
                            'rate': f"{rate:.0f}/s" if rate > 0 else "0/s"
                        })
            
            # Note: Progress updates are now handled per individual node above
            
            # Update overall progress if progress bar provided (but don't update for every batch if we have detailed progress)
            if progress_bar and not detailed_progress:
                batch_num = (i // batch_size) + 1
                if full_diff:
                    progress_bar.set_description(f"Full diff {label} nodes ({comparison['identical_nodes']} ✅, {comparison['different_nodes']} ❌, {comparison['missing_in_target']} 🚫)")
                else:
                    progress_bar.set_description(f"Comparing {label} nodes (batch {batch_num}/{num_batches})")
                
            # Always update main progress bar once we finish this node type
            if progress_bar and i + batch_size >= len(compare_ids):  # Last batch
                if full_diff:
                    progress_bar.set_description(f"Completed {label} full diff ({comparison['identical_nodes']} ✅, {comparison['different_nodes']} ❌, {comparison['missing_in_target']} 🚫)")
                else:
                    progress_bar.set_description(f"Completed {label} sampling")
                progress_bar.update(1)
        
        # Close detailed progress bar and show final results
        if detailed_progress:
            detailed_progress.set_postfix({
                'result': f"✅{comparison['identical_nodes']} ❌{comparison['different_nodes']} 🚫{comparison['missing_in_target']}"
            })
            detailed_progress.close()
        
        # Add target-only nodes to differences if doing full diff
        if full_diff and target_only_ids:
            for node_id in list(target_only_ids)[:100]:  # Limit to first 100 for reporting
                comparison['differences'].append({
                    'id': node_id,
                    'status': 'missing_in_source',
                    'target_data': self.get_detailed_node_data(self.target_session, label, node_id)
                })
        
        return comparison
    
    def get_batch_node_data(self, session, label: str, node_ids: List[str]) -> Dict[str, Optional[Dict]]:
        """Get detailed data for multiple nodes in a single batch query"""
        if not node_ids:
            return {}
        
        batch_data = {}
        
        # Try common ID properties - for Papers, try title first for hash-based IDs
        if label == 'Paper':
            id_properties = ['arxiv_id', 'url', 'title', 'id']
        else:
            id_properties = ['id', 'arxiv_id', 'url', 'name']
        
        for id_prop in id_properties:
            try:
                # Use UNWIND for efficient batch querying
                query = f"""
                UNWIND $node_ids AS node_id
                MATCH (n:{label}) 
                WHERE n.{id_prop} = node_id
                RETURN n.{id_prop} as id, n
                """
                
                result = session.run(query, node_ids=node_ids)
                for record in result:
                    batch_data[str(record['id'])] = dict(record['n'])
                
                # If we got results, we found the right ID property
                if batch_data:
                    break
                    
            except Exception as e:
                logger.debug(f"Batch query failed for {id_prop}: {e}")
                continue
        
        # Fill in None for missing nodes
        for node_id in node_ids:
            if node_id not in batch_data:
                batch_data[node_id] = None
        
        return batch_data
    
    def nodes_are_equivalent(self, source_data: Optional[Dict], target_data: Optional[Dict]) -> bool:
        """Compare two nodes ignoring timestamp and auto-generated fields"""
        if not source_data and not target_data:
            return True
        if not source_data or not target_data:
            return False
        
        # Fields to ignore in comparison (timestamps and auto-generated fields)
        ignore_fields = {'updated_at', 'created_at', 'last_modified', 'timestamp'}
        
        # Create copies without ignored fields
        source_filtered = {k: v for k, v in source_data.items() if k not in ignore_fields}
        target_filtered = {k: v for k, v in target_data.items() if k not in ignore_fields}
        
        return source_filtered == target_filtered
    
    def full_comparison(self, sample_size: int = 10, batch_size: int = 100, full_diff: bool = False) -> Dict[str, Any]:
        """Perform a full comparison between the two Neo4j instances with batch optimization"""
        logger.info("🔍 Starting full Neo4j instance comparison...")
        start_time = datetime.now()
        
        # Count operations: 2 (node counts + relationship counts) + sample comparisons
        all_labels_preview = set()
        try:
            # Quick preview to estimate total work
            if self.pwc_only:
                all_labels_preview = set(self.PWC_NODE_TYPES)
            else:
                source_labels = self.source_session.run("CALL db.labels()")
                target_labels = self.target_session.run("CALL db.labels()")
                all_labels_preview = set([r["label"] for r in source_labels]) | set([r["label"] for r in target_labels])
        except:
            all_labels_preview = set()
        
        # Calculate total steps: node counts + relationship counts + sample comparisons
        total_steps = 2 + len(all_labels_preview)
        
        # Create overall progress bar
        progress_bar = tqdm(
            total=total_steps,
            desc="Neo4j Comparison Progress",
            unit="step",
            leave=True
        )
        
        try:
            # Step 1: Compare node counts
            progress_bar.set_description("Comparing node counts")
            node_comparison = self.compare_node_counts()
            progress_bar.update(1)
            
            # Step 2: Compare relationship counts
            progress_bar.set_description("Comparing relationship counts")
            relationship_comparison = self.compare_relationship_counts()
            progress_bar.update(1)
            
            comparison_result = {
                'timestamp': start_time.isoformat(),
                'source_uri': self.source_uri,
                'target_uri': self.target_uri,
                'node_comparison': node_comparison,
                'relationship_comparison': relationship_comparison,
                'sample_comparisons': {},
                'summary': {},
                'batch_size_used': batch_size
            }
            
            # Sample detailed comparisons for each node type with batch processing
            all_labels = set(comparison_result['node_comparison']['source_counts'].keys()) | \
                        set(comparison_result['node_comparison']['target_counts'].keys())
            
            for label in all_labels:
                if comparison_result['node_comparison']['source_counts'].get(label, 0) > 0:
                    if full_diff:
                        progress_bar.set_description(f"Full diff {label} nodes")
                    else:
                        progress_bar.set_description(f"Sampling {label} nodes")
                    
                    # Temporarily disable the main progress bar updates during node comparison
                    comparison_result['sample_comparisons'][label] = self.sample_node_comparison(
                        label, sample_size, batch_size, None, full_diff  # Pass None for progress_bar to avoid conflicts
                    )
                    
                    # Update main progress after completion
                    if full_diff:
                        sample_data = comparison_result['sample_comparisons'][label]
                        progress_bar.set_description(f"Completed {label} full diff ({sample_data['identical_nodes']} ✅, {sample_data['different_nodes']} ❌, {sample_data['missing_in_target']} 🚫)")
                    else:
                        progress_bar.set_description(f"Completed {label} sampling")
                    progress_bar.update(1)
                else:
                    # Still update progress for labels with no data
                    progress_bar.set_description(f"Skipping {label} (no data)")
                    progress_bar.update(1)
            
            # Step 3: Generate summary
            progress_bar.set_description("Generating summary")
            node_comp = comparison_result['node_comparison']
            rel_comp = comparison_result['relationship_comparison']
            sample_comps = comparison_result['sample_comparisons']
            
            total_source_nodes = sum(node_comp['source_counts'].values())
            total_target_nodes = sum(node_comp['target_counts'].values())
            total_source_rels = sum(rel_comp['source_counts'].values())
            total_target_rels = sum(rel_comp['target_counts'].values())
            
            # Check if sample comparisons show differences
            sample_differences_found = False
            for label, sample_data in sample_comps.items():
                if (sample_data['different_nodes'] > 0 or 
                    sample_data['missing_in_target'] > 0 or
                    len(sample_data['differences']) > 0):
                    sample_differences_found = True
                    break
            
            comparison_result['summary'] = {
                'total_nodes': {
                    'source': total_source_nodes,
                    'target': total_target_nodes,
                    'match': total_source_nodes == total_target_nodes
                },
                'total_relationships': {
                    'source': total_source_rels,
                    'target': total_target_rels,
                    'match': total_source_rels == total_target_rels
                },
                'node_types_match': len(node_comp['differences']) == 0,
                'relationship_types_match': len(rel_comp['differences']) == 0,
                'sample_differences_found': sample_differences_found,
                'identical': (total_source_nodes == total_target_nodes and 
                            total_source_rels == total_target_rels and
                            len(node_comp['differences']) == 0 and
                            len(rel_comp['differences']) == 0 and
                            not sample_differences_found)
            }
            
            end_time = datetime.now()
            comparison_result['duration'] = str(end_time - start_time)
            
            progress_bar.set_description("Comparison completed")
            progress_bar.close()
            
            logger.info(f"✅ Comparison completed in {comparison_result['duration']}")
            return comparison_result
            
        except Exception as e:
            progress_bar.close()
            raise e
    
    def print_comparison_report(self, comparison: Dict[str, Any]):
        """Print a human-readable comparison report"""
        print("\n" + "="*80)
        print("📊 NEO4J INSTANCE COMPARISON REPORT")
        print("="*80)
        print(f"🕒 Timestamp: {comparison['timestamp']}")
        print(f"📍 Source: {comparison['source_uri']}")
        print(f"📍 Target: {comparison['target_uri']}")
        print(f"⏱️  Duration: {comparison['duration']}")
        if self.pwc_only:
            print("🎯 Mode: Papers with Code data types only")
        else:
            print("🌐 Mode: All data types")
        
        # Summary
        print("\n📋 SUMMARY")
        print("-" * 40)
        summary = comparison['summary']
        
        total_match = "✅ IDENTICAL" if summary['identical'] else "❌ DIFFERENT"
        print(f"Overall: {total_match}")
        
        node_match = "✅" if summary['total_nodes']['match'] else "❌"
        print(f"Total Nodes: {node_match} Source: {summary['total_nodes']['source']}, Target: {summary['total_nodes']['target']}")
        
        rel_match = "✅" if summary['total_relationships']['match'] else "❌"
        print(f"Total Relationships: {rel_match} Source: {summary['total_relationships']['source']}, Target: {summary['total_relationships']['target']}")
        
        # Node type comparison
        print("\n📦 NODE TYPE COMPARISON")
        print("-" * 40)
        node_comp = comparison['node_comparison']
        
        if node_comp['identical_counts']:
            print(f"✅ Identical counts: {', '.join(node_comp['identical_counts'])}")
        
        if node_comp['differences']:
            print("❌ Different counts:")
            for label, diff in node_comp['differences'].items():
                print(f"   {label}: Source={diff['source']}, Target={diff['target']} (diff: {diff['difference']:+d})")
        
        if node_comp['missing_in_target']:
            print(f"⚠️  Missing in target: {', '.join(node_comp['missing_in_target'])}")
        
        if node_comp['missing_in_source']:
            print(f"⚠️  Missing in source: {', '.join(node_comp['missing_in_source'])}")
        
        # Relationship type comparison
        print("\n🔗 RELATIONSHIP TYPE COMPARISON")
        print("-" * 40)
        rel_comp = comparison['relationship_comparison']
        
        if rel_comp['identical_counts']:
            print(f"✅ Identical counts: {', '.join(rel_comp['identical_counts'])}")
        
        if rel_comp['differences']:
            print("❌ Different counts:")
            for rel_type, diff in rel_comp['differences'].items():
                print(f"   {rel_type}: Source={diff['source']}, Target={diff['target']} (diff: {diff['difference']:+d})")
        
        if rel_comp['missing_in_target']:
            print(f"⚠️  Missing in target: {', '.join(rel_comp['missing_in_target'])}")
        
        if rel_comp['missing_in_source']:
            print(f"⚠️  Missing in source: {', '.join(rel_comp['missing_in_source'])}")
        
        # Sample/Full comparisons
        if any(comparison['sample_comparisons'].values()):
            comparison_type = "FULL NODE COMPARISONS" if any(s.get('full_diff', False) for s in comparison['sample_comparisons'].values()) else "SAMPLE NODE COMPARISONS"
            print(f"\n🔍 {comparison_type}")
            print("-" * 40)
            for label, sample in comparison['sample_comparisons'].items():
                status = "✅" if (sample['missing_in_target'] == 0 and 
                                sample['different_nodes'] == 0 and 
                                sample.get('missing_in_source', 0) == 0) else "❌"
                
                if sample.get('full_diff', False):
                    print(f"{status} {label} (full diff {sample['total_compared']} nodes): "
                          f"{sample['identical_nodes']} identical, "
                          f"{sample['different_nodes']} different, "
                          f"{sample['missing_in_target']} missing in target, "
                          f"{sample.get('missing_in_source', 0)} missing in source")
                else:
                    print(f"{status} {label} (sample {sample['sample_size']}): "
                          f"{sample['identical_nodes']} identical, "
                          f"{sample['different_nodes']} different, "
                          f"{sample['missing_in_target']} missing")
        
        print("\n" + "="*80)

def main():
    """Main function to run Neo4j diff comparison"""
    import argparse
    from config_parser import Neo4jConfig
    
    parser = argparse.ArgumentParser(
        description="Compare data between two Neo4j instances"
    )
    parser.add_argument(
        "--source-uri", 
        default="bolt://localhost:7687",
        help="Source Neo4j URI (default: bolt://localhost:7687)"
    )
    parser.add_argument(
        "--source-user", 
        default="neo4j",
        help="Source Neo4j username (default: neo4j)"
    )
    parser.add_argument(
        "--source-password", 
        default="password",
        help="Source Neo4j password (default: password)"
    )
    parser.add_argument(
        "--target-uri", 
        help="Target Neo4j URI to compare against (required unless using --remote with config)"
    )
    parser.add_argument(
        "--target-user", 
        default="neo4j",
        help="Target Neo4j username (default: neo4j)"
    )
    parser.add_argument(
        "--target-password", 
        default="password",
        help="Target Neo4j password (default: password)"
    )
    parser.add_argument(
        "--sample", 
        type=int, 
        default=10,
        help="Number of nodes to sample for detailed comparison (default: 10)"
    )
    parser.add_argument(
        "--full-diff",
        action="store_true",
        help="Compare ALL nodes, not just samples (may be slow for large datasets)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=100,
        help="Batch size for node queries and comparisons (default: 100)"
    )
    parser.add_argument(
        "--output", 
        help="Output file path for JSON comparison result"
    )
    parser.add_argument(
        "--pwc-only",
        action="store_true",
        help="Only compare Papers with Code specific data types (useful for mixed databases)"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local environment from config file as source"
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Use remote environment from config file as target"
    )
    
    args = parser.parse_args()
    
    # Load configuration if using config flags
    config = None
    local_config = None
    remote_config = None
    
    if args.local or args.remote:
        config = Neo4jConfig(args.config)
        
        if args.local:
            local_config = config.get_local_config()
            if not local_config:
                logger.error("❌ Failed to load local configuration")
                logger.error(f"   Check your config file: {args.config}")
                return
        
        if args.remote:
            remote_config = config.get_remote_config()
            if not remote_config:
                logger.error("❌ Failed to load remote configuration")
                logger.error(f"   Check your config file: {args.config}")
                return
    
    # Determine source and target URIs and credentials
    if local_config:
        args.source_uri = local_config['uri']
        args.source_user = local_config['user']
        args.source_password = local_config['password']
        logger.info("🔧 Using local config for source connection")
    
    if remote_config:
        args.target_uri = remote_config['uri']
        args.target_user = remote_config['user']
        args.target_password = remote_config['password']
        logger.info("🔧 Using remote config for target connection")
    
    # Validate that we have both source and target after config processing
    if not args.target_uri:
        logger.error("❌ Target URI is required")
        logger.error("   Use --target-uri or --remote with config file")
        return
    
    try:
        # Initialize diff tool
        diff_tool = Neo4jDiff(
            source_uri=args.source_uri,
            source_user=args.source_user,
            source_password=args.source_password,
            target_uri=args.target_uri,
            target_user=args.target_user,
            target_password=args.target_password,
            pwc_only=args.pwc_only
        )
        
        # Perform comparison with batch optimization
        comparison_result = diff_tool.full_comparison(
            sample_size=args.sample,
            batch_size=args.batch_size,
            full_diff=args.full_diff
        )
        
        # Print report
        diff_tool.print_comparison_report(comparison_result)
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(comparison_result, f, indent=2, default=str)
            print(f"\n💾 Detailed comparison saved to: {args.output}")
        
        # Clean up
        diff_tool.close()
        
        # Exit with appropriate code
        if comparison_result['summary']['identical']:
            print("\n✅ Instances are identical!")
            exit(0)
        else:
            print("\n❌ Instances have differences!")
            exit(1)
        
    except Exception as e:
        logger.error(f"❌ Comparison failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()