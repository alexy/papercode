#!/usr/bin/env python3

import logging
from typing import Dict, List, Set, Any, Optional, Tuple
from neo4j import GraphDatabase
import json
from datetime import datetime
from collections import defaultdict

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
        """Get count of each node type in the database"""
        counts = {}
        
        if self.pwc_only:
            # Only check PWC-specific node types
            labels = self.PWC_NODE_TYPES
        else:
            # Get all labels
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
        
        # Count nodes for each label
        for label in labels:
            try:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                record = result.single()
                counts[label] = record['count'] if record else 0
            except Exception as e:
                logger.debug(f"Could not count {label} nodes: {e}")
                counts[label] = 0
        
        return counts
    
    def get_relationship_counts(self, session) -> Dict[str, int]:
        """Get count of each relationship type in the database"""
        counts = {}
        
        if self.pwc_only:
            # Only check PWC-specific relationship types
            rel_types = self.PWC_RELATIONSHIP_TYPES
        else:
            # Get all relationship types
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in result]
        
        # Count relationships for each type
        for rel_type in rel_types:
            try:
                result = session.run(f"MATCH ()-[r:{rel_type}]-() RETURN count(r) as count")
                record = result.single()
                counts[rel_type] = record['count'] if record else 0
            except Exception as e:
                logger.debug(f"Could not count {rel_type} relationships: {e}")
                counts[rel_type] = 0
        
        return counts
    
    def get_node_ids_by_label(self, session, label: str) -> Set[str]:
        """Get all node IDs for a specific label"""
        ids = set()
        
        # Try common ID properties
        id_properties = ['id', 'arxiv_id', 'url', 'name']
        
        for id_prop in id_properties:
            try:
                result = session.run(f"MATCH (n:{label}) WHERE n.{id_prop} IS NOT NULL RETURN n.{id_prop} as id")
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
    
    def sample_node_comparison(self, label: str, sample_size: int = 10) -> Dict[str, Any]:
        """Compare a sample of nodes between instances"""
        logger.info(f"🔍 Sampling {sample_size} {label} nodes for detailed comparison...")
        
        # Get node IDs from source
        source_ids = self.get_node_ids_by_label(self.source_session, label)
        target_ids = self.get_node_ids_by_label(self.target_session, label)
        
        # Sample IDs for comparison
        sample_ids = list(source_ids)[:sample_size]
        
        comparison = {
            'label': label,
            'sample_size': len(sample_ids),
            'identical_nodes': 0,
            'different_nodes': 0,
            'missing_in_target': 0,
            'differences': []
        }
        
        for node_id in sample_ids:
            source_data = self.get_detailed_node_data(self.source_session, label, node_id)
            target_data = self.get_detailed_node_data(self.target_session, label, node_id)
            
            if not target_data:
                comparison['missing_in_target'] += 1
                comparison['differences'].append({
                    'id': node_id,
                    'status': 'missing_in_target',
                    'source_data': source_data
                })
            elif source_data == target_data:
                comparison['identical_nodes'] += 1
            else:
                comparison['different_nodes'] += 1
                comparison['differences'].append({
                    'id': node_id,
                    'status': 'different',
                    'source_data': source_data,
                    'target_data': target_data
                })
        
        return comparison
    
    def full_comparison(self, sample_size: int = 10) -> Dict[str, Any]:
        """Perform a full comparison between the two Neo4j instances"""
        logger.info("🔍 Starting full Neo4j instance comparison...")
        start_time = datetime.now()
        
        comparison_result = {
            'timestamp': start_time.isoformat(),
            'source_uri': self.source_uri,
            'target_uri': self.target_uri,
            'node_comparison': self.compare_node_counts(),
            'relationship_comparison': self.compare_relationship_counts(),
            'sample_comparisons': {},
            'summary': {}
        }
        
        # Sample detailed comparisons for each node type
        all_labels = set(comparison_result['node_comparison']['source_counts'].keys()) | \
                    set(comparison_result['node_comparison']['target_counts'].keys())
        
        for label in all_labels:
            if comparison_result['node_comparison']['source_counts'].get(label, 0) > 0:
                comparison_result['sample_comparisons'][label] = self.sample_node_comparison(label, sample_size)
        
        # Generate summary
        node_comp = comparison_result['node_comparison']
        rel_comp = comparison_result['relationship_comparison']
        
        total_source_nodes = sum(node_comp['source_counts'].values())
        total_target_nodes = sum(node_comp['target_counts'].values())
        total_source_rels = sum(rel_comp['source_counts'].values())
        total_target_rels = sum(rel_comp['target_counts'].values())
        
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
            'identical': total_source_nodes == total_target_nodes and 
                        total_source_rels == total_target_rels and
                        len(node_comp['differences']) == 0 and
                        len(rel_comp['differences']) == 0
        }
        
        end_time = datetime.now()
        comparison_result['duration'] = str(end_time - start_time)
        
        logger.info(f"✅ Comparison completed in {comparison_result['duration']}")
        return comparison_result
    
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
        
        # Sample comparisons
        print("\n🔍 SAMPLE NODE COMPARISONS")
        print("-" * 40)
        for label, sample in comparison['sample_comparisons'].items():
            status = "✅" if sample['missing_in_target'] == 0 and sample['different_nodes'] == 0 else "❌"
            print(f"{status} {label} (sample {sample['sample_size']}): {sample['identical_nodes']} identical, "
                  f"{sample['different_nodes']} different, {sample['missing_in_target']} missing")
        
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
        required=True,
        help="Target Neo4j URI to compare against"
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
        "--sample-size", 
        type=int, 
        default=10,
        help="Number of nodes to sample for detailed comparison (default: 10)"
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
    
    # Validate that we have both source and target
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
        
        # Perform comparison
        comparison_result = diff_tool.full_comparison(sample_size=args.sample_size)
        
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