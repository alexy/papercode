#!/usr/bin/env python3

import logging
import sys
from models import PapersWithCodeGraph
from neo4j_diff import Neo4jDiff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data(graph: PapersWithCodeGraph):
    """Create some test data in the database"""
    logger.info("🧪 Creating test data...")
    
    try:
        session = graph.get_session()
        
        # Create some test nodes and relationships
        test_queries = [
            # Create a test paper
            """
            CREATE (p:Paper {
                id: 'test-paper-1',
                title: 'Test Paper for Selective Clearing',
                abstract: 'This is a test paper'
            })
            """,
            
            # Create a test repository
            """
            CREATE (r:Repository {
                url: 'https://github.com/test/selective-clearing',
                owner: 'test',
                name: 'selective-clearing',
                description: 'Test repository for selective clearing'
            })
            """,
            
            # Create a test dataset
            """
            CREATE (d:Dataset {
                id: 'test-dataset-1',
                name: 'Test Dataset',
                description: 'Test dataset for selective clearing'
            })
            """,
            
            # Create a test author
            """
            CREATE (a:Author {
                name: 'Test Author',
                email: 'test@example.com'
            })
            """,
            
            # Create some non-PWC data to ensure it's preserved
            """
            CREATE (u:User {
                id: 'test-user-1',
                name: 'Test User',
                role: 'administrator'
            })
            """,
            
            """
            CREATE (c:Company {
                id: 'test-company-1',
                name: 'Test Company',
                industry: 'Technology'
            })
            """,
            
            # Create relationships
            """
            MATCH (p:Paper {id: 'test-paper-1'}), (a:Author {name: 'Test Author'})
            CREATE (a)-[:AUTHORED]->(p)
            """,
            
            """
            MATCH (p:Paper {id: 'test-paper-1'}), (r:Repository {url: 'https://github.com/test/selective-clearing'})
            CREATE (p)-[:HAS_CODE]->(r)
            """,
            
            """
            MATCH (p:Paper {id: 'test-paper-1'}), (d:Dataset {id: 'test-dataset-1'})
            CREATE (p)-[:USES_DATASET]->(d)
            """,
            
            # Create non-PWC relationship
            """
            MATCH (u:User {id: 'test-user-1'}), (c:Company {id: 'test-company-1'})
            CREATE (u)-[:WORKS_FOR]->(c)
            """
        ]
        
        for query in test_queries:
            session.run(query)
        
        # Verify test data was created
        stats = graph.get_graph_stats()
        logger.info(f"✅ Test data created. Current stats: {stats}")
        
        # Check non-PWC data
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        user_count = result.single()['count']
        
        result = session.run("MATCH (c:Company) RETURN count(c) as count")
        company_count = result.single()['count']
        
        result = session.run("MATCH ()-[r:WORKS_FOR]-() RETURN count(r) as count")
        works_for_count = result.single()['count']
        
        logger.info(f"Non-PWC data created: {user_count} Users, {company_count} Companies, {works_for_count} WORKS_FOR relationships")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create test data: {e}")
        return False

def test_selective_clearing():
    """Test the selective clearing functionality"""
    logger.info("🧪 Testing selective clearing functionality...")
    
    try:
        # Connect to Neo4j
        graph = PapersWithCodeGraph(
            neo4j_uri="bolt://localhost:7687",
            username="neo4j", 
            password="password"
        )
        
        # Create test data
        if not create_test_data(graph):
            return False
        
        session = graph.get_session()
        
        # Get counts before clearing
        logger.info("📊 Checking data before selective clearing...")
        before_stats = graph.get_graph_stats()
        
        # Check non-PWC data before
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        users_before = result.single()['count']
        
        result = session.run("MATCH (c:Company) RETURN count(c) as count") 
        companies_before = result.single()['count']
        
        result = session.run("MATCH ()-[r:WORKS_FOR]-() RETURN count(r) as count")
        works_for_before = result.single()['count']
        
        logger.info(f"Before clearing - PWC data: {before_stats}")
        logger.info(f"Before clearing - Non-PWC data: {users_before} Users, {companies_before} Companies, {works_for_before} WORKS_FOR")
        
        # Perform selective clearing
        logger.info("🗑️ Performing selective clearing...")
        cleared_data = graph.clear_pwc_data_only()
        graph.clear_pwc_indexes_only()
        
        # Get counts after clearing
        logger.info("📊 Checking data after selective clearing...")
        after_stats = graph.get_graph_stats()
        
        # Check non-PWC data after
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        users_after = result.single()['count']
        
        result = session.run("MATCH (c:Company) RETURN count(c) as count")
        companies_after = result.single()['count']
        
        result = session.run("MATCH ()-[r:WORKS_FOR]-() RETURN count(r) as count")
        works_for_after = result.single()['count']
        
        logger.info(f"After clearing - PWC data: {after_stats}")
        logger.info(f"After clearing - Non-PWC data: {users_after} Users, {companies_after} Companies, {works_for_after} WORKS_FOR")
        
        # Validate results
        success = True
        
        # PWC data should be cleared
        if after_stats['papers'] != 0 or after_stats['repositories'] != 0 or after_stats['datasets'] != 0 or after_stats['authors'] != 0:
            logger.error("❌ PWC data was not completely cleared!")
            success = False
        else:
            logger.info("✅ PWC data successfully cleared")
        
        # Non-PWC data should be preserved
        if users_after != users_before or companies_after != companies_before or works_for_after != works_for_before:
            logger.error("❌ Non-PWC data was not preserved!")
            logger.error(f"Users: {users_before} -> {users_after}")
            logger.error(f"Companies: {companies_before} -> {companies_after}")
            logger.error(f"WORKS_FOR: {works_for_before} -> {works_for_after}")
            success = False
        else:
            logger.info("✅ Non-PWC data successfully preserved")
        
        # Clean up test data
        logger.info("🧹 Cleaning up remaining test data...")
        cleanup_queries = [
            "MATCH (u:User) DETACH DELETE u",
            "MATCH (c:Company) DETACH DELETE c"
        ]
        
        for query in cleanup_queries:
            session.run(query)
        
        graph.close()
        
        if success:
            logger.info("✅ Selective clearing test passed!")
        else:
            logger.error("❌ Selective clearing test failed!")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Selective clearing test failed: {e}")
        logger.info("Note: This test requires a running Neo4j instance with correct credentials")
        return False

def test_pwc_only_diff():
    """Test the PWC-only diff functionality"""
    logger.info("🧪 Testing PWC-only diff functionality...")
    
    try:
        # Test with PWC-only mode (should be empty after clearing)
        diff_tool = Neo4jDiff(
            source_uri="bolt://localhost:7687",
            source_user="neo4j",
            source_password="password",
            target_uri="bolt://localhost:7687",  # Same instance
            target_user="neo4j",
            target_password="password",
            pwc_only=True
        )
        
        # Perform comparison
        comparison_result = diff_tool.full_comparison(sample_size=5)
        
        # Check that we only compared PWC types
        node_types = set(comparison_result['node_comparison']['source_counts'].keys())
        rel_types = set(comparison_result['relationship_comparison']['source_counts'].keys())
        
        logger.info(f"PWC-only mode checked node types: {node_types}")
        logger.info(f"PWC-only mode checked relationship types: {rel_types}")
        
        # Validate that only PWC types were checked
        expected_node_types = set(diff_tool.PWC_NODE_TYPES)
        expected_rel_types = set(diff_tool.PWC_RELATIONSHIP_TYPES)
        
        if not node_types.issubset(expected_node_types):
            logger.error(f"❌ Unexpected node types found: {node_types - expected_node_types}")
            return False
        
        if not rel_types.issubset(expected_rel_types):
            logger.error(f"❌ Unexpected relationship types found: {rel_types - expected_rel_types}")
            return False
        
        diff_tool.close()
        logger.info("✅ PWC-only diff test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ PWC-only diff test failed: {e}")
        return False

def test_clear_all_flag():
    """Test that --clear-all flag uses selective clearing"""
    logger.info("🧪 Testing --clear-all flag selective behavior...")
    
    try:
        # This is a simulation test since we can't easily test the full pipeline
        # We'll test the method directly
        from models import PapersWithCodeGraph
        
        graph = PapersWithCodeGraph(
            neo4j_uri="bolt://localhost:7687",
            username="neo4j", 
            password="password"
        )
        
        # Create test data again
        if not create_test_data(graph):
            return False
        
        session = graph.get_session()
        
        # Get counts before clearing
        before_stats = graph.get_graph_stats()
        
        # Check non-PWC data before
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        users_before = result.single()['count']
        
        # Test the method that --clear-all now uses
        cleared_data = graph.clear_pwc_data_only()
        graph.clear_pwc_indexes_only()
        
        # Get counts after clearing
        after_stats = graph.get_graph_stats()
        
        # Check non-PWC data after
        result = session.run("MATCH (u:User) RETURN count(u) as count")
        users_after = result.single()['count']
        
        # Validate that PWC data was cleared but non-PWC preserved
        pwc_cleared = (after_stats['papers'] == 0 and after_stats['repositories'] == 0 and 
                      after_stats['datasets'] == 0 and after_stats['authors'] == 0)
        non_pwc_preserved = (users_after == users_before)
        
        # Clean up
        session.run("MATCH (u:User) DETACH DELETE u")
        session.run("MATCH (c:Company) DETACH DELETE c")
        graph.close()
        
        if pwc_cleared and non_pwc_preserved:
            logger.info("✅ --clear-all flag behavior test passed!")
            return True
        else:
            logger.error("❌ --clear-all flag behavior test failed!")
            logger.error(f"PWC cleared: {pwc_cleared}, Non-PWC preserved: {non_pwc_preserved}")
            return False
        
    except Exception as e:
        logger.error(f"❌ --clear-all flag test failed: {e}")
        return False

def run_all_tests():
    """Run all selective clearing tests"""
    logger.info("🚀 Starting selective clearing tests...")
    
    tests = [
        ("Selective Clearing", test_selective_clearing),
        ("PWC-Only Diff", test_pwc_only_diff),
        ("Clear-All Flag Behavior", test_clear_all_flag),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*60}")
        logger.info(f"Running: {test_name}")
        logger.info('='*60)
        
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ Test {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("📊 TEST SUMMARY")
    logger.info('='*60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status} {test_name}")
        if result:
            passed += 1
    
    logger.info(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All selective clearing tests passed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)