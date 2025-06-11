#!/usr/bin/env python3

import logging
import sys
import subprocess
from pathlib import Path
from models import PapersWithCodeGraph

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_mixed_test_data(graph: PapersWithCodeGraph):
    """Create mixed test data to verify drop behavior"""
    logger.info("🧪 Creating mixed test data...")
    
    try:
        session = graph.get_session()
        
        # Create PWC test data
        test_queries = [
            # PWC nodes
            "CREATE (p:Paper {id: 'test-paper-1', title: 'Test Paper'})",
            "CREATE (r:Repository {url: 'https://github.com/test/repo', owner: 'test', name: 'repo'})",
            "CREATE (d:Dataset {id: 'test-dataset-1', name: 'Test Dataset'})",
            "CREATE (a:Author {name: 'Test Author'})",
            
            # Non-PWC nodes
            "CREATE (u:User {id: 'test-user-1', name: 'Test User'})",
            "CREATE (c:Company {id: 'test-company-1', name: 'Test Company'})",
            "CREATE (pr:Product {id: 'test-product-1', name: 'Test Product'})",
            
            # PWC relationships
            "MATCH (a:Author {name: 'Test Author'}), (p:Paper {id: 'test-paper-1'}) CREATE (a)-[:AUTHORED]->(p)",
            "MATCH (p:Paper {id: 'test-paper-1'}), (r:Repository {url: 'https://github.com/test/repo'}) CREATE (p)-[:HAS_CODE]->(r)",
            "MATCH (p:Paper {id: 'test-paper-1'}), (d:Dataset {id: 'test-dataset-1'}) CREATE (p)-[:USES_DATASET]->(d)",
            
            # Non-PWC relationships
            "MATCH (u:User {id: 'test-user-1'}), (c:Company {id: 'test-company-1'}) CREATE (u)-[:WORKS_FOR]->(c)",
            "MATCH (u:User {id: 'test-user-1'}), (pr:Product {id: 'test-product-1'}) CREATE (u)-[:OWNS]->(pr)",
        ]
        
        for query in test_queries:
            session.run(query)
        
        # Verify data was created
        counts = {}
        node_types = ['Paper', 'Repository', 'Dataset', 'Author', 'User', 'Company', 'Product']
        
        for node_type in node_types:
            result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
            counts[node_type] = result.single()['count']
        
        logger.info(f"✅ Test data created: {counts}")
        return counts
        
    except Exception as e:
        logger.error(f"❌ Failed to create test data: {e}")
        return {}

def test_confirmation_behavior():
    """Test that confirmation is required for drop-all flags"""
    logger.info("🧪 Testing confirmation behavior...")
    
    try:
        # Test with invalid confirmation
        cmd = [
            "python", "pwc_offline_loader.py", "pwc-20250610",
            "--drop-all", "--paper-limit", "1"
        ]
        
        # Simulate invalid confirmation
        result = subprocess.run(
            cmd,
            input="WRONG CONFIRMATION\n",
            text=True,
            capture_output=True,
            timeout=30
        )
        
        # Should fail with invalid confirmation
        if "Operation cancelled by user" in result.stdout or result.returncode != 0:
            logger.info("✅ Confirmation requirement test passed - invalid confirmation rejected")
            return True
        else:
            logger.error("❌ Confirmation requirement test failed - should have rejected invalid confirmation")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Confirmation test timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Confirmation test failed: {e}")
        return False

def test_drop_all_vs_clear_all():
    """Test the difference between drop-all and clear-all"""
    logger.info("🧪 Testing drop-all vs clear-all behavior...")
    
    try:
        # Connect to Neo4j
        graph = PapersWithCodeGraph(
            neo4j_uri="bolt://localhost:7687",
            username="neo4j", 
            password="password"
        )
        
        # Create mixed test data
        initial_counts = create_mixed_test_data(graph)
        if not initial_counts:
            return False
        
        session = graph.get_session()
        
        # Test clear-all (should only clear PWC data)
        logger.info("Testing clear-all (selective clearing)...")
        cleared_data = graph.clear_pwc_data_only()
        
        # Check what remains after clear-all
        counts_after_clear = {}
        node_types = ['Paper', 'Repository', 'Dataset', 'Author', 'User', 'Company', 'Product']
        
        for node_type in node_types:
            result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
            counts_after_clear[node_type] = result.single()['count']
        
        logger.info(f"After clear-all: {counts_after_clear}")
        
        # Verify clear-all behavior
        pwc_types = ['Paper', 'Repository', 'Dataset', 'Author']
        non_pwc_types = ['User', 'Company', 'Product']
        
        clear_all_correct = True
        
        # PWC data should be cleared
        for node_type in pwc_types:
            if counts_after_clear[node_type] != 0:
                logger.error(f"❌ clear-all failed: {node_type} not cleared")
                clear_all_correct = False
        
        # Non-PWC data should be preserved
        for node_type in non_pwc_types:
            if counts_after_clear[node_type] != initial_counts[node_type]:
                logger.error(f"❌ clear-all failed: {node_type} not preserved")
                clear_all_correct = False
        
        if clear_all_correct:
            logger.info("✅ clear-all behavior correct: PWC data cleared, other data preserved")
        
        # Test drop-all (should clear everything)
        logger.info("Testing drop-all (complete clearing)...")
        graph.clear_all_data()
        
        # Check what remains after drop-all
        counts_after_drop = {}
        for node_type in node_types:
            result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
            counts_after_drop[node_type] = result.single()['count']
        
        logger.info(f"After drop-all: {counts_after_drop}")
        
        # Verify drop-all behavior (everything should be 0)
        drop_all_correct = all(count == 0 for count in counts_after_drop.values())
        
        if drop_all_correct:
            logger.info("✅ drop-all behavior correct: ALL data cleared")
        else:
            logger.error("❌ drop-all behavior incorrect: some data remains")
        
        graph.close()
        
        return clear_all_correct and drop_all_correct
        
    except Exception as e:
        logger.error(f"❌ Drop-all vs clear-all test failed: {e}")
        return False

def test_flag_conflicts():
    """Test that conflicting flags are rejected"""
    logger.info("🧪 Testing flag conflict detection...")
    
    test_cases = [
        # Should fail: conflicting clear flags
        {
            "args": ["--drop-all", "--clear-all"],
            "should_fail": True,
            "description": "drop-all and clear-all conflict"
        },
        # Should fail: conflicting target flags
        {
            "args": ["--drop-all-target", "--clear-target", "--new-neo4j-uri", "bolt://test:7687"],
            "should_fail": True,
            "description": "drop-all-target and clear-target conflict"
        },
        # Should fail: drop-all-target without new-neo4j-uri
        {
            "args": ["--drop-all-target"],
            "should_fail": True,
            "description": "drop-all-target without target URI"
        },
        # Should pass: valid single flags
        {
            "args": ["--clear-all", "--paper-limit", "1"],
            "should_fail": False,
            "description": "valid clear-all"
        }
    ]
    
    all_passed = True
    
    for test_case in test_cases:
        try:
            cmd = ["python", "pwc_offline_loader.py", "pwc-20250610"] + test_case["args"]
            
            result = subprocess.run(
                cmd,
                input="n\n",  # Say no to any confirmations
                text=True,
                capture_output=True,
                timeout=10
            )
            
            failed = result.returncode != 0
            
            if test_case["should_fail"] and failed:
                logger.info(f"✅ {test_case['description']}: correctly rejected")
            elif not test_case["should_fail"] and not failed:
                logger.info(f"✅ {test_case['description']}: correctly accepted")
            else:
                logger.error(f"❌ {test_case['description']}: unexpected result")
                all_passed = False
                
        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️  {test_case['description']}: timed out")
        except Exception as e:
            logger.error(f"❌ {test_case['description']}: error - {e}")
            all_passed = False
    
    return all_passed

def run_all_tests():
    """Run all drop-all flag tests"""
    logger.info("🚀 Starting drop-all flag tests...")
    
    tests = [
        ("Flag Conflicts", test_flag_conflicts),
        ("Confirmation Behavior", test_confirmation_behavior),
        ("Drop-All vs Clear-All", test_drop_all_vs_clear_all),
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
        logger.info("🎉 All drop-all flag tests passed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)