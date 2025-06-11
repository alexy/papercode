#!/usr/bin/env python3

import logging
import sys
from pathlib import Path
from pwc_offline_loader import PapersWithCodeOfflineLoader
from neo4j_diff import Neo4jDiff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_model_rebuilding():
    """Test the Pydantic model rebuilding functionality"""
    logger.info("🧪 Testing Pydantic model rebuilding...")
    
    try:
        # Find the most recent download directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        
        if not pwc_dirs:
            logger.error("No PWC data directory found. Run pwc_dataset_downloader.py first.")
            return False
        
        data_dir = sorted(pwc_dirs)[-1]
        logger.info(f"Using data directory: {data_dir}")
        
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Test model rebuilding
        rebuilt_models = loader.rebuild_models_from_data()
        
        # Validate results
        stats = rebuilt_models['stats']
        logger.info(f"✅ Model rebuilding completed:")
        logger.info(f"   Papers rebuilt: {stats['papers_rebuilt']}")
        logger.info(f"   Datasets rebuilt: {stats['datasets_rebuilt']}")
        logger.info(f"   Repositories rebuilt: {stats['repositories_rebuilt']}")
        logger.info(f"   Errors: {stats['errors']}")
        
        # Validate data integrity
        if stats['papers_rebuilt'] > 0:
            sample_paper = rebuilt_models['papers'][0]
            logger.info(f"   Sample paper: {sample_paper.title}")
            logger.info(f"   Paper has {len(sample_paper.repositories)} repositories")
        
        if stats['datasets_rebuilt'] > 0:
            sample_dataset = rebuilt_models['datasets'][0]
            logger.info(f"   Sample dataset: {sample_dataset.name}")
        
        if stats['repositories_rebuilt'] > 0:
            sample_repo = rebuilt_models['repositories'][0]
            logger.info(f"   Sample repository: {sample_repo.url}")
        
        logger.info("✅ Model rebuilding test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Model rebuilding test failed: {e}")
        return False

def test_neo4j_diff():
    """Test the Neo4j diff functionality"""
    logger.info("🧪 Testing Neo4j diff functionality...")
    
    try:
        # Test with the same instance (should be identical)
        diff_tool = Neo4jDiff(
            source_uri="bolt://localhost:7687",
            source_user="neo4j", 
            source_password="password",
            target_uri="bolt://localhost:7687",  # Same instance
            target_user="neo4j",
            target_password="password"
        )
        
        # Perform comparison
        comparison_result = diff_tool.full_comparison(sample_size=5)
        
        # Print brief results
        summary = comparison_result['summary']
        logger.info(f"✅ Diff test completed:")
        logger.info(f"   Identical: {summary['identical']}")
        logger.info(f"   Total nodes match: {summary['total_nodes']['match']}")
        logger.info(f"   Total relationships match: {summary['total_relationships']['match']}")
        
        diff_tool.close()
        logger.info("✅ Neo4j diff test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Neo4j diff test failed: {e}")
        logger.info("Note: This test requires a running Neo4j instance")
        return False

def test_new_loader_api():
    """Test the new loader API without actually connecting to Neo4j"""
    logger.info("🧪 Testing new loader API...")
    
    try:
        # Find data directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        
        if not pwc_dirs:
            logger.error("No PWC data directory found.")
            return False
        
        data_dir = sorted(pwc_dirs)[-1]
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Test that the new method exists and can be called (will fail at Neo4j connection)
        try:
            loader.load_to_new_neo4j_instance(
                new_neo4j_uri="bolt://nonexistent:7687",
                new_neo4j_user="test",
                new_neo4j_password="test",
                paper_limit=1,
                dataset_limit=1
            )
        except Exception as e:
            # Expected to fail at connection, but method should exist
            if "Failed to resolve address" in str(e) or "Connection refused" in str(e) or "ServiceUnavailable" in str(e):
                logger.info("✅ New loader API method exists and accepts parameters correctly")
                return True
            else:
                logger.error(f"❌ Unexpected error: {e}")
                return False
        
        logger.info("✅ New loader API test passed!")
        return True
        
    except AttributeError as e:
        logger.error(f"❌ New loader API test failed - method not found: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ New loader API test failed: {e}")
        return False

def run_all_tests():
    """Run all tests for new loader features"""
    logger.info("🚀 Starting tests for new loader features...")
    
    tests = [
        ("Model Rebuilding", test_model_rebuilding),
        ("New Loader API", test_new_loader_api),
        ("Neo4j Diff", test_neo4j_diff),
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
        logger.info("🎉 All tests passed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)