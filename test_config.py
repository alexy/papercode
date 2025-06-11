#!/usr/bin/env python3

import logging
import sys
import tempfile
import os
from pathlib import Path
from config_parser import Neo4jConfig, create_example_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_config_creation():
    """Test creating an example config file"""
    logger.info("🧪 Testing config file creation...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            
            # Test creating example config
            success = create_example_config(config_path)
            if not success:
                logger.error("❌ Failed to create example config")
                return False
            
            # Verify file was created
            if not Path(config_path).exists():
                logger.error("❌ Config file was not created")
                return False
            
            logger.info("✅ Config file creation test passed")
            return True
            
    except Exception as e:
        logger.error(f"❌ Config creation test failed: {e}")
        return False

def test_config_parsing():
    """Test parsing a config file"""
    logger.info("🧪 Testing config file parsing...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            
            # Create test config
            test_config_content = """
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "test_password"
  description: "Test local instance"

remote:
  uri: "bolt://remote.example.com:7687"
  user: "neo4j_remote"
  password: "remote_test_password"
  description: "Test remote instance"

staging:
  uri: "bolt://staging.example.com:7687"
  user: "neo4j_staging"
  password: "staging_password"
"""
            
            with open(config_path, 'w') as f:
                f.write(test_config_content)
            
            # Test parsing
            config = Neo4jConfig(config_path)
            
            # Test local config
            local_config = config.get_local_config()
            if not local_config:
                logger.error("❌ Failed to get local config")
                return False
            
            if local_config['uri'] != "bolt://localhost:7687":
                logger.error(f"❌ Incorrect local URI: {local_config['uri']}")
                return False
            
            # Test remote config
            remote_config = config.get_remote_config()
            if not remote_config:
                logger.error("❌ Failed to get remote config")
                return False
            
            if remote_config['user'] != "neo4j_remote":
                logger.error(f"❌ Incorrect remote user: {remote_config['user']}")
                return False
            
            # Test environment listing
            environments = config.list_environments()
            expected_envs = ['local', 'remote', 'staging']
            if set(environments) != set(expected_envs):
                logger.error(f"❌ Incorrect environments: {environments}, expected: {expected_envs}")
                return False
            
            # Test custom environment
            staging_config = config.get_environment_config('staging')
            if not staging_config:
                logger.error("❌ Failed to get staging config")
                return False
            
            logger.info("✅ Config parsing test passed")
            return True
            
    except Exception as e:
        logger.error(f"❌ Config parsing test failed: {e}")
        return False

def test_config_validation():
    """Test config validation"""
    logger.info("🧪 Testing config validation...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "test_config.yaml")
            
            # Create config with issues
            test_config_content = """
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "password"  # Insecure password
  description: "Test local instance"

remote:
  uri: "http://remote.example.com:7687"  # Wrong protocol
  user: "neo4j_remote"
  # missing password

invalid_env: "not a dictionary"
"""
            
            with open(config_path, 'w') as f:
                f.write(test_config_content)
            
            # Test validation
            config = Neo4jConfig(config_path)
            validation = config.validate_config()
            
            # Should have errors
            if validation['valid']:
                logger.error("❌ Validation should have failed")
                return False
            
            # Should have specific issues
            if not validation['errors']:
                logger.error("❌ Should have validation errors")
                return False
            
            if not validation['warnings']:
                logger.error("❌ Should have validation warnings")
                return False
            
            logger.info("✅ Config validation test passed")
            return True
            
    except Exception as e:
        logger.error(f"❌ Config validation test failed: {e}")
        return False

def test_missing_config():
    """Test handling of missing config file"""
    logger.info("🧪 Testing missing config file handling...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nonexistent_config.yaml")
            
            # Test with missing config
            config = Neo4jConfig(config_path)
            
            # Should handle gracefully
            local_config = config.get_local_config()
            if local_config is not None:
                logger.error("❌ Should return None for missing config")
                return False
            
            environments = config.list_environments()
            if environments:
                logger.error("❌ Should return empty list for missing config")
                return False
            
            logger.info("✅ Missing config test passed")
            return True
            
    except Exception as e:
        logger.error(f"❌ Missing config test failed: {e}")
        return False

def test_real_config():
    """Test with the actual config.yaml file if it exists"""
    logger.info("🧪 Testing with real config.yaml file...")
    
    try:
        config_path = "config.yaml"
        if not Path(config_path).exists():
            logger.info("ℹ️  No config.yaml found - skipping real config test")
            return True
        
        # Test with real config
        config = Neo4jConfig(config_path)
        
        # Just test that it loads without errors
        environments = config.list_environments()
        logger.info(f"Real config environments: {environments}")
        
        # Test validation
        validation = config.validate_config()
        if not validation['valid']:
            logger.warning("⚠️  Real config has validation issues:")
            for error in validation['errors']:
                logger.warning(f"   Error: {error}")
            for warning in validation['warnings']:
                logger.warning(f"   Warning: {warning}")
        else:
            logger.info("✅ Real config is valid")
        
        logger.info("✅ Real config test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Real config test failed: {e}")
        return False

def run_all_tests():
    """Run all config tests"""
    logger.info("🚀 Starting config system tests...")
    
    tests = [
        ("Config Creation", test_config_creation),
        ("Config Parsing", test_config_parsing),
        ("Config Validation", test_config_validation),
        ("Missing Config Handling", test_missing_config),
        ("Real Config Test", test_real_config),
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
        logger.info("🎉 All config tests passed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)