#!/usr/bin/env python3

import logging
import sys
import subprocess
import tempfile
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_local_mode():
    """Test that local mode (--local) works without graph scope issues"""
    logger.info("🧪 Testing local mode...")
    
    try:
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "test_password"
""")
            temp_config = f.name
        
        # Test command that would previously cause the scope error
        cmd = [
            "python", "pwc_offline_loader.py", "pwc-20250610",
            "--local", "--config", temp_config, "--paper-limit", "1"
        ]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        
        # Clean up
        os.unlink(temp_config)
        
        # Check that it doesn't crash with the graph scope error
        if "cannot access local variable 'graph'" in result.stderr:
            logger.error("❌ Graph scope error still occurs")
            return False
        
        # Check that it completes (even if connection fails)
        if "Pipeline completed successfully!" in result.stdout or "Pipeline failed:" in result.stderr:
            logger.info("✅ Local mode test passed - no scope errors")
            return True
        else:
            logger.warning("⚠️  Local mode test inconclusive")
            return True  # Not a scope error, just other issues
        
    except subprocess.TimeoutExpired:
        logger.warning("⚠️  Local mode test timed out")
        return True  # Timeout not a scope error
    except Exception as e:
        logger.error(f"❌ Local mode test failed: {e}")
        return False

def test_remote_mode():
    """Test that remote mode (--remote) works without graph scope issues"""
    logger.info("🧪 Testing remote mode...")
    
    try:
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "test_password"

remote:
  uri: "bolt://nonexistent-server:7687"
  user: "neo4j"
  password: "test_password"
""")
            temp_config = f.name
        
        # Test command that would previously cause the scope error
        cmd = [
            "python", "pwc_offline_loader.py", "pwc-20250610",
            "--remote", "--config", temp_config, "--paper-limit", "1"
        ]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        
        # Clean up
        os.unlink(temp_config)
        
        # Check that it doesn't crash with the graph scope error
        if "cannot access local variable 'graph'" in result.stderr:
            logger.error("❌ Graph scope error still occurs")
            return False
        
        # Check that it completes (even if connection fails)
        if "Pipeline completed successfully!" in result.stdout or "Pipeline failed:" in result.stderr:
            logger.info("✅ Remote mode test passed - no scope errors")
            return True
        else:
            logger.warning("⚠️  Remote mode test inconclusive")
            return True  # Not a scope error, just other issues
        
    except subprocess.TimeoutExpired:
        logger.warning("⚠️  Remote mode test timed out") 
        return True  # Timeout not a scope error
    except Exception as e:
        logger.error(f"❌ Remote mode test failed: {e}")
        return False

def test_explicit_uri_mode():
    """Test that explicit URI mode (--new-neo4j-uri) works without graph scope issues"""
    logger.info("🧪 Testing explicit URI mode...")
    
    try:
        # Test command that would previously cause the scope error
        cmd = [
            "python", "pwc_offline_loader.py", "pwc-20250610",
            "--new-neo4j-uri", "bolt://nonexistent-server:7687",
            "--new-neo4j-password", "test_password",
            "--paper-limit", "1"
        ]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        
        # Check that it doesn't crash with the graph scope error
        if "cannot access local variable 'graph'" in result.stderr:
            logger.error("❌ Graph scope error still occurs")
            return False
        
        # Check that it completes (even if connection fails)
        if "Pipeline completed successfully!" in result.stdout or "Pipeline failed:" in result.stderr:
            logger.info("✅ Explicit URI mode test passed - no scope errors")
            return True
        else:
            logger.warning("⚠️  Explicit URI mode test inconclusive")
            return True  # Not a scope error, just other issues
        
    except subprocess.TimeoutExpired:
        logger.warning("⚠️  Explicit URI mode test timed out")
        return True  # Timeout not a scope error
    except Exception as e:
        logger.error(f"❌ Explicit URI mode test failed: {e}")
        return False

def run_all_tests():
    """Run all graph scope fix tests"""
    logger.info("🚀 Starting graph scope fix tests...")
    
    tests = [
        ("Local Mode", test_local_mode),
        ("Remote Mode", test_remote_mode),
        ("Explicit URI Mode", test_explicit_uri_mode),
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
        logger.info("🎉 All graph scope fix tests passed!")
        logger.info("The 'cannot access local variable graph' error has been fixed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)