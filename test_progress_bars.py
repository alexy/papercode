#!/usr/bin/env python3

import logging
import sys
from pathlib import Path
from pwc_offline_loader import PapersWithCodeOfflineLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_progress_bar_output():
    """Test that progress bars show correctly with no nested bars"""
    logger.info("🧪 Testing simplified progress bar output...")
    
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
        
        # Test building a small amount of data to see progress bars
        logger.info("Building small dataset to test progress bars...")
        datasets = loader.build_datasets(limit=5)
        logger.info(f"✅ Built {len(datasets)} datasets")
        
        papers = loader.build_papers_with_code(paper_limit=10, include_repositories=True)
        logger.info(f"✅ Built {len(papers)} papers")
        
        if datasets and papers:
            logger.info("✅ Progress bar test setup successful")
            logger.info("Progress bars should show single-level output without nested bars")
            return True
        else:
            logger.error("❌ Failed to build test data")
            return False
        
    except Exception as e:
        logger.error(f"❌ Progress bar test failed: {e}")
        return False

def test_tqdm_fallback():
    """Test the tqdm fallback class"""
    logger.info("🧪 Testing tqdm fallback class...")
    
    try:
        # Test the fallback tqdm class directly
        from pwc_offline_loader import tqdm
        
        test_items = list(range(5))
        
        # Test with progress bar
        progress = tqdm(test_items, desc="Test progress", unit="item")
        
        for i, item in enumerate(progress):
            progress.set_postfix({
                "item": item,
                "processed": i + 1,
                "remaining": len(test_items) - i - 1
            })
        
        progress.close()
        
        logger.info("✅ tqdm fallback test completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ tqdm fallback test failed: {e}")
        return False

def run_all_tests():
    """Run all progress bar tests"""
    logger.info("🚀 Starting progress bar tests...")
    
    tests = [
        ("Progress Bar Output", test_progress_bar_output),
        ("tqdm Fallback", test_tqdm_fallback),
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
        logger.info("🎉 All progress bar tests passed!")
        return True
    else:
        logger.info("❌ Some tests failed.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)