#!/usr/bin/env python3

import logging
from pathlib import Path
from pwc_dataset_downloader import PapersWithCodeDatasetDownloader
from pwc_offline_loader import PapersWithCodeOfflineLoader
from models import PapersWithCodeGraph

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_dataset_downloader():
    """Test the dataset downloader with small sample"""
    logger.info("🧪 Testing Papers with Code dataset downloader...")
    
    try:
        downloader = PapersWithCodeDatasetDownloader()
        
        # Test downloading just one file
        logger.info("Testing single file download...")
        success = downloader.download_file('datasets')
        
        if success:
            logger.info("✅ Single file download test passed")
            
            # Test extraction
            extracted_path = downloader.extract_file('datasets')
            if extracted_path:
                logger.info("✅ File extraction test passed")
                
                # Test loading data
                records = downloader.load_json_file('datasets', max_records=5)
                if records:
                    logger.info(f"✅ JSON loading test passed - loaded {len(records)} records")
                    logger.info(f"Sample record: {records[0] if records else 'None'}")
                else:
                    logger.error("❌ JSON loading test failed")
            else:
                logger.error("❌ File extraction test failed")
        else:
            logger.error("❌ Single file download test failed")
        
        # Get download info
        info = downloader.get_download_info()
        logger.info(f"Download info: {info}")
        
    except Exception as e:
        logger.error(f"❌ Dataset downloader test failed: {e}")

def test_offline_loader():
    """Test the offline loader"""
    logger.info("🧪 Testing offline data loader...")
    
    try:
        # Find the most recent download directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        
        if not pwc_dirs:
            logger.warning("No PWC data directory found. Running downloader first...")
            downloader = PapersWithCodeDatasetDownloader()
            downloader.download_file('datasets')  # Download at least one file
            downloader.extract_file('datasets')
            data_dir = downloader.download_dir
        else:
            data_dir = sorted(pwc_dirs)[-1]  # Most recent
            logger.info(f"Using data directory: {data_dir}")
        
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Test data summary
        summary = loader.get_data_summary()
        logger.info(f"Data summary: {summary}")
        
        # Test building datasets
        datasets = loader.build_datasets(limit=5)
        logger.info(f"✅ Built {len(datasets)} datasets")
        if datasets:
            logger.info(f"Sample dataset: {datasets[0].name}")
        
        # Test building repositories (if links data is available)
        try:
            repositories = loader.build_repositories(limit=5)
            logger.info(f"✅ Built {len(repositories)} repositories")
            if repositories:
                logger.info(f"Sample repository: {repositories[0].url}")
        except Exception as e:
            logger.warning(f"Repository building failed (links data may not be available): {e}")
        
        # Test building papers (if papers data is available)
        try:
            papers = loader.build_papers_with_code(paper_limit=3, include_repositories=False)
            logger.info(f"✅ Built {len(papers)} papers")
            if papers:
                logger.info(f"Sample paper: {papers[0].title}")
        except Exception as e:
            logger.warning(f"Paper building failed (papers data may not be available): {e}")
        
        logger.info(f"Loader stats: {loader.stats}")
        
    except Exception as e:
        logger.error(f"❌ Offline loader test failed: {e}")

def test_neo4j_integration():
    """Test Neo4j integration with offline data (optional - requires Neo4j)"""
    logger.info("🧪 Testing Neo4j integration with offline data...")
    
    try:
        # Find data directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        
        if not pwc_dirs:
            logger.warning("No PWC data directory found. Skipping Neo4j test.")
            return
        
        data_dir = sorted(pwc_dirs)[-1]
        
        # Initialize components
        graph = PapersWithCodeGraph(
            neo4j_uri="bolt://localhost:7687",
            username="neo4j",
            password="password"  # Change to your password
        )
        
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Load small amount of data
        stats = loader.load_and_save_to_neo4j(
            graph=graph,
            paper_limit=5,
            dataset_limit=5,
            include_repositories=True
        )
        
        logger.info(f"✅ Neo4j integration test completed")
        logger.info(f"Final stats: {stats}")
        
        graph.close()
        
    except Exception as e:
        logger.error(f"❌ Neo4j integration test failed: {e}")
        logger.info("Make sure Neo4j is running on bolt://localhost:7687")

def download_sample_data():
    """Download a small sample of data for testing"""
    logger.info("📥 Downloading sample data for testing...")
    
    try:
        downloader = PapersWithCodeDatasetDownloader()
        
        # Download just a couple of files for testing
        files_to_download = ['datasets', 'links']
        
        for file_key in files_to_download:
            logger.info(f"Downloading {file_key}...")
            if downloader.download_file(file_key):
                downloader.extract_file(file_key)
                logger.info(f"✅ {file_key} ready")
            else:
                logger.error(f"❌ Failed to download {file_key}")
        
        return str(downloader.download_dir)
        
    except Exception as e:
        logger.error(f"❌ Sample data download failed: {e}")
        return None

def run_all_tests():
    """Run all offline pipeline tests"""
    logger.info("🚀 Starting Papers with Code Offline Pipeline Tests")
    
    # Test 1: Dataset downloader
    test_dataset_downloader()
    
    # Test 2: Offline loader
    test_offline_loader()
    
    # Test 3: Neo4j integration (optional)
    try:
        test_neo4j_integration()
    except Exception as e:
        logger.warning(f"Neo4j test skipped: {e}")
    
    logger.info("🎉 All offline pipeline tests completed!")

if __name__ == "__main__":
    run_all_tests()