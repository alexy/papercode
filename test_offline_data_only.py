#!/usr/bin/env python3

import logging
from pathlib import Path
from pwc_offline_loader import PapersWithCodeOfflineLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_offline_data_parsing():
    """Test offline data parsing without Neo4j"""
    logger.info("🧪 Testing offline data parsing (no Neo4j required)")
    
    # Find data directory
    base_dir = Path(".")
    pwc_dirs = list(base_dir.glob("pwc-*"))
    
    if not pwc_dirs:
        logger.error("No PWC data directory found. Run pwc_dataset_downloader.py first.")
        return False
    
    data_dir = sorted(pwc_dirs)[-1]
    logger.info(f"Using data directory: {data_dir}")
    
    try:
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Show data summary
        summary = loader.get_data_summary()
        logger.info(f"📋 Data Summary:")
        for dataset_key, available in summary['files_available'].items():
            status = "✅" if available else "❌"
            logger.info(f"   {status} {dataset_key}")
        
        # Test building datasets
        logger.info("\n🗄️ Testing dataset parsing...")
        datasets = loader.build_datasets(limit=3)
        logger.info(f"✅ Successfully parsed {len(datasets)} datasets")
        if datasets:
            logger.info(f"Sample dataset: {datasets[0].name}")
        
        # Test building repositories (if links data available)
        logger.info("\n💾 Testing repository parsing...")
        try:
            repositories = loader.build_repositories(limit=3)
            logger.info(f"✅ Successfully parsed {len(repositories)} repositories")
            if repositories:
                logger.info(f"Sample repository: {repositories[0].url}")
        except Exception as e:
            logger.warning(f"Repository parsing failed (may need links data): {e}")
        
        # Test building papers (if papers data available)
        logger.info("\n📄 Testing paper parsing...")
        try:
            papers = loader.build_papers_with_code(paper_limit=2, include_repositories=False)
            logger.info(f"✅ Successfully parsed {len(papers)} papers")
            if papers:
                logger.info(f"Sample paper: {papers[0].title}")
                logger.info(f"   Authors: {len(papers[0].authors)}")
        except Exception as e:
            logger.warning(f"Paper parsing failed (may need papers data): {e}")
        
        # Test paper-repository mapping
        logger.info("\n🔗 Testing paper-repository mapping...")
        try:
            mapping = loader.build_paper_repository_mapping()
            logger.info(f"✅ Built mapping for {len(mapping)} papers")
            
            if mapping:
                sample_paper_id = list(mapping.keys())[0]
                repos = mapping[sample_paper_id]
                logger.info(f"Sample: Paper {sample_paper_id} has {len(repos)} repositories")
        except Exception as e:
            logger.warning(f"Paper-repository mapping failed: {e}")
        
        logger.info(f"\n📊 Final stats: {loader.stats}")
        logger.info("✅ Offline data parsing test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Offline data parsing test failed: {e}")
        return False

def main():
    """Main function"""
    success = test_offline_data_parsing()
    
    if success:
        logger.info("\n🎯 Data parsing works! You can now:")
        logger.info("   1. Fix Neo4j connection issues")
        logger.info("   2. Run: python pwc_offline_loader.py --neo4j-password YOUR_PASSWORD")
        logger.info("   3. Or continue testing with: python test_neo4j_connection.py")
    else:
        logger.info("\n❌ Data parsing issues found. Check your PWC data files.")

if __name__ == "__main__":
    main()