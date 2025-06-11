#!/usr/bin/env python3

import logging
from pathlib import Path
from pwc_offline_loader import PapersWithCodeOfflineLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def demo_offline_pipeline():
    """Demonstrate the offline pipeline functionality"""
    logger.info("🎬 Papers with Code Offline Pipeline Demo")
    
    # Find data directory
    base_dir = Path(".")
    pwc_dirs = list(base_dir.glob("pwc-*"))
    
    if not pwc_dirs:
        logger.error("❌ No PWC data directory found")
        logger.info("💡 First run: python pwc_dataset_downloader.py --datasets papers links datasets")
        return False
    
    data_dir = sorted(pwc_dirs)[-1]
    logger.info(f"📁 Using data directory: {data_dir}")
    
    try:
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(str(data_dir))
        
        # Show what data is available
        summary = loader.get_data_summary()
        logger.info("📋 Available Data:")
        for dataset_key, available in summary['files_available'].items():
            status = "✅" if available else "❌"
            logger.info(f"   {status} {dataset_key}")
        
        if not summary['files_available'].get('papers'):
            logger.error("❌ Papers data not available - download it first")
            return False
        
        # Demo 1: Parse datasets
        logger.info("\n🗄️ Demo 1: Loading Datasets")
        datasets = loader.build_datasets(limit=3)
        logger.info(f"✅ Loaded {len(datasets)} datasets")
        for i, dataset in enumerate(datasets, 1):
            logger.info(f"   {i}. {dataset.name} - {dataset.description[:50] if dataset.description else 'No description'}...")
        
        # Demo 2: Parse repositories  
        logger.info("\n💾 Demo 2: Loading Repositories")
        repositories = loader.build_repositories(limit=3)
        logger.info(f"✅ Loaded {len(repositories)} unique repositories")
        for i, repo in enumerate(repositories, 1):
            logger.info(f"   {i}. {repo.url} (Framework: {repo.framework})")
        
        # Demo 3: Parse papers
        logger.info("\n📄 Demo 3: Loading Papers")
        papers = loader.build_papers_with_code(paper_limit=3, include_repositories=False)
        logger.info(f"✅ Loaded {len(papers)} papers")
        for i, paper in enumerate(papers, 1):
            logger.info(f"   {i}. {paper.title[:60]}...")
            logger.info(f"      Authors: {len(paper.authors)}, Published: {paper.published}")
        
        # Demo 4: Paper-Repository relationships
        logger.info("\n🔗 Demo 4: Paper-Repository Relationships")
        mapping = loader.build_paper_repository_mapping()
        logger.info(f"✅ Built relationships for {len(mapping)} papers")
        
        # Show examples
        example_count = 0
        for paper_id, repos in mapping.items():
            if example_count >= 3:
                break
            logger.info(f"   Paper {paper_id}: {len(repos)} repositories")
            for repo in repos[:2]:  # Show first 2 repos
                logger.info(f"     - {repo.url}")
            example_count += 1
        
        # Demo 5: Build papers WITH repositories
        logger.info("\n🚀 Demo 5: Papers with Code Relationships")
        papers_with_code = loader.build_papers_with_code(paper_limit=3, include_repositories=True)
        logger.info(f"✅ Built {len(papers_with_code)} papers with code relationships")
        
        for i, paper in enumerate(papers_with_code, 1):
            if paper.repositories:
                logger.info(f"   {i}. {paper.title[:50]}...")
                logger.info(f"      Code: {len(paper.repositories)} repositories")
                for repo in paper.repositories[:2]:
                    logger.info(f"        - {repo.url}")
            else:
                logger.info(f"   {i}. {paper.title[:50]}... (no code repositories)")
        
        # Final stats
        logger.info(f"\n📊 Final Statistics:")
        logger.info(f"   Papers processed: {loader.stats['papers_processed']}")
        logger.info(f"   Repositories processed: {loader.stats['repositories_processed']}")
        logger.info(f"   Datasets processed: {loader.stats['datasets_processed']}")
        logger.info(f"   Links processed: {loader.stats['links_processed']}")
        logger.info(f"   Errors: {loader.stats['errors']}")
        
        logger.info("\n✅ Demo completed successfully!")
        logger.info("💡 To load this data into Neo4j, run:")
        logger.info(f"   python pwc_offline_loader.py {data_dir} --neo4j-password YOUR_PASSWORD")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        return False

def main():
    """Main function"""
    success = demo_offline_pipeline()
    
    if not success:
        logger.info("\n🔧 Troubleshooting:")
        logger.info("1. Download data: python pwc_dataset_downloader.py --datasets papers links")
        logger.info("2. Check data: ls pwc-*/")
        logger.info("3. Test parsing: python test_offline_data_only.py")

if __name__ == "__main__":
    main()