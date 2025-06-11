#!/usr/bin/env python3

import logging
from datetime import datetime
from models import Paper, Repository, Dataset, Task, Author, PapersWithCodeGraph, Framework
from pwc_loader import PapersWithCodeLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pydantic_models():
    """Test Pydantic model creation and validation"""
    logger.info("Testing Pydantic model creation...")
    
    # Test Author
    author = Author(name="John Doe", email="john@example.com", affiliation="MIT")
    assert author.name == "John Doe"
    
    # Test Dataset
    dataset = Dataset(
        id="imagenet",
        name="ImageNet",
        full_name="ImageNet Dataset",
        url="http://www.image-net.org/",
        description="Large visual database",
        paper_count=1000
    )
    assert dataset.name == "ImageNet"
    
    # Test Repository
    repo = Repository(
        url="https://github.com/pytorch/pytorch",
        owner="pytorch",
        name="pytorch",
        description="Deep learning framework",
        stars=50000,
        framework=Framework.PYTORCH,
        language="Python"
    )
    assert repo.framework == Framework.PYTORCH
    
    # Test Paper
    paper = Paper(
        id="1234",
        title="Deep Learning Paper",
        abstract="A paper about deep learning",
        authors=[author],
        published=datetime.now(),
        repositories=[repo],
        datasets=["imagenet"],
        tasks=["image-classification"]
    )
    assert len(paper.authors) == 1
    assert len(paper.repositories) == 1
    
    logger.info("✅ Pydantic models test passed")

def test_neo4j_integration():
    """Test Neo4j integration (requires running Neo4j instance)"""
    logger.info("Testing Neo4j integration...")
    
    try:
        # Initialize test graph
        graph = PapersWithCodeGraph(
            neo4j_uri="bolt://localhost:7687",
            username="neo4j",
            password="password"  # Change to your password
        )
        
        # Test dataset save/load
        test_dataset = Dataset(
            id="test_dataset",
            name="Test Dataset",
            description="Test dataset for unit testing"
        )
        
        if test_dataset.save_to_neo4j():
            logger.info("✅ Dataset saved to Neo4j")
            
            # Test loading
            loaded_dataset = Dataset.load_from_neo4j("test_dataset")
            if loaded_dataset and loaded_dataset.name == "Test Dataset":
                logger.info("✅ Dataset loaded from Neo4j")
            else:
                logger.error("❌ Dataset loading failed")
        else:
            logger.error("❌ Dataset save failed")
        
        # Test repository save/load
        test_repo = Repository(
            url="https://github.com/test/test",
            owner="test",
            name="test",
            description="Test repository",
            framework=Framework.PYTORCH
        )
        
        if test_repo.save_to_neo4j():
            logger.info("✅ Repository saved to Neo4j")
            
            loaded_repo = Repository.load_from_neo4j("https://github.com/test/test")
            if loaded_repo and loaded_repo.name == "test":
                logger.info("✅ Repository loaded from Neo4j")
            else:
                logger.error("❌ Repository loading failed")
        else:
            logger.error("❌ Repository save failed")
        
        # Test paper with relationships
        test_paper = Paper(
            id="test_paper",
            title="Test Paper",
            abstract="A test paper",
            authors=[Author(name="Test Author")],
            repositories=[test_repo],
            datasets=["test_dataset"]
        )
        
        if test_paper.save_to_neo4j():
            logger.info("✅ Paper with relationships saved to Neo4j")
            
            loaded_paper = Paper.load_from_neo4j("test_paper")
            if loaded_paper and loaded_paper.title == "Test Paper":
                logger.info("✅ Paper loaded from Neo4j")
                logger.info(f"   Authors: {len(loaded_paper.authors)}")
                logger.info(f"   Repositories: {len(loaded_paper.repositories)}")
                logger.info(f"   Datasets: {len(loaded_paper.datasets)}")
            else:
                logger.error("❌ Paper loading failed")
        else:
            logger.error("❌ Paper save failed")
        
        # Test graph statistics
        stats = graph.get_graph_stats()
        logger.info(f"Graph stats: {stats}")
        
        graph.close()
        logger.info("✅ Neo4j integration test completed")
        
    except Exception as e:
        logger.error(f"❌ Neo4j integration test failed: {e}")
        logger.info("Make sure Neo4j is running on bolt://localhost:7687")

def test_api_loader():
    """Test the Papers with Code API loader"""
    logger.info("Testing Papers with Code API loader...")
    
    try:
        loader = PapersWithCodeLoader()
        
        # Test loading small amounts of data
        datasets = loader.load_datasets(limit=5)
        logger.info(f"✅ Loaded {len(datasets)} datasets from API")
        
        tasks = loader.load_tasks(limit=5)
        logger.info(f"✅ Loaded {len(tasks)} tasks from API")
        
        repositories = loader.load_repositories(limit=5)
        logger.info(f"✅ Loaded {len(repositories)} repositories from API")
        
        papers = loader.load_papers(limit=3)  # Small limit as papers are slower
        logger.info(f"✅ Loaded {len(papers)} papers from API")
        
        if papers:
            logger.info(f"Sample paper: {papers[0].title}")
            logger.info(f"  Authors: {len(papers[0].authors)}")
            logger.info(f"  Repositories: {len(papers[0].repositories)}")
        
        logger.info(f"API loader stats: {loader.stats}")
        
    except Exception as e:
        logger.error(f"❌ API loader test failed: {e}")

def run_all_tests():
    """Run all tests"""
    logger.info("🚀 Starting Papers with Code Knowledge Graph Tests")
    
    test_pydantic_models()
    test_api_loader()
    
    # Neo4j test is optional (requires running Neo4j)
    try:
        test_neo4j_integration()
    except Exception as e:
        logger.warning(f"Neo4j test skipped (Neo4j may not be running): {e}")
    
    logger.info("🎉 All tests completed!")

if __name__ == "__main__":
    run_all_tests()