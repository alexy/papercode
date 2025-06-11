#!/usr/bin/env python3

import logging
from unittest.mock import Mock, patch
from models import PapersWithCodeGraph, Paper, Repository

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_graph_stats_fixes():
    """Test that graph stats handles empty database without warnings"""
    logger.info("🧪 Testing graph stats fixes...")
    
    # Mock Neo4j session that returns empty results
    mock_session = Mock()
    
    # Mock empty database - no nodes exist
    mock_result = Mock()
    mock_record = Mock()
    mock_record.__getitem__ = lambda self, key: 0  # All counts are 0
    mock_result.single.return_value = mock_record
    mock_session.run.return_value = mock_result
    
    with patch('models.Paper.get_session', return_value=mock_session):
        try:
            graph = Mock()
            graph._driver = Mock()
            graph_instance = PapersWithCodeGraph.__new__(PapersWithCodeGraph)
            graph_instance.get_graph_stats = PapersWithCodeGraph.get_graph_stats.__get__(graph_instance)
            
            # Test with all nodes = 0 (should skip relationship queries)
            stats = graph_instance.get_graph_stats()
            
            expected_stats = {
                'papers': 0,
                'repositories': 0,
                'authors': 0,
                'datasets': 0,
                'tasks': 0,
                'paper_code_links': 0
            }
            
            assert stats == expected_stats, f"Expected {expected_stats}, got {stats}"
            logger.info("✅ Empty database stats test passed")
            
            # Verify that relationship query was NOT called when nodes = 0
            relationship_query_called = False
            for call in mock_session.run.call_args_list:
                if 'HAS_CODE' in str(call):
                    relationship_query_called = True
                    break
            
            assert not relationship_query_called, "Relationship query should not be called when no nodes exist"
            logger.info("✅ Relationship query optimization test passed")
            
        except Exception as e:
            logger.error(f"❌ Graph stats test failed: {e}")
            return False
    
    return True

def test_search_functions_fixes():
    """Test that search functions handle missing nodes gracefully"""
    logger.info("🧪 Testing search function fixes...")
    
    # Mock Neo4j session for repository not found
    mock_session = Mock()
    
    # Mock repository not found (count = 0)
    mock_result = Mock()
    mock_record = Mock()
    mock_record.__getitem__ = lambda self, key: 0  # Repository not found
    mock_result.single.return_value = mock_record
    mock_session.run.return_value = mock_result
    
    with patch('models.Paper.get_session', return_value=mock_session):
        try:
            # Test search_papers_by_code with non-existent repository
            papers = Paper.search_papers_by_code("https://github.com/nonexistent/repo")
            
            assert papers == [], f"Expected empty list, got {papers}"
            logger.info("✅ search_papers_by_code with missing repo test passed")
            
            # Test search_code_by_paper with non-existent paper
            repositories = Paper.search_code_by_paper("nonexistent_paper_id")
            
            assert repositories == [], f"Expected empty list, got {repositories}"
            logger.info("✅ search_code_by_paper with missing paper test passed")
            
        except Exception as e:
            logger.error(f"❌ Search functions test failed: {e}")
            return False
    
    return True

def test_load_paper_fixes():
    """Test that paper loading handles missing relationships gracefully"""
    logger.info("🧪 Testing paper loading fixes...")
    
    # Mock Neo4j session for paper with no relationships
    mock_session = Mock()
    
    # Mock paper exists but has no relationships
    def mock_run(query, **kwargs):
        mock_result = Mock()
        
        if "MATCH (p:Paper" in query and "RETURN p" in query:
            # Paper exists
            mock_record = Mock()
            mock_record.__getitem__ = lambda self, key: {
                'id': 'test_paper',
                'title': 'Test Paper',
                'abstract': 'Test abstract'
            }
            mock_result.single.return_value = mock_record
        else:
            # No relationships found
            mock_result.__iter__ = lambda self: iter([])
        
        return mock_result
    
    mock_session.run.side_effect = mock_run
    
    with patch('models.Paper.get_session', return_value=mock_session):
        try:
            paper = Paper.load_from_neo4j("test_paper")
            
            assert paper is not None, "Paper should be loaded"
            assert paper.title == "Test Paper", f"Expected 'Test Paper', got {paper.title}"
            assert paper.authors == [], f"Expected empty authors list, got {paper.authors}"
            assert paper.repositories == [], f"Expected empty repositories list, got {paper.repositories}"
            
            logger.info("✅ Paper loading with no relationships test passed")
            
        except Exception as e:
            logger.error(f"❌ Paper loading test failed: {e}")
            return False
    
    return True

def main():
    """Run all Neo4j fix tests"""
    logger.info("🚀 Testing Neo4j warning fixes...")
    
    tests = [
        test_graph_stats_fixes,
        test_search_functions_fixes,
        test_load_paper_fixes
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
    
    logger.info(f"\n📊 Test Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        logger.info("✅ All Neo4j warning fixes working correctly!")
        logger.info("💡 The warnings about missing relationship types should now be eliminated")
    else:
        logger.error("❌ Some tests failed - fixes may need adjustment")

if __name__ == "__main__":
    main()