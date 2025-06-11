#!/usr/bin/env python3

import requests
import json
import time
from typing import List, Dict, Optional, Any
from datetime import datetime
from pathlib import Path
import logging
from models import (
    Paper, Repository, Dataset, Task, Author, PapersWithCodeGraph, Framework
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PapersWithCodeLoader:
    """Loader class to fetch data from Papers with Code API and load into Neo4j"""
    
    def __init__(self, base_url: str = "https://paperswithcode.com/api/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PapersWithCode-Loader/1.0',
            'Accept': 'application/json'
        })
        
        # Rate limiting
        self.request_delay = 1.0  # seconds between requests
        self.last_request_time = 0
        
        # Stats
        self.stats = {
            'papers_loaded': 0,
            'repos_loaded': 0,
            'datasets_loaded': 0,
            'tasks_loaded': 0,
            'errors': 0
        }
    
    def _rate_limit(self):
        """Implement rate limiting between API calls"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make rate-limited request to Papers with Code API"""
        self._rate_limit()
        
        try:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            response = self.session.get(url, params=params or {})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            self.stats['errors'] += 1
            return None
    
    def load_datasets(self, limit: int = 100) -> List[Dataset]:
        """Load datasets from Papers with Code API"""
        logger.info(f"Loading datasets (limit: {limit})")
        datasets = []
        
        params = {'page': 1}
        while len(datasets) < limit:
            data = self._make_request('/datasets/', params)
            if not data or 'results' not in data:
                break
            
            for item in data['results']:
                if len(datasets) >= limit:
                    break
                
                try:
                    dataset = Dataset(
                        id=str(item.get('id', item.get('name', ''))),
                        name=item.get('name', ''),
                        full_name=item.get('full_name'),
                        url=item.get('url'),
                        description=item.get('description'),
                        paper_count=item.get('paper_count', 0)
                    )
                    datasets.append(dataset)
                    self.stats['datasets_loaded'] += 1
                    
                except Exception as e:
                    logger.error(f"Error parsing dataset: {e}")
                    self.stats['errors'] += 1
            
            # Check if there are more pages
            if not data.get('next'):
                break
            params['page'] += 1
        
        logger.info(f"Loaded {len(datasets)} datasets")
        return datasets
    
    def load_tasks(self, limit: int = 100) -> List[Task]:
        """Load tasks from Papers with Code API"""
        logger.info(f"Loading tasks (limit: {limit})")
        tasks = []
        
        params = {'page': 1}
        while len(tasks) < limit:
            data = self._make_request('/tasks/', params)
            if not data or 'results' not in data:
                break
            
            for item in data['results']:
                if len(tasks) >= limit:
                    break
                
                try:
                    task = Task(
                        id=str(item.get('id', item.get('name', ''))),
                        name=item.get('name', ''),
                        description=item.get('description'),
                        area=item.get('area')
                    )
                    tasks.append(task)
                    self.stats['tasks_loaded'] += 1
                    
                except Exception as e:
                    logger.error(f"Error parsing task: {e}")
                    self.stats['errors'] += 1
            
            if not data.get('next'):
                break
            params['page'] += 1
        
        logger.info(f"Loaded {len(tasks)} tasks")
        return tasks
    
    def load_repositories(self, limit: int = 100) -> List[Repository]:
        """Load repositories from Papers with Code API"""
        logger.info(f"Loading repositories (limit: {limit})")
        repositories = []
        
        params = {'page': 1}
        while len(repositories) < limit:
            data = self._make_request('/repositories/', params)
            if not data or 'results' not in data:
                break
            
            for item in data['results']:
                if len(repositories) >= limit:
                    break
                
                try:
                    # Parse framework
                    framework = None
                    if item.get('framework'):
                        framework = Framework(item['framework'])
                    
                    repository = Repository(
                        url=item.get('url', ''),
                        owner=item.get('owner', ''),
                        name=item.get('name', ''),
                        description=item.get('description'),
                        stars=item.get('stars', 0),
                        framework=framework,
                        language=item.get('language'),
                        license=item.get('license')
                    )
                    repositories.append(repository)
                    self.stats['repos_loaded'] += 1
                    
                except Exception as e:
                    logger.error(f"Error parsing repository: {e}")
                    self.stats['errors'] += 1
            
            if not data.get('next'):
                break
            params['page'] += 1
        
        logger.info(f"Loaded {len(repositories)} repositories")
        return repositories
    
    def load_papers(self, limit: int = 100) -> List[Paper]:
        """Load papers from Papers with Code API with relationships"""
        logger.info(f"Loading papers (limit: {limit})")
        papers = []
        
        params = {'page': 1}
        while len(papers) < limit:
            data = self._make_request('/papers/', params)
            if not data or 'results' not in data:
                break
            
            for item in data['results']:
                if len(papers) >= limit:
                    break
                
                try:
                    # Parse authors
                    authors = []
                    if item.get('authors'):
                        for author_name in item['authors']:
                            authors.append(Author(name=author_name))
                    
                    # Parse publication date
                    published = None
                    if item.get('published'):
                        try:
                            published = datetime.fromisoformat(item['published'].replace('Z', '+00:00'))
                        except:
                            pass
                    
                    paper = Paper(
                        id=item.get('id', ''),
                        arxiv_id=item.get('arxiv_id'),
                        url_abs=item.get('url_abs'),
                        title=item.get('title', ''),
                        abstract=item.get('abstract'),
                        authors=authors,
                        published=published,
                        venue=item.get('venue'),
                        citation_count=item.get('citation_count', 0)
                    )
                    
                    # Load related repositories for this paper
                    if item.get('id'):
                        paper.repositories = self.load_paper_repositories(item['id'])
                    
                    papers.append(paper)
                    self.stats['papers_loaded'] += 1
                    
                except Exception as e:
                    logger.error(f"Error parsing paper: {e}")
                    self.stats['errors'] += 1
            
            if not data.get('next'):
                break
            params['page'] += 1
        
        logger.info(f"Loaded {len(papers)} papers")
        return papers
    
    def load_paper_repositories(self, paper_id: str) -> List[Repository]:
        """Load repositories associated with a specific paper"""
        data = self._make_request(f'/papers/{paper_id}/repositories/')
        if not data or 'results' not in data:
            return []
        
        repositories = []
        for item in data['results']:
            try:
                framework = None
                if item.get('framework'):
                    framework = Framework(item['framework'])
                
                repository = Repository(
                    url=item.get('url', ''),
                    owner=item.get('owner', ''),
                    name=item.get('name', ''),
                    description=item.get('description'),
                    stars=item.get('stars', 0),
                    framework=framework,
                    language=item.get('language'),
                    license=item.get('license')
                )
                repositories.append(repository)
                
            except Exception as e:
                logger.error(f"Error parsing paper repository: {e}")
        
        return repositories
    
    def load_and_save_to_neo4j(self, 
                               graph: PapersWithCodeGraph,
                               paper_limit: int = 50,
                               repo_limit: int = 100,
                               dataset_limit: int = 50,
                               task_limit: int = 50):
        """Load data from API and save to Neo4j knowledge graph"""
        logger.info("Starting full data pipeline from Papers with Code to Neo4j")
        
        # Load datasets first (referenced by papers)
        logger.info("Phase 1: Loading datasets")
        datasets = self.load_datasets(dataset_limit)
        for dataset in datasets:
            if dataset.save_to_neo4j():
                logger.info(f"Saved dataset: {dataset.name}")
            else:
                logger.error(f"Failed to save dataset: {dataset.name}")
        
        # Load tasks
        logger.info("Phase 2: Loading tasks")
        tasks = self.load_tasks(task_limit)
        for task in tasks:
            if task.save_to_neo4j():
                logger.info(f"Saved task: {task.name}")
            else:
                logger.error(f"Failed to save task: {task.name}")
        
        # Load repositories
        logger.info("Phase 3: Loading repositories")
        repositories = self.load_repositories(repo_limit)
        for repo in repositories:
            if repo.save_to_neo4j():
                logger.info(f"Saved repository: {repo.owner}/{repo.name}")
            else:
                logger.error(f"Failed to save repository: {repo.owner}/{repo.name}")
        
        # Load papers with relationships
        logger.info("Phase 4: Loading papers with relationships")
        papers = self.load_papers(paper_limit)
        for paper in papers:
            if paper.save_to_neo4j():
                logger.info(f"Saved paper: {paper.title[:50]}...")
            else:
                logger.error(f"Failed to save paper: {paper.title[:50]}...")
        
        # Print final statistics
        logger.info("Data loading complete!")
        logger.info(f"Statistics: {self.stats}")
        
        # Get Neo4j graph statistics
        neo4j_stats = graph.get_graph_stats()
        logger.info(f"Neo4j Graph Statistics: {neo4j_stats}")
        
        return self.stats

def main():
    """Main function to run the data loading pipeline"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Load Papers with Code data via API into Neo4j knowledge graph"
    )
    parser.add_argument(
        "--neo4j-uri", 
        default="bolt://localhost:7687",
        help="Neo4j database URI (default: bolt://localhost:7687)"
    )
    parser.add_argument(
        "--neo4j-user", 
        default="neo4j",
        help="Neo4j username (default: neo4j)"
    )
    parser.add_argument(
        "--neo4j-password", 
        default="password",
        help="Neo4j password (default: password)"
    )
    parser.add_argument(
        "--paper-limit", 
        type=int, 
        default=20,
        help="Maximum number of papers to load (default: 20)"
    )
    parser.add_argument(
        "--repo-limit", 
        type=int, 
        default=50,
        help="Maximum number of repositories to load (default: 50)"
    )
    parser.add_argument(
        "--dataset-limit", 
        type=int, 
        default=30,
        help="Maximum number of datasets to load (default: 30)"
    )
    parser.add_argument(
        "--task-limit", 
        type=int, 
        default=30,
        help="Maximum number of tasks to load (default: 30)"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize knowledge graph
        logger.info("Initializing Papers with Code Knowledge Graph")
        logger.info(f"Neo4j URI: {args.neo4j_uri}")
        logger.info(f"Neo4j User: {args.neo4j_user}")
        
        graph = PapersWithCodeGraph(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
        
        # Initialize loader
        loader = PapersWithCodeLoader()
        
        # Load data with specified limits
        logger.info(f"Loading with limits - Papers: {args.paper_limit}, Repos: {args.repo_limit}, Datasets: {args.dataset_limit}, Tasks: {args.task_limit}")
        
        stats = loader.load_and_save_to_neo4j(
            graph=graph,
            paper_limit=args.paper_limit,
            repo_limit=args.repo_limit,
            dataset_limit=args.dataset_limit,
            task_limit=args.task_limit
        )
        
        logger.info("Pipeline completed successfully!")
        
        # Example queries
        logger.info("\n--- Example Queries ---")
        
        # Find papers by specific repository
        example_repo_url = "https://github.com/pytorch/pytorch"
        papers_with_pytorch = Paper.search_papers_by_code(example_repo_url)
        logger.info(f"Papers using PyTorch: {len(papers_with_pytorch)}")
        
        # Get graph statistics
        final_stats = graph.get_graph_stats()
        logger.info(f"Final Graph Statistics: {final_stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        # Clean up
        if 'graph' in locals():
            graph.close()

if __name__ == "__main__":
    main()