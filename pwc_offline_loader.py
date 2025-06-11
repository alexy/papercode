#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime
from models import (
    Paper, Repository, Dataset, Task, Author, PapersWithCodeGraph, Framework
)
from pwc_dataset_downloader import PapersWithCodeDatasetDownloader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PapersWithCodeOfflineLoader:
    """Loader that uses downloaded Papers with Code JSON datasets instead of API"""
    
    def __init__(self, data_dir: str):
        """Initialize with directory containing downloaded JSON files"""
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {data_dir}")
        
        self.downloader = PapersWithCodeDatasetDownloader(str(self.data_dir.parent))
        self.downloader.download_dir = self.data_dir
        
        # Caches for loaded data
        self._papers_cache = None
        self._links_cache = None
        self._datasets_cache = None
        self._methods_cache = None
        self._evaluations_cache = None
        
        # Statistics
        self.stats = {
            'papers_processed': 0,
            'repositories_processed': 0,
            'datasets_processed': 0,
            'methods_processed': 0,
            'links_processed': 0,
            'errors': 0
        }
        
        logger.info(f"📁 Using offline data from: {self.data_dir}")
    
    def load_papers(self, limit: Optional[int] = None) -> List[Dict]:
        """Load papers from JSON file"""
        if self._papers_cache is None:
            logger.info("📖 Loading papers dataset...")
            self._papers_cache = self.downloader.load_json_file('papers', limit)
            if self._papers_cache is None:
                self._papers_cache = []
        
        return self._papers_cache
    
    def load_links(self, limit: Optional[int] = None) -> List[Dict]:
        """Load paper-code links from JSON file"""
        if self._links_cache is None:
            logger.info("📖 Loading paper-code links dataset...")
            self._links_cache = self.downloader.load_json_file('links', limit)
            if self._links_cache is None:
                self._links_cache = []
        
        return self._links_cache
    
    def load_datasets_json(self, limit: Optional[int] = None) -> List[Dict]:
        """Load datasets from JSON file"""
        if self._datasets_cache is None:
            logger.info("📖 Loading datasets dataset...")
            self._datasets_cache = self.downloader.load_json_file('datasets', limit)
            if self._datasets_cache is None:
                self._datasets_cache = []
        
        return self._datasets_cache
    
    def load_methods(self, limit: Optional[int] = None) -> List[Dict]:
        """Load methods from JSON file"""
        if self._methods_cache is None:
            logger.info("📖 Loading methods dataset...")
            self._methods_cache = self.downloader.load_json_file('methods', limit)
            if self._methods_cache is None:
                self._methods_cache = []
        
        return self._methods_cache
    
    def load_evaluations(self, limit: Optional[int] = None) -> List[Dict]:
        """Load evaluation tables from JSON file"""
        if self._evaluations_cache is None:
            logger.info("📖 Loading evaluation tables dataset...")
            self._evaluations_cache = self.downloader.load_json_file('evaluations', limit)
            if self._evaluations_cache is None:
                self._evaluations_cache = []
        
        return self._evaluations_cache
    
    def parse_paper(self, paper_data: Dict) -> Optional[Paper]:
        """Parse a paper from JSON data"""
        try:
            # Parse authors
            authors = []
            if paper_data.get('authors'):
                if isinstance(paper_data['authors'], list):
                    for author_name in paper_data['authors']:
                        if isinstance(author_name, str):
                            authors.append(Author(name=author_name))
                        elif isinstance(author_name, dict) and author_name.get('name'):
                            authors.append(Author(
                                name=author_name['name'],
                                email=author_name.get('email'),
                                affiliation=author_name.get('affiliation')
                            ))
            
            # Parse publication date
            published = None
            if paper_data.get('date'):
                try:
                    published = datetime.fromisoformat(paper_data['date'].replace('Z', '+00:00'))
                except:
                    pass
            
            # Create paper object
            paper = Paper(
                id=str(paper_data.get('id', paper_data.get('paper_id', ''))),
                arxiv_id=paper_data.get('arxiv_id'),
                url_abs=paper_data.get('url_abs') or paper_data.get('url'),
                title=paper_data.get('title', ''),
                abstract=paper_data.get('abstract'),
                authors=authors,
                published=published,
                venue=paper_data.get('proceeding') or paper_data.get('venue'),
                citation_count=paper_data.get('citation_count', 0)
            )
            
            return paper
            
        except Exception as e:
            logger.error(f"Error parsing paper: {e}")
            self.stats['errors'] += 1
            return None
    
    def parse_repository(self, repo_data: Dict) -> Optional[Repository]:
        """Parse a repository from JSON data"""
        try:
            # Handle different possible formats
            url = repo_data.get('url') or repo_data.get('repo_url') or repo_data.get('github_url')
            if not url:
                return None
            
            # Extract owner and name from URL
            owner = ""
            name = ""
            if 'github.com' in url:
                parts = url.rstrip('/').split('/')
                if len(parts) >= 2:
                    owner = parts[-2]
                    name = parts[-1]
            
            # Parse framework
            framework = None
            framework_str = repo_data.get('framework') or repo_data.get('ml_framework')
            if framework_str:
                try:
                    framework = Framework(framework_str.lower())
                except ValueError:
                    # Use the validator to parse framework
                    framework = Repository.__fields__['framework'].type_.parse_framework(None, framework_str)
            
            repository = Repository(
                url=url,
                owner=owner,
                name=name,
                description=repo_data.get('description'),
                stars=repo_data.get('stars', 0),
                framework=framework,
                language=repo_data.get('language'),
                license=repo_data.get('license')
            )
            
            return repository
            
        except Exception as e:
            logger.error(f"Error parsing repository: {e}")
            self.stats['errors'] += 1
            return None
    
    def parse_dataset(self, dataset_data: Dict) -> Optional[Dataset]:
        """Parse a dataset from JSON data"""
        try:
            dataset = Dataset(
                id=str(dataset_data.get('id', dataset_data.get('name', ''))),
                name=dataset_data.get('name', ''),
                full_name=dataset_data.get('full_name'),
                url=dataset_data.get('url'),
                description=dataset_data.get('description'),
                paper_count=dataset_data.get('paper_count', 0)
            )
            
            return dataset
            
        except Exception as e:
            logger.error(f"Error parsing dataset: {e}")
            self.stats['errors'] += 1
            return None
    
    def build_paper_repository_mapping(self) -> Dict[str, List[Repository]]:
        """Build mapping from paper IDs to repositories using links dataset"""
        logger.info("🔗 Building paper-repository mapping...")
        
        mapping = {}
        links_data = self.load_links()
        
        for link in links_data:
            try:
                paper_id = str(link.get('paper_id', ''))
                if not paper_id:
                    continue
                
                # Parse repository from link data
                repo = self.parse_repository(link)
                if repo:
                    if paper_id not in mapping:
                        mapping[paper_id] = []
                    mapping[paper_id].append(repo)
                    self.stats['links_processed'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing link: {e}")
                self.stats['errors'] += 1
        
        logger.info(f"✅ Built mapping for {len(mapping)} papers with {self.stats['links_processed']} repository links")
        return mapping
    
    def build_papers_with_code(self, 
                               paper_limit: Optional[int] = None,
                               include_repositories: bool = True) -> List[Paper]:
        """Build Paper objects with code repositories from offline data"""
        logger.info("🏗️ Building papers with code from offline data...")
        
        papers = []
        
        # Load papers data
        papers_data = self.load_papers(paper_limit)
        if not papers_data:
            logger.error("No papers data loaded")
            return []
        
        # Build repository mapping if needed
        repo_mapping = {}
        if include_repositories:
            repo_mapping = self.build_paper_repository_mapping()
        
        # Process papers
        for paper_data in papers_data:
            paper = self.parse_paper(paper_data)
            if paper:
                # Add repositories from mapping
                if include_repositories and paper.id in repo_mapping:
                    paper.repositories = repo_mapping[paper.id]
                
                papers.append(paper)
                self.stats['papers_processed'] += 1
        
        logger.info(f"✅ Built {len(papers)} papers from offline data")
        return papers
    
    def build_datasets(self, limit: Optional[int] = None) -> List[Dataset]:
        """Build Dataset objects from offline data"""
        logger.info("🗄️ Building datasets from offline data...")
        
        datasets = []
        datasets_data = self.load_datasets_json(limit)
        
        for dataset_data in datasets_data:
            dataset = self.parse_dataset(dataset_data)
            if dataset:
                datasets.append(dataset)
                self.stats['datasets_processed'] += 1
        
        logger.info(f"✅ Built {len(datasets)} datasets from offline data")
        return datasets
    
    def build_repositories(self, limit: Optional[int] = None) -> List[Repository]:
        """Build Repository objects from links data"""
        logger.info("💾 Building repositories from offline data...")
        
        repositories = []
        seen_urls = set()
        
        links_data = self.load_links(limit)
        
        for link_data in links_data:
            repo = self.parse_repository(link_data)
            if repo and repo.url not in seen_urls:
                repositories.append(repo)
                seen_urls.add(repo.url)
                self.stats['repositories_processed'] += 1
        
        logger.info(f"✅ Built {len(repositories)} unique repositories from offline data")
        return repositories
    
    def load_and_save_to_neo4j(self, 
                               graph: PapersWithCodeGraph,
                               paper_limit: Optional[int] = 100,
                               dataset_limit: Optional[int] = 50,
                               include_repositories: bool = True) -> Dict:
        """Load offline data and save to Neo4j knowledge graph"""
        logger.info("🚀 Starting offline data pipeline to Neo4j")
        
        # Load datasets first
        logger.info("Phase 1: Loading datasets")
        datasets = self.build_datasets(dataset_limit)
        for dataset in datasets:
            if dataset.save_to_neo4j():
                logger.debug(f"Saved dataset: {dataset.name}")
            else:
                logger.error(f"Failed to save dataset: {dataset.name}")
        
        # Load papers with repositories
        logger.info("Phase 2: Loading papers with code repositories")
        papers = self.build_papers_with_code(paper_limit, include_repositories)
        
        for i, paper in enumerate(papers, 1):
            if paper.save_to_neo4j():
                if i % 10 == 0:
                    logger.info(f"Saved {i}/{len(papers)} papers...")
            else:
                logger.error(f"Failed to save paper: {paper.title[:50]}...")
        
        # Final statistics
        logger.info("📊 Pipeline complete!")
        logger.info(f"Statistics: {self.stats}")
        
        # Get Neo4j graph statistics
        neo4j_stats = graph.get_graph_stats()
        logger.info(f"Neo4j Graph Statistics: {neo4j_stats}")
        
        return {**self.stats, **neo4j_stats}
    
    def get_data_summary(self) -> Dict:
        """Get summary of available offline data"""
        summary = {
            'data_dir': str(self.data_dir),
            'files_available': {},
            'sample_records': {}
        }
        
        # Check each dataset
        for dataset_key in ['papers', 'links', 'datasets', 'methods', 'evaluations']:
            try:
                # Try to load a few records
                records = getattr(self, f'load_{dataset_key}' if dataset_key != 'datasets' else 'load_datasets_json')(5)
                summary['files_available'][dataset_key] = len(records) > 0
                if records:
                    summary['sample_records'][dataset_key] = records[0]
            except Exception as e:
                summary['files_available'][dataset_key] = False
                logger.warning(f"Could not load {dataset_key}: {e}")
        
        return summary

def main():
    """Main function to run offline data loading pipeline"""
    import sys
    
    # Check for data directory argument
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    else:
        # Look for most recent download directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        if pwc_dirs:
            data_dir = str(sorted(pwc_dirs)[-1])  # Most recent
            logger.info(f"Using most recent data directory: {data_dir}")
        else:
            logger.error("No PWC data directory found. Run pwc_dataset_downloader.py first.")
            return
    
    try:
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(data_dir)
        
        # Show data summary
        summary = loader.get_data_summary()
        logger.info(f"📋 Data Summary:")
        for dataset_key, available in summary['files_available'].items():
            status = "✅" if available else "❌"
            logger.info(f"   {status} {dataset_key}")
        
        # Neo4j configuration
        NEO4J_URI = "bolt://localhost:7687"
        NEO4J_USERNAME = "neo4j"
        NEO4J_PASSWORD = "password"  # Change this
        
        # Initialize knowledge graph
        logger.info("Initializing Papers with Code Knowledge Graph")
        graph = PapersWithCodeGraph(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
        
        # Load data with limits (adjust as needed)
        stats = loader.load_and_save_to_neo4j(
            graph=graph,
            paper_limit=50,    # Start small for testing
            dataset_limit=30,
            include_repositories=True
        )
        
        logger.info("Pipeline completed successfully!")
        
        # Example queries
        logger.info("\n--- Example Queries ---")
        
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