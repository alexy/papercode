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
            # Convert 0 to None (no limit) for the downloader
            actual_limit = None if limit == 0 else limit
            self._papers_cache = self.downloader.load_json_file('papers', actual_limit)
            if self._papers_cache is None:
                self._papers_cache = []
        
        return self._papers_cache
    
    def load_links(self, limit: Optional[int] = None) -> List[Dict]:
        """Load paper-code links from JSON file"""
        if self._links_cache is None:
            logger.info("📖 Loading paper-code links dataset...")
            # Convert 0 to None (no limit) for the downloader
            actual_limit = None if limit == 0 else limit
            self._links_cache = self.downloader.load_json_file('links', actual_limit)
            if self._links_cache is None:
                self._links_cache = []
        
        return self._links_cache
    
    def load_datasets_json(self, limit: Optional[int] = None) -> List[Dict]:
        """Load datasets from JSON file"""
        if self._datasets_cache is None:
            logger.info("📖 Loading datasets dataset...")
            # Convert 0 to None (no limit) for the downloader
            actual_limit = None if limit == 0 else limit
            self._datasets_cache = self.downloader.load_json_file('datasets', actual_limit)
            if self._datasets_cache is None:
                self._datasets_cache = []
        
        return self._datasets_cache
    
    def load_methods(self, limit: Optional[int] = None) -> List[Dict]:
        """Load methods from JSON file"""
        if self._methods_cache is None:
            logger.info("📖 Loading methods dataset...")
            # Convert 0 to None (no limit) for the downloader
            actual_limit = None if limit == 0 else limit
            self._methods_cache = self.downloader.load_json_file('methods', actual_limit)
            if self._methods_cache is None:
                self._methods_cache = []
        
        return self._methods_cache
    
    def load_evaluations(self, limit: Optional[int] = None) -> List[Dict]:
        """Load evaluation tables from JSON file"""
        if self._evaluations_cache is None:
            logger.info("📖 Loading evaluation tables dataset...")
            # Convert 0 to None (no limit) for the downloader
            actual_limit = None if limit == 0 else limit
            self._evaluations_cache = self.downloader.load_json_file('evaluations', actual_limit)
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
            
            # Use arxiv_id as the primary identifier if available, otherwise fallback to other IDs
            paper_id = (paper_data.get('arxiv_id') or 
                       paper_data.get('id') or 
                       paper_data.get('paper_id') or 
                       str(hash(paper_data.get('title', ''))))
            
            # Create paper object
            paper = Paper(
                id=str(paper_id),
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
                # Try different possible paper ID fields
                paper_id = str(link.get('paper_id', '') or 
                              link.get('paper_arxiv_id', '') or 
                              link.get('arxiv_id', ''))
                
                if not paper_id:
                    # Skip if no paper identifier found
                    continue
                
                # Parse repository from link data
                repo_data = {
                    'url': link.get('repo_url'),
                    'framework': link.get('framework'),
                    'description': None,
                    'stars': 0,
                    'language': None,
                    'license': None
                }
                
                repo = self.parse_repository(repo_data)
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
        
        # Convert 0 to None (no limit) for consistent handling
        actual_limit = None if paper_limit == 0 else paper_limit
        
        # Load papers data
        papers_data = self.load_papers(actual_limit)
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
        
        # Convert 0 to None (no limit) for consistent handling
        actual_limit = None if limit == 0 else limit
        
        datasets = []
        datasets_data = self.load_datasets_json(actual_limit)
        
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
        
        # Convert 0 to None (no limit) for consistent handling
        actual_limit = None if limit == 0 else limit
        
        repositories = []
        seen_urls = set()
        
        links_data = self.load_links(actual_limit)
        
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
                               paper_limit: Optional[int] = 0,
                               dataset_limit: Optional[int] = 0,
                               include_repositories: bool = True) -> Dict:
        """Load offline data and save to Neo4j knowledge graph"""
        logger.info("🚀 Starting offline data pipeline to Neo4j")
        
        # Convert 0 to None (no limit) for consistent handling
        actual_paper_limit = None if paper_limit == 0 else paper_limit
        actual_dataset_limit = None if dataset_limit == 0 else dataset_limit
        
        # Load datasets first
        logger.info("Phase 1: Loading datasets")
        datasets = self.build_datasets(actual_dataset_limit)
        for dataset in datasets:
            if dataset.save_to_neo4j():
                logger.debug(f"Saved dataset: {dataset.name}")
            else:
                logger.error(f"Failed to save dataset: {dataset.name}")
        
        # Load papers with repositories
        logger.info("Phase 2: Loading papers with code repositories")
        papers = self.build_papers_with_code(actual_paper_limit, include_repositories)
        
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
        try:
            neo4j_stats = graph.get_graph_stats()
            logger.info(f"Neo4j Graph Statistics: {neo4j_stats}")
            return {**self.stats, **neo4j_stats}
        except Exception as e:
            logger.error(f"Failed to get Neo4j stats: {e}")
            return self.stats
    
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
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Load Papers with Code offline data into Neo4j knowledge graph"
    )
    parser.add_argument(
        "data_dir", 
        nargs="?",
        help="Path to PWC data directory (optional - will auto-detect most recent if not provided)"
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
        default=0,
        help="Maximum number of papers to load (default: 0 = no limit)"
    )
    parser.add_argument(
        "--dataset-limit", 
        type=int, 
        default=0,
        help="Maximum number of datasets to load (default: 0 = no limit)"
    )
    parser.add_argument(
        "--include-repositories", 
        action="store_true", 
        default=True,
        help="Include repository links (default: True)"
    )
    parser.add_argument(
        "--no-repositories", 
        action="store_true",
        help="Exclude repository links"
    )
    
    args = parser.parse_args()
    
    # Determine data directory
    if args.data_dir:
        data_dir = args.data_dir
    else:
        # Look for most recent download directory
        base_dir = Path(".")
        pwc_dirs = list(base_dir.glob("pwc-*"))
        if pwc_dirs:
            data_dir = str(sorted(pwc_dirs)[-1])  # Most recent
            logger.info(f"Using most recent data directory: {data_dir}")
        else:
            logger.error("No PWC data directory found. Run pwc_dataset_downloader.py first.")
            logger.info("Usage: python pwc_offline_loader.py [data_dir] [options]")
            return
    
    # Handle repository inclusion flag
    include_repositories = args.include_repositories and not args.no_repositories
    
    try:
        # Initialize offline loader
        loader = PapersWithCodeOfflineLoader(data_dir)
        
        # Show data summary
        summary = loader.get_data_summary()
        logger.info(f"📋 Data Summary:")
        for dataset_key, available in summary['files_available'].items():
            status = "✅" if available else "❌"
            logger.info(f"   {status} {dataset_key}")
        
        # Initialize knowledge graph
        logger.info("Initializing Papers with Code Knowledge Graph")
        logger.info(f"Neo4j URI: {args.neo4j_uri}")
        logger.info(f"Neo4j User: {args.neo4j_user}")
        
        try:
            graph = PapersWithCodeGraph(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
            # Test connection
            graph.get_graph_stats()
            logger.info("✅ Neo4j connection successful")
        except Exception as e:
            logger.error(f"❌ Neo4j connection failed: {e}")
            logger.info("💡 Troubleshooting tips:")
            logger.info("   1. Make sure Neo4j is running (check http://localhost:7474)")
            logger.info("   2. Verify your password with: python test_neo4j_connection.py")
            logger.info("   3. For Docker: docker run -p 7474:7474 -p 7687:7687 neo4j:latest")
            logger.info("   4. Use --neo4j-password YOUR_ACTUAL_PASSWORD")
            return
        
        # Load data with specified limits
        logger.info(f"Loading with limits - Papers: {args.paper_limit}, Datasets: {args.dataset_limit}, Include Repos: {include_repositories}")
        
        stats = loader.load_and_save_to_neo4j(
            graph=graph,
            paper_limit=args.paper_limit,
            dataset_limit=args.dataset_limit,
            include_repositories=include_repositories
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