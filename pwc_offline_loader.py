#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Set, Any
from datetime import datetime
import time

# Optional tqdm import with fallback
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Simple fallback for tqdm
    class tqdm:
        def __init__(self, iterable, desc="Processing", unit="item", leave=True, **kwargs):
            self.iterable = iterable
            self.desc = desc
            self.leave = leave
            self.total = len(iterable) if hasattr(iterable, '__len__') else None
            self.current = 0
            
        def __iter__(self):
            for i, item in enumerate(self.iterable):
                self.current = i
                # Only print for main progress bars (leave=True) to avoid spam
                if self.leave and (i % 10 == 0 or i == 0):
                    if self.total:
                        print(f"{self.desc}: {i+1}/{self.total} ({(i+1)/self.total*100:.1f}%)")
                    else:
                        print(f"{self.desc}: {i+1} items processed")
                yield item
                
        def set_postfix(self, **kwargs):
            # For nested progress bars, show completion info
            if not self.leave and kwargs:
                info = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                print(f"    {self.desc}: {info}")
            
        def close(self):
            if self.leave and self.total:
                print(f"{self.desc}: Completed {self.total}/{self.total} (100.0%)")
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
            'errors': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0
        }
        
        logger.info(f"📁 Using offline data from: {self.data_dir}")
    
    def clear_cache(self):
        """Clear all cached data to force reload"""
        self._papers_cache = None
        self._links_cache = None
        self._datasets_cache = None
        self._methods_cache = None
        self._evaluations_cache = None
        logger.info("🗑️ Cleared data cache")
    
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
            
            # Safely get title with fallback
            title = paper_data.get('title')
            if not title:  # Handle None, empty string, etc.
                title = "Untitled Paper"
            
            # Use arxiv_id as the primary identifier if available, otherwise fallback to other IDs
            paper_id = (paper_data.get('arxiv_id') or 
                       paper_data.get('id') or 
                       paper_data.get('paper_id') or 
                       str(hash(str(title))))
            
            # Skip papers with completely missing critical data
            if not paper_id or paper_id == str(hash("")):
                logger.debug(f"Skipping paper with missing ID and title")
                return None
            
            # Safely handle other fields
            url_abs = paper_data.get('url_abs') or paper_data.get('url')
            
            # Create paper object with safe defaults
            paper = Paper(
                id=str(paper_id),
                arxiv_id=paper_data.get('arxiv_id'),
                url_abs=url_abs,
                title=title,
                abstract=paper_data.get('abstract'),
                authors=authors,
                published=published,
                venue=paper_data.get('proceeding') or paper_data.get('venue'),
                citation_count=paper_data.get('citation_count', 0) if paper_data.get('citation_count') is not None else 0
            )
            
            return paper
            
        except Exception as e:
            # Log more detailed error information for debugging
            paper_title = paper_data.get('title', 'Unknown')
            paper_id = paper_data.get('id', 'Unknown')
            logger.error(f"Error parsing paper '{paper_title}' (ID: {paper_id}): {e}")
            logger.debug(f"Paper data causing error: {paper_data}")
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
                    framework = Repository.parse_framework(framework_str)
            
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
            repo_url = repo_data.get('url', 'Unknown')
            logger.error(f"Error parsing repository '{repo_url}': {e}")
            logger.debug(f"Repository data causing error: {repo_data}")
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
            dataset_name = dataset_data.get('name', 'Unknown')
            logger.error(f"Error parsing dataset '{dataset_name}': {e}")
            logger.debug(f"Dataset data causing error: {dataset_data}")
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
        
        # Process papers with progress bar
        logger.info(f"🔄 Processing {len(papers_data)} papers...")
        paper_build_progress = tqdm(papers_data, desc="Building papers", unit="paper")
        for paper_data in paper_build_progress:
            paper = self.parse_paper(paper_data)
            if paper:
                # Add repositories from mapping
                if include_repositories and paper.id in repo_mapping:
                    paper.repositories = repo_mapping[paper.id]
                
                papers.append(paper)
                self.stats['papers_processed'] += 1
                
                # Update progress bar with current paper title
                if paper.title:
                    paper_build_progress.set_postfix({"current": paper.title[:30]})
        
        paper_build_progress.close()
        logger.info(f"✅ Built {len(papers)} papers from offline data")
        return papers
    
    def build_datasets(self, limit: Optional[int] = None) -> List[Dataset]:
        """Build Dataset objects from offline data"""
        logger.info("🗄️ Building datasets from offline data...")
        
        # Convert 0 to None (no limit) for consistent handling
        actual_limit = None if limit == 0 else limit
        
        datasets = []
        datasets_data = self.load_datasets_json(actual_limit)
        
        # Process datasets with progress bar
        if datasets_data:
            logger.info(f"🔄 Processing {len(datasets_data)} datasets...")
            dataset_build_progress = tqdm(datasets_data, desc="Building datasets", unit="dataset")
            for dataset_data in dataset_build_progress:
                dataset = self.parse_dataset(dataset_data)
                if dataset:
                    datasets.append(dataset)
                    self.stats['datasets_processed'] += 1
                    # Update progress bar with current dataset name
                    if dataset.name:
                        dataset_build_progress.set_postfix({"current": dataset.name[:30]})
            dataset_build_progress.close()
        
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
    
    def rebuild_models_from_data(self) -> Dict[str, Any]:
        """Rebuild Pydantic models from existing data without database dependency"""
        logger.info("🔨 Rebuilding Pydantic models from offline data...")
        
        rebuilt_models = {
            'papers': [],
            'datasets': [],
            'repositories': [],
            'stats': {
                'papers_rebuilt': 0,
                'datasets_rebuilt': 0,
                'repositories_rebuilt': 0,
                'errors': 0
            }
        }
        
        # Rebuild datasets
        try:
            datasets_data = self.load_datasets_json()
            for dataset_data in datasets_data:
                dataset = self.parse_dataset(dataset_data)
                if dataset:
                    rebuilt_models['datasets'].append(dataset)
                    rebuilt_models['stats']['datasets_rebuilt'] += 1
                else:
                    rebuilt_models['stats']['errors'] += 1
        except Exception as e:
            logger.error(f"Error rebuilding datasets: {e}")
        
        # Rebuild repositories
        try:
            links_data = self.load_links()
            seen_urls = set()
            for link_data in links_data:
                repo = self.parse_repository(link_data)
                if repo and repo.url not in seen_urls:
                    rebuilt_models['repositories'].append(repo)
                    rebuilt_models['stats']['repositories_rebuilt'] += 1
                    seen_urls.add(repo.url)
                elif not repo:
                    rebuilt_models['stats']['errors'] += 1
        except Exception as e:
            logger.error(f"Error rebuilding repositories: {e}")
        
        # Rebuild papers with repositories
        try:
            papers_data = self.load_papers()
            repo_mapping = self.build_paper_repository_mapping()
            
            for paper_data in papers_data:
                paper = self.parse_paper(paper_data)
                if paper:
                    # Add repositories from mapping
                    if paper.id in repo_mapping:
                        paper.repositories = repo_mapping[paper.id]
                    rebuilt_models['papers'].append(paper)
                    rebuilt_models['stats']['papers_rebuilt'] += 1
                else:
                    rebuilt_models['stats']['errors'] += 1
        except Exception as e:
            logger.error(f"Error rebuilding papers: {e}")
        
        logger.info(f"✅ Model rebuilding complete: {rebuilt_models['stats']}")
        return rebuilt_models
    
    def load_to_new_neo4j_instance(self,
                                   new_neo4j_uri: str,
                                   new_neo4j_user: str = "neo4j",
                                   new_neo4j_password: str = "password",
                                   paper_limit: Optional[int] = 0,
                                   dataset_limit: Optional[int] = 0,
                                   include_repositories: bool = True,
                                   clear_target: bool = False,
                                   drop_all_target: bool = False) -> Dict:
        """Load data directly to a new Neo4j instance with rebuilt models"""
        logger.info(f"🔄 Loading data to new Neo4j instance: {new_neo4j_uri}")
        
        try:
            # Create new graph connection
            from models import PapersWithCodeGraph
            new_graph = PapersWithCodeGraph(new_neo4j_uri, new_neo4j_user, new_neo4j_password)
            
            # Test connection
            new_graph.get_graph_stats()
            logger.info("✅ New Neo4j connection successful")
            
            # Handle dangerous drop-all-target first
            if drop_all_target:
                if confirm_dangerous_operation("DROP ALL TARGET DATA", new_neo4j_uri):
                    logger.info("💥 Dropping ALL data from target Neo4j instance...")
                    new_graph.clear_all_data()
                    logger.info("✅ Target database completely cleared")
                else:
                    logger.info("❌ Operation cancelled by user")
                    new_graph.close()
                    return {}
            # Clear target if requested (only PWC data types)
            elif clear_target:
                logger.info("🗑️ Selectively clearing Papers with Code data from target Neo4j instance...")
                cleared_data = new_graph.clear_pwc_data_only()
                logger.info(f"✅ Cleared PWC data: {cleared_data}")
                # Also clear PWC-specific indexes
                new_graph.clear_pwc_indexes_only()
            
            # Load data using the existing pipeline but with new graph
            stats = self.load_and_save_to_neo4j(
                graph=new_graph,
                paper_limit=paper_limit,
                dataset_limit=dataset_limit,
                include_repositories=include_repositories,
                rebuild_models=True,
                force_reload=True
            )
            
            new_graph.close()
            logger.info(f"✅ Data successfully loaded to new Neo4j instance")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to load to new Neo4j instance: {e}")
            raise
    
    def load_and_save_to_neo4j(self, 
                               graph: PapersWithCodeGraph,
                               paper_limit: Optional[int] = 0,
                               dataset_limit: Optional[int] = 0,
                               include_repositories: bool = True,
                               skip_datasets: bool = False,
                               force_reload: bool = False,
                               skip_if_exists: bool = False,
                               clear_all: bool = False,
                               rebuild_models: bool = False,
                               drop_all: bool = False) -> Dict:
        """Load offline data and save to Neo4j knowledge graph"""
        # Start timing
        self.stats['start_time'] = datetime.now()
        start_time = time.time()
        
        logger.info("🚀 Starting offline data pipeline to Neo4j")
        logger.info(f"⏰ Started at: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Handle dangerous drop-all flag first
        if drop_all:
            if confirm_dangerous_operation("DROP ALL LOCAL DATA", "Local Neo4j database"):
                logger.info("💥 Dropping ALL data from local Neo4j instance...")
                graph.clear_all_data()
                logger.info("✅ Local database completely cleared")
                force_reload = True  # Automatically enable force-reload
            else:
                logger.info("❌ Operation cancelled by user")
                return {}
        # Handle clear-all flag (implies force-reload)
        elif clear_all:
            logger.info("💥 Clear all Papers with Code data enabled - clearing PWC data types only")
            cleared_data = graph.clear_pwc_data_only()
            logger.info(f"✅ PWC data cleared: {cleared_data}")
            # Also clear PWC-specific indexes
            graph.clear_pwc_indexes_only()
            force_reload = True  # Automatically enable force-reload
        
        # Check existing data for smart resumption
        if not force_reload and not clear_all and not drop_all:
            logger.info("🔍 Checking existing data in Neo4j...")
            existing_data = graph.check_existing_data()
            logger.info(f"📊 Found existing data: {existing_data}")
            
            # Smart skip logic
            if existing_data['datasets'] > 0 and not skip_datasets:
                logger.info(f"✅ Found {existing_data['datasets']} datasets already loaded - skipping dataset phase")
                skip_datasets = True
            
            if existing_data['papers'] > 0:
                logger.info(f"⚠️  Found {existing_data['papers']} papers already loaded")
                if skip_if_exists:
                    logger.info("⏭️  Skipping all loading due to --skip-if-exists flag")
                    return self.stats
                elif not force_reload:
                    logger.info("💡 To continue loading more papers, use --force-reload or --skip-datasets")
                    logger.info("📋 Continuing with paper loading (appending to existing data)...")
        elif force_reload:  # Only if force_reload is explicitly set (not from clear_all)
            logger.info("🔄 Force reload enabled - will clear and reload specific data")
            
            # Clear existing data based on what we're loading (only if not already cleared by clear_all or drop_all)
            if not clear_all and not drop_all:
                if not skip_datasets:
                    # Clear datasets if we're going to load them
                    graph.clear_datasets_only()
                
                # Always clear papers when force reloading (since we're likely loading papers)
                graph.clear_papers_only()
        
        # Convert 0 to None (no limit) for consistent handling
        actual_paper_limit = None if paper_limit == 0 else paper_limit
        actual_dataset_limit = None if dataset_limit == 0 else dataset_limit
        
        # Phase 1: Load datasets (if not skipping)
        phase1_duration = 0
        if not skip_datasets:
            logger.info("Phase 1: Loading datasets")
            phase1_start = time.time()
            datasets = self.build_datasets(actual_dataset_limit)
            
            # Save datasets with batch processing for speed
            if datasets:
                logger.info(f"💾 Saving {len(datasets)} datasets to Neo4j using batch processing...")
                
                batch_size = 50
                total_batches = (len(datasets) + batch_size - 1) // batch_size
                
                dataset_progress = tqdm(
                    range(total_batches),
                    desc="Saving dataset batches", 
                    unit="batch",
                    mininterval=0.1
                )
                
                total_saved = 0
                total_failed = 0
                
                for batch_idx in dataset_progress:
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(datasets))
                    batch = datasets[start_idx:end_idx]
                    
                    dataset_progress.set_postfix({
                        "batch": f"{batch_idx+1}/{total_batches}",
                        "items": f"{start_idx}-{end_idx-1}",
                        "saved": total_saved,
                        "failed": total_failed
                    })
                    
                    # Process dataset batch with nested progress bar
                    batch_progress = tqdm(
                        range(1), 
                        desc=f"  Saving dataset batch {batch_idx+1}", 
                        unit="batch",
                        leave=False,
                        mininterval=0.1
                    )
                    
                    for _ in batch_progress:
                        batch_progress.set_postfix({
                            "datasets": f"{len(batch)}",
                            "processing": "..."
                        })
                        
                        result = graph.batch_save_datasets(batch, batch_size)
                        total_saved += result["saved"]
                        total_failed += result["failed"]
                        
                        batch_progress.set_postfix({
                            "datasets": f"{len(batch)}",
                            "saved": result["saved"],
                            "failed": result["failed"]
                        })
                    
                    batch_progress.close()
                
                dataset_progress.close()
                logger.info(f"📊 Dataset batch saving summary: {total_saved} saved, {total_failed} failed")
            
            phase1_duration = time.time() - phase1_start
            logger.info(f"✅ Phase 1 complete: {phase1_duration:.2f} seconds")
        else:
            logger.info("⏭️  Phase 1: Skipping datasets (already loaded or --skip-datasets flag)")
            phase1_start = time.time()
        
        # Load papers with repositories
        logger.info("Phase 2: Loading papers with code repositories")
        phase2_start = time.time()
        papers = self.build_papers_with_code(actual_paper_limit, include_repositories)
        
        # Save papers with batch processing for much faster performance
        if papers:
            logger.info(f"💾 Saving {len(papers)} papers to Neo4j using batch processing...")
            
            batch_size = 100  # Larger batches for papers
            total_batches = (len(papers) + batch_size - 1) // batch_size
            
            # Enhanced progress bar for batch processing
            paper_progress = tqdm(
                range(total_batches),
                desc="Saving paper batches", 
                unit="batch",
                mininterval=0.5,
                maxinterval=2.0
            )
            
            total_saved = 0
            total_failed = 0
            
            for batch_idx in paper_progress:
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(papers))
                batch = papers[start_idx:end_idx]
                
                # Calculate current rates
                elapsed = time.time() - phase2_start if batch_idx > 0 else 1
                current_papers_processed = min(end_idx, len(papers))
                rate = current_papers_processed / elapsed
                
                # Calculate ETA
                remaining_papers = len(papers) - current_papers_processed
                eta_seconds = remaining_papers / rate if rate > 0 else 0
                eta_hours = eta_seconds / 3600
                
                # Update progress bar with detailed info
                paper_progress.set_postfix({
                    "batch": f"{batch_idx+1}/{total_batches}",
                    "papers": f"{start_idx}-{end_idx-1}",
                    "saved": total_saved,
                    "failed": total_failed,
                    "rate": f"{rate:.1f}/s",
                    "ETA": f"{eta_hours:.1f}h"
                })
                
                # Process batch with nested progress bar
                batch_progress = tqdm(
                    range(1), 
                    desc=f"  Saving batch {batch_idx+1}", 
                    unit="batch",
                    leave=False,  # Don't leave the nested progress bar
                    mininterval=0.1
                )
                
                for _ in batch_progress:
                    batch_progress.set_postfix({
                        "papers": f"{len(batch)}",
                        "processing": "..."
                    })
                    
                    result = graph.batch_save_papers(batch, batch_size)
                    total_saved += result["saved"]
                    total_failed += result["failed"]
                    
                    batch_progress.set_postfix({
                        "papers": f"{len(batch)}",
                        "saved": result["saved"], 
                        "failed": result["failed"]
                    })
                
                batch_progress.close()
                
                # Detailed logging every 10 batches for very large datasets
                if (batch_idx + 1) % 10 == 0:
                    progress_pct = (current_papers_processed / len(papers)) * 100
                    logger.info(f"📊 Batch Progress: {current_papers_processed}/{len(papers)} papers ({progress_pct:.1f}%) - Rate: {rate:.1f} papers/s - ETA: {eta_hours:.1f}h")
            
            paper_progress.close()
            logger.info(f"📊 Paper batch saving summary: {total_saved} saved, {total_failed} failed")
        
        phase2_duration = time.time() - phase2_start
        logger.info(f"✅ Phase 2 complete: {phase2_duration:.2f} seconds")
        
        # End timing
        end_time = time.time()
        self.stats['end_time'] = datetime.now()
        self.stats['duration_seconds'] = end_time - start_time
        
        # Calculate and display timing information
        duration = self.stats['end_time'] - self.stats['start_time']
        hours, remainder = divmod(self.stats['duration_seconds'], 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Final statistics with timing
        logger.info("📊 Pipeline complete!")
        logger.info(f"⏰ Finished at: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏱️  Total duration: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} ({self.stats['duration_seconds']:.2f} seconds)")
        logger.info(f"📈 Processing rate: {self.stats['papers_processed'] / max(self.stats['duration_seconds'], 1):.1f} papers/second")
        logger.info(f"📊 Statistics: {self.stats}")
        
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
                # Check if file exists without loading (to avoid caching limited data)
                filename_mapping = {
                    'papers': 'papers-with-abstracts.json',
                    'links': 'links-between-papers-and-code.json', 
                    'datasets': 'datasets.json',
                    'methods': 'methods.json',
                    'evaluations': 'evaluation-tables.json'
                }
                
                file_path = self.data_dir / filename_mapping[dataset_key]
                if file_path.exists():
                    summary['files_available'][dataset_key] = True
                    # Load just 1 record for sample without caching
                    if hasattr(self, 'downloader'):
                        sample_records = self.downloader.load_json_file(dataset_key, 1)
                        if sample_records:
                            summary['sample_records'][dataset_key] = sample_records[0]
                else:
                    summary['files_available'][dataset_key] = False
            except Exception as e:
                summary['files_available'][dataset_key] = False
                logger.warning(f"Could not check {dataset_key}: {e}")
        
        return summary

def confirm_dangerous_operation(operation_type: str, target_description: str) -> bool:
    """Request user confirmation for dangerous operations that delete all data"""
    print("\n" + "="*80)
    print("🚨 DANGER: DESTRUCTIVE OPERATION WARNING 🚨")
    print("="*80)
    print(f"Operation: {operation_type}")
    print(f"Target: {target_description}")
    print("This will DELETE ALL DATA in the specified Neo4j database!")
    print("This action is IRREVERSIBLE and will destroy:")
    print("  • All nodes of every type")
    print("  • All relationships")
    print("  • All indexes")
    print("  • All constraints")
    print("  • Everything in the database")
    print("="*80)
    
    # Require explicit confirmation
    confirmation_text = "DELETE ALL DATA"
    print(f"To confirm this destructive operation, type exactly: {confirmation_text}")
    user_input = input("Confirmation: ").strip()
    
    if user_input == confirmation_text:
        print("⚠️  Confirmation received. Proceeding with destructive operation...")
        return True
    else:
        print("❌ Confirmation not received. Operation cancelled.")
        print("Database preserved - no changes made.")
        return False

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
    parser.add_argument(
        "--skip-datasets",
        action="store_true",
        help="Skip dataset loading and go directly to papers"
    )
    parser.add_argument(
        "--force-reload",
        action="store_true", 
        help="Force reload all data even if already exists in Neo4j"
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip loading if data already exists in Neo4j"
    )
    parser.add_argument(
        "--clear-all",
        action="store_true",
        help="Clear Papers with Code data from local Neo4j instance before loading (preserves other data types, implies --force-reload)"
    )
    parser.add_argument(
        "--new-neo4j-uri",
        help="Load data to a different Neo4j instance (alternative to default URI)"
    )
    parser.add_argument(
        "--new-neo4j-user",
        default="neo4j",
        help="Username for new Neo4j instance (default: neo4j)"
    )
    parser.add_argument(
        "--new-neo4j-password",
        default="password",
        help="Password for new Neo4j instance (default: password)"
    )
    parser.add_argument(
        "--clear-target",
        action="store_true",
        help="Clear Papers with Code data from target Neo4j instance before loading (preserves other data types)"
    )
    parser.add_argument(
        "--rebuild-models",
        action="store_true",
        help="Rebuild Pydantic models from data before loading"
    )
    parser.add_argument(
        "--drop-all",
        action="store_true",
        help="⚠️  DANGER: Drop ALL data from local Neo4j database (requires confirmation)"
    )
    parser.add_argument(
        "--drop-all-target",
        action="store_true",
        help="⚠️  DANGER: Drop ALL data from target Neo4j database (requires confirmation, use with --new-neo4j-uri)"
    )
    
    args = parser.parse_args()
    
    # Validate conflicting arguments
    if args.drop_all and args.clear_all:
        logger.error("❌ Cannot use both --drop-all and --clear-all flags")
        logger.error("   --drop-all: Destroys ALL data in database")
        logger.error("   --clear-all: Only clears Papers with Code data")
        logger.error("   Choose one based on your needs")
        return
    
    if args.drop_all_target and args.clear_target:
        logger.error("❌ Cannot use both --drop-all-target and --clear-target flags")
        logger.error("   --drop-all-target: Destroys ALL data in target database")
        logger.error("   --clear-target: Only clears Papers with Code data from target")
        logger.error("   Choose one based on your needs")
        return
    
    if args.drop_all_target and not args.new_neo4j_uri:
        logger.error("❌ --drop-all-target requires --new-neo4j-uri")
        logger.error("   You must specify a target database to drop all data from")
        return
    
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
        
        # Check if we're loading to a new Neo4j instance
        if args.new_neo4j_uri:
            logger.info(f"🔄 Loading to new Neo4j instance: {args.new_neo4j_uri}")
            logger.info(f"New Neo4j User: {args.new_neo4j_user}")
            
            # Use the new loading method
            stats = loader.load_to_new_neo4j_instance(
                new_neo4j_uri=args.new_neo4j_uri,
                new_neo4j_user=args.new_neo4j_user,
                new_neo4j_password=args.new_neo4j_password,
                paper_limit=args.paper_limit,
                dataset_limit=args.dataset_limit,
                include_repositories=include_repositories,
                clear_target=args.clear_target,
                drop_all_target=args.drop_all_target
            )
        else:
            # Initialize knowledge graph (original behavior)
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
            
            # Load data with specified limits and flags
            logger.info(f"Loading with limits - Papers: {args.paper_limit}, Datasets: {args.dataset_limit}, Include Repos: {include_repositories}")
            logger.info(f"Flags - Skip Datasets: {args.skip_datasets}, Force Reload: {args.force_reload}, Skip If Exists: {args.skip_if_exists}, Clear All: {args.clear_all}, Drop All: {args.drop_all}, Rebuild Models: {args.rebuild_models}")
            
            stats = loader.load_and_save_to_neo4j(
                graph=graph,
                paper_limit=args.paper_limit,
                dataset_limit=args.dataset_limit,
                include_repositories=include_repositories,
                skip_datasets=args.skip_datasets,
                force_reload=args.force_reload,
                skip_if_exists=args.skip_if_exists,
                clear_all=args.clear_all,
                rebuild_models=args.rebuild_models,
                drop_all=args.drop_all
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