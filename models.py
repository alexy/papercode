#!/usr/bin/env python3

from pydantic import BaseModel, Field, HttpUrl, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
import json
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Framework(str, Enum):
    """ML/AI frameworks"""
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    TF = "tf"
    KERAS = "keras"
    JAX = "jax"
    SKLEARN = "sklearn"
    HUGGINGFACE = "huggingface"
    NONE = "none"
    OTHER = "other"

class Neo4jMixin:
    """Mixin class to provide Neo4j database operations"""
    
    @classmethod
    def init_neo4j(cls, uri: str, username: str, password: str):
        """Initialize Neo4j connection"""
        cls._driver = GraphDatabase.driver(uri, auth=(username, password))
        cls._session = None
        
    @classmethod
    def get_session(cls):
        """Get or create Neo4j session"""
        if not hasattr(cls, '_driver'):
            raise ValueError("Neo4j not initialized. Call init_neo4j() first.")
        if cls._session is None or cls._session.closed():
            cls._session = cls._driver.session()
        return cls._session
    
    @classmethod
    def close_connection(cls):
        """Close Neo4j connection"""
        if hasattr(cls, '_session') and cls._session:
            cls._session.close()
        if hasattr(cls, '_driver') and cls._driver:
            cls._driver.close()

class Author(BaseModel):
    """Author model"""
    name: str
    email: Optional[str] = None
    affiliation: Optional[str] = None
    
    class Config:
        extra = "allow"

class Dataset(BaseModel, Neo4jMixin):
    """Dataset model with Neo4j integration"""
    id: str
    name: str
    full_name: Optional[str] = None
    url: Optional[str] = None  # Changed from HttpUrl to allow empty strings
    description: Optional[str] = None
    paper_count: Optional[int] = 0
    
    @validator('url', pre=True)
    def validate_url(cls, v):
        if not v or v.strip() == '':
            return None
        return v
    
    @validator('name', pre=True)
    def validate_name(cls, v):
        """Ensure name is never None or empty"""
        if not v:
            return "Unknown Dataset"
        return str(v).strip()
    
    @validator('paper_count', pre=True)
    def validate_paper_count(cls, v):
        """Ensure paper_count is always a valid integer"""
        if v is None or v == "":
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0
    
    def save_to_neo4j(self) -> bool:
        """Save dataset to Neo4j"""
        try:
            session = self.get_session()
            query = """
            MERGE (d:Dataset {id: $id})
            SET d.name = $name,
                d.full_name = $full_name,
                d.url = $url,
                d.description = $description,
                d.paper_count = $paper_count,
                d.updated_at = datetime()
            RETURN d
            """
            result = session.run(query, **self.dict())
            return result.single() is not None
        except Exception as e:
            logger.error(f"Error saving dataset to Neo4j: {e}")
            return False
    
    @classmethod
    def load_from_neo4j(cls, dataset_id: str) -> Optional['Dataset']:
        """Load dataset from Neo4j"""
        try:
            session = cls.get_session()
            query = """
            MATCH (d:Dataset {id: $id})
            RETURN d
            """
            result = session.run(query, id=dataset_id)
            record = result.single()
            if record:
                return cls(**dict(record['d']))
            return None
        except Exception as e:
            logger.error(f"Error loading dataset from Neo4j: {e}")
            return None

class Task(BaseModel, Neo4jMixin):
    """Research task model with Neo4j integration"""
    id: str
    name: str
    description: Optional[str] = None
    area: Optional[str] = None
    
    def save_to_neo4j(self) -> bool:
        """Save task to Neo4j"""
        try:
            session = self.get_session()
            query = """
            MERGE (t:Task {id: $id})
            SET t.name = $name,
                t.description = $description,
                t.area = $area,
                t.updated_at = datetime()
            RETURN t
            """
            result = session.run(query, **self.dict())
            return result.single() is not None
        except Exception as e:
            logger.error(f"Error saving task to Neo4j: {e}")
            return False
    
    @classmethod
    def load_from_neo4j(cls, task_id: str) -> Optional['Task']:
        """Load task from Neo4j"""
        try:
            session = cls.get_session()
            query = """
            MATCH (t:Task {id: $id})
            RETURN t
            """
            result = session.run(query, id=task_id)
            record = result.single()
            if record:
                return cls(**dict(record['t']))
            return None
        except Exception as e:
            logger.error(f"Error loading task from Neo4j: {e}")
            return None

class Repository(BaseModel, Neo4jMixin):
    """Code repository model with Neo4j integration"""
    url: HttpUrl
    owner: str
    name: str
    description: Optional[str] = None
    stars: Optional[int] = 0
    framework: Optional[Framework] = None
    language: Optional[str] = None
    license: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def parse_framework(cls, v):
        """Class method to parse framework from string"""
        if isinstance(v, str):
            v_lower = v.lower().strip()
            # Direct match first
            try:
                return Framework(v_lower)
            except ValueError:
                # Try to match common framework names
                if 'pytorch' in v_lower or 'torch' in v_lower:
                    return Framework.PYTORCH
                elif 'tensorflow' in v_lower or v_lower == 'tf':
                    return Framework.TF
                elif 'keras' in v_lower:
                    return Framework.KERAS
                elif 'jax' in v_lower:
                    return Framework.JAX
                elif 'sklearn' in v_lower or 'scikit' in v_lower:
                    return Framework.SKLEARN
                elif 'hugging' in v_lower or 'transformers' in v_lower:
                    return Framework.HUGGINGFACE
                elif v_lower in ['none', '', 'null']:
                    return Framework.NONE
                else:
                    return Framework.OTHER
        return v if v else Framework.NONE
    
    @validator('framework', pre=True)
    def validate_framework(cls, v):
        return cls.parse_framework(v)
    
    @validator('stars', pre=True)
    def validate_stars(cls, v):
        """Ensure stars is always a valid integer"""
        if v is None or v == "":
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0
    
    @validator('name', pre=True)
    def validate_name(cls, v):
        """Ensure name is never None"""
        if not v:
            return "unknown"
        return str(v)
    
    @validator('owner', pre=True) 
    def validate_owner(cls, v):
        """Ensure owner is never None"""
        if not v:
            return "unknown"
        return str(v)
    
    def save_to_neo4j(self) -> bool:
        """Save repository to Neo4j"""
        try:
            session = self.get_session()
            query = """
            MERGE (r:Repository {url: $url})
            SET r.owner = $owner,
                r.name = $name,
                r.description = $description,
                r.stars = $stars,
                r.framework = $framework,
                r.language = $language,
                r.license = $license,
                r.created_at = $created_at,
                r.updated_at = datetime()
            RETURN r
            """
            data = self.dict()
            # Convert datetime objects to strings for Neo4j
            if data.get('created_at'):
                data['created_at'] = data['created_at'].isoformat()
            if data.get('framework'):
                data['framework'] = data['framework'].value if hasattr(data['framework'], 'value') else str(data['framework'])
            # Convert HttpUrl to string for Neo4j
            if data.get('url'):
                data['url'] = str(data['url'])
            
            result = session.run(query, **data)
            return result.single() is not None
        except Exception as e:
            logger.error(f"Error saving repository to Neo4j: {e}")
            return False
    
    @classmethod
    def load_from_neo4j(cls, repo_url: str) -> Optional['Repository']:
        """Load repository from Neo4j"""
        try:
            session = cls.get_session()
            query = """
            MATCH (r:Repository {url: $url})
            RETURN r
            """
            result = session.run(query, url=repo_url)
            record = result.single()
            if record:
                data = dict(record['r'])
                # Convert string dates back to datetime
                if data.get('created_at'):
                    data['created_at'] = datetime.fromisoformat(data['created_at'])
                return cls(**data)
            return None
        except Exception as e:
            logger.error(f"Error loading repository from Neo4j: {e}")
            return None

class Paper(BaseModel, Neo4jMixin):
    """Research paper model with Neo4j integration"""
    id: str
    arxiv_id: Optional[str] = None
    url_abs: Optional[HttpUrl] = None
    title: Optional[str] = "Untitled Paper"  # Allow None, provide default
    abstract: Optional[str] = None
    authors: List[Author] = []
    published: Optional[datetime] = None
    venue: Optional[str] = None
    citation_count: Optional[int] = 0
    
    @validator('title', pre=True)
    def validate_title(cls, v):
        """Ensure title is never None or empty"""
        if v is None or v == "":
            return "Untitled Paper"
        return str(v).strip()
    
    @validator('citation_count', pre=True)
    def validate_citation_count(cls, v):
        """Ensure citation_count is always a valid integer"""
        if v is None or v == "":
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0
    
    @validator('id', pre=True)
    def validate_id(cls, v):
        """Ensure id is never None or empty"""
        if not v:
            import uuid
            return str(uuid.uuid4())
        return str(v)
    
    # Relationships
    repositories: List[Repository] = []
    datasets: List[str] = []  # Dataset IDs
    tasks: List[str] = []  # Task IDs
    
    def save_to_neo4j(self) -> bool:
        """Save paper to Neo4j with all relationships"""
        try:
            session = self.get_session()
            
            # Save paper node
            paper_query = """
            MERGE (p:Paper {id: $id})
            SET p.arxiv_id = $arxiv_id,
                p.url_abs = $url_abs,
                p.title = $title,
                p.abstract = $abstract,
                p.published = $published,
                p.venue = $venue,
                p.citation_count = $citation_count,
                p.updated_at = datetime()
            RETURN p
            """
            
            paper_data = self.dict(exclude={'authors', 'repositories', 'datasets', 'tasks'})
            if paper_data.get('published'):
                paper_data['published'] = paper_data['published'].isoformat()
            # Convert HttpUrl to string for Neo4j
            if paper_data.get('url_abs'):
                paper_data['url_abs'] = str(paper_data['url_abs'])
            
            result = session.run(paper_query, **paper_data)
            if not result.single():
                return False
            
            # Save authors and relationships
            for author in self.authors:
                author_query = """
                MERGE (a:Author {name: $name})
                SET a.email = $email,
                    a.affiliation = $affiliation
                WITH a
                MATCH (p:Paper {id: $paper_id})
                MERGE (a)-[:AUTHORED]->(p)
                """
                session.run(author_query, **author.dict(), paper_id=self.id)
            
            # Save repository relationships
            for repo in self.repositories:
                repo.save_to_neo4j()  # Ensure repository exists
                repo_rel_query = """
                MATCH (p:Paper {id: $paper_id})
                MATCH (r:Repository {url: $repo_url})
                MERGE (p)-[:HAS_CODE]->(r)
                """
                session.run(repo_rel_query, paper_id=self.id, repo_url=str(repo.url))
            
            # Save dataset relationships
            for dataset_id in self.datasets:
                dataset_rel_query = """
                MATCH (p:Paper {id: $paper_id})
                MATCH (d:Dataset {id: $dataset_id})
                MERGE (p)-[:USES_DATASET]->(d)
                """
                session.run(dataset_rel_query, paper_id=self.id, dataset_id=dataset_id)
            
            # Save task relationships
            for task_id in self.tasks:
                task_rel_query = """
                MATCH (p:Paper {id: $paper_id})
                MATCH (t:Task {id: $task_id})
                MERGE (p)-[:ADDRESSES_TASK]->(t)
                """
                session.run(task_rel_query, paper_id=self.id, task_id=task_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving paper to Neo4j: {e}")
            return False
    
    @classmethod
    def load_from_neo4j(cls, paper_id: str) -> Optional['Paper']:
        """Load paper from Neo4j with all relationships"""
        try:
            session = cls.get_session()
            
            # First load just the paper
            paper_query = "MATCH (p:Paper {id: $id}) RETURN p"
            result = session.run(paper_query, id=paper_id)
            record = result.single()
            
            if not record:
                return None
            
            paper_data = dict(record['p'])
            
            # Convert datetime
            if paper_data.get('published'):
                paper_data['published'] = datetime.fromisoformat(paper_data['published'])
            
            # Initialize relationship lists
            paper_data['authors'] = []
            paper_data['repositories'] = []
            paper_data['datasets'] = []
            paper_data['tasks'] = []
            
            # Load authors
            try:
                author_query = "MATCH (a:Author)-[:AUTHORED]->(p:Paper {id: $id}) RETURN a"
                result = session.run(author_query, id=paper_id)
                paper_data['authors'] = [Author(**dict(record['a'])) for record in result if record['a']]
            except Exception:
                pass  # No authors or relationship doesn't exist
            
            # Load repositories
            try:
                repo_query = "MATCH (p:Paper {id: $id})-[:HAS_CODE]->(r:Repository) RETURN r"
                result = session.run(repo_query, id=paper_id)
                paper_data['repositories'] = [Repository(**dict(record['r'])) for record in result if record['r']]
            except Exception:
                pass  # No repositories or relationship doesn't exist
            
            # Load datasets
            try:
                dataset_query = "MATCH (p:Paper {id: $id})-[:USES_DATASET]->(d:Dataset) RETURN d.id as id"
                result = session.run(dataset_query, id=paper_id)
                paper_data['datasets'] = [record['id'] for record in result if record['id']]
            except Exception:
                pass  # No datasets or relationship doesn't exist
            
            # Load tasks
            try:
                task_query = "MATCH (p:Paper {id: $id})-[:ADDRESSES_TASK]->(t:Task) RETURN t.id as id"
                result = session.run(task_query, id=paper_id)
                paper_data['tasks'] = [record['id'] for record in result if record['id']]
            except Exception:
                pass  # No tasks or relationship doesn't exist
            
            return cls(**paper_data)
            
        except Exception as e:
            logger.error(f"Error loading paper from Neo4j: {e}")
            return None
    
    @classmethod
    def search_papers_by_code(cls, repo_url: str) -> List['Paper']:
        """Find all papers that use a specific code repository"""
        try:
            session = cls.get_session()
            
            # First check if the repository exists
            repo_check = session.run("MATCH (r:Repository {url: $repo_url}) RETURN count(r) as count", repo_url=repo_url)
            repo_record = repo_check.single()
            
            if not repo_record or repo_record['count'] == 0:
                logger.info(f"Repository not found in database: {repo_url}")
                return []
            
            query = """
            MATCH (p:Paper)-[:HAS_CODE]->(r:Repository {url: $repo_url})
            RETURN p.id as paper_id
            """
            result = session.run(query, repo_url=repo_url)
            
            papers = []
            for record in result:
                paper = cls.load_from_neo4j(record['paper_id'])
                if paper:
                    papers.append(paper)
            
            return papers
            
        except Exception as e:
            logger.error(f"Error searching papers by code: {e}")
            return []
    
    @classmethod
    def search_code_by_paper(cls, paper_id: str) -> List[Repository]:
        """Find all code repositories associated with a paper"""
        try:
            session = cls.get_session()
            
            # First check if the paper exists
            paper_check = session.run("MATCH (p:Paper {id: $paper_id}) RETURN count(p) as count", paper_id=paper_id)
            paper_record = paper_check.single()
            
            if not paper_record or paper_record['count'] == 0:
                logger.info(f"Paper not found in database: {paper_id}")
                return []
            
            query = """
            MATCH (p:Paper {id: $paper_id})-[:HAS_CODE]->(r:Repository)
            RETURN r.url as repo_url
            """
            result = session.run(query, paper_id=paper_id)
            
            repositories = []
            for record in result:
                repo = Repository.load_from_neo4j(record['repo_url'])
                if repo:
                    repositories.append(repo)
            
            return repositories
            
        except Exception as e:
            logger.error(f"Error searching code by paper: {e}")
            return []

class PapersWithCodeGraph:
    """Main class to manage the Papers with Code knowledge graph"""
    
    def __init__(self, neo4j_uri: str, username: str, password: str):
        """Initialize the knowledge graph with Neo4j connection"""
        Paper.init_neo4j(neo4j_uri, username, password)
        Repository.init_neo4j(neo4j_uri, username, password)
        Dataset.init_neo4j(neo4j_uri, username, password)
        Task.init_neo4j(neo4j_uri, username, password)
        
        self.create_indexes()
    
    def create_indexes(self):
        """Create Neo4j indexes for better performance"""
        try:
            session = Paper.get_session()
            
            indexes = [
                "CREATE INDEX paper_id_index IF NOT EXISTS FOR (p:Paper) ON (p.id)",
                "CREATE INDEX paper_arxiv_index IF NOT EXISTS FOR (p:Paper) ON (p.arxiv_id)",
                "CREATE INDEX repo_url_index IF NOT EXISTS FOR (r:Repository) ON (r.url)",
                "CREATE INDEX author_name_index IF NOT EXISTS FOR (a:Author) ON (a.name)",
                "CREATE INDEX dataset_id_index IF NOT EXISTS FOR (d:Dataset) ON (d.id)",
                "CREATE INDEX task_id_index IF NOT EXISTS FOR (t:Task) ON (t.id)",
            ]
            
            for index_query in indexes:
                session.run(index_query)
            
            logger.info("Neo4j indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Could not create indexes (this is normal for new databases): {e}")
    
    def close(self):
        """Close all Neo4j connections"""
        Paper.close_connection()
    
    def batch_save_papers(self, papers: List, batch_size: int = 100) -> Dict[str, int]:
        """Save papers in batches for better performance"""
        if not papers:
            return {"saved": 0, "failed": 0}
        
        saved_count = 0
        failed_count = 0
        
        try:
            session = Paper.get_session()
            
            # Process papers in batches
            for i in range(0, len(papers), batch_size):
                batch = papers[i:i + batch_size]
                
                try:
                    # Prepare batch data
                    papers_data = []
                    authors_data = []
                    repositories_data = []
                    paper_repo_links = []
                    
                    for paper in batch:
                        # Prepare paper data
                        paper_dict = paper.dict(exclude={'authors', 'repositories', 'datasets', 'tasks'})
                        if paper_dict.get('published'):
                            paper_dict['published'] = paper_dict['published'].isoformat()
                        if paper_dict.get('url_abs'):
                            paper_dict['url_abs'] = str(paper_dict['url_abs'])
                        papers_data.append(paper_dict)
                        
                        # Prepare authors data
                        for author in paper.authors:
                            authors_data.append({
                                'paper_id': paper.id,
                                'name': author.name,
                                'email': author.email,
                                'affiliation': author.affiliation
                            })
                        
                        # Prepare repositories data
                        for repo in paper.repositories:
                            repo_dict = repo.dict()
                            if repo_dict.get('url'):
                                repo_dict['url'] = str(repo_dict['url'])
                            if repo_dict.get('framework'):
                                repo_dict['framework'] = repo_dict['framework'].value if hasattr(repo_dict['framework'], 'value') else str(repo_dict['framework'])
                            if repo_dict.get('created_at'):
                                repo_dict['created_at'] = repo_dict['created_at'].isoformat()
                            repositories_data.append(repo_dict)
                            
                            paper_repo_links.append({
                                'paper_id': paper.id,
                                'repo_url': str(repo.url)
                            })
                    
                    # Batch insert papers
                    if papers_data:
                        paper_batch_query = """
                        UNWIND $papers AS paper_data
                        MERGE (p:Paper {id: paper_data.id})
                        SET p.arxiv_id = paper_data.arxiv_id,
                            p.url_abs = paper_data.url_abs,
                            p.title = paper_data.title,
                            p.abstract = paper_data.abstract,
                            p.published = paper_data.published,
                            p.venue = paper_data.venue,
                            p.citation_count = paper_data.citation_count,
                            p.updated_at = datetime()
                        """
                        session.run(paper_batch_query, papers=papers_data)
                    
                    # Batch insert authors and relationships
                    if authors_data:
                        author_batch_query = """
                        UNWIND $authors AS author_data
                        MERGE (a:Author {name: author_data.name})
                        SET a.email = author_data.email,
                            a.affiliation = author_data.affiliation
                        WITH a, author_data
                        MATCH (p:Paper {id: author_data.paper_id})
                        MERGE (a)-[:AUTHORED]->(p)
                        """
                        session.run(author_batch_query, authors=authors_data)
                    
                    # Batch insert repositories
                    if repositories_data:
                        repo_batch_query = """
                        UNWIND $repositories AS repo_data
                        MERGE (r:Repository {url: repo_data.url})
                        SET r.owner = repo_data.owner,
                            r.name = repo_data.name,
                            r.description = repo_data.description,
                            r.stars = repo_data.stars,
                            r.framework = repo_data.framework,
                            r.language = repo_data.language,
                            r.license = repo_data.license,
                            r.created_at = repo_data.created_at,
                            r.updated_at = datetime()
                        """
                        session.run(repo_batch_query, repositories=repositories_data)
                    
                    # Batch create paper-repository relationships
                    if paper_repo_links:
                        link_batch_query = """
                        UNWIND $links AS link_data
                        MATCH (p:Paper {id: link_data.paper_id})
                        MATCH (r:Repository {url: link_data.repo_url})
                        MERGE (p)-[:HAS_CODE]->(r)
                        """
                        session.run(link_batch_query, links=paper_repo_links)
                    
                    saved_count += len(batch)
                    
                except Exception as e:
                    logger.error(f"Failed to save batch starting at index {i}: {e}")
                    failed_count += len(batch)
                    continue
            
            return {"saved": saved_count, "failed": failed_count}
            
        except Exception as e:
            logger.error(f"Batch save failed: {e}")
            return {"saved": saved_count, "failed": len(papers) - saved_count}
    
    def batch_save_datasets(self, datasets: List, batch_size: int = 50) -> Dict[str, int]:
        """Save datasets in batches for better performance"""
        if not datasets:
            return {"saved": 0, "failed": 0}
        
        saved_count = 0
        failed_count = 0
        
        try:
            session = Dataset.get_session()
            
            # Process datasets in batches
            for i in range(0, len(datasets), batch_size):
                batch = datasets[i:i + batch_size]
                
                try:
                    datasets_data = [dataset.dict() for dataset in batch]
                    
                    batch_query = """
                    UNWIND $datasets AS dataset_data
                    MERGE (d:Dataset {id: dataset_data.id})
                    SET d.name = dataset_data.name,
                        d.full_name = dataset_data.full_name,
                        d.url = dataset_data.url,
                        d.description = dataset_data.description,
                        d.paper_count = dataset_data.paper_count,
                        d.updated_at = datetime()
                    """
                    session.run(batch_query, datasets=datasets_data)
                    saved_count += len(batch)
                    
                except Exception as e:
                    logger.error(f"Failed to save dataset batch starting at index {i}: {e}")
                    failed_count += len(batch)
                    continue
            
            return {"saved": saved_count, "failed": failed_count}
            
        except Exception as e:
            logger.error(f"Dataset batch save failed: {e}")
            return {"saved": saved_count, "failed": len(datasets) - saved_count}
    
    def clear_all_data(self) -> Dict[str, int]:
        """Clear all data from Neo4j database"""
        try:
            session = Paper.get_session()
            
            # Count existing data before deletion
            existing_data = self.check_existing_data()
            
            logger.info("🗑️  Clearing all existing data from Neo4j...")
            
            # Delete all relationships first (to avoid constraint issues)
            delete_queries = [
                "MATCH ()-[r]->() DELETE r",  # Delete all relationships
                "MATCH (n) DELETE n"         # Delete all nodes
            ]
            
            for query in delete_queries:
                session.run(query)
            
            logger.info(f"✅ Cleared: {existing_data}")
            return existing_data
            
        except Exception as e:
            logger.error(f"Failed to clear data: {e}")
            return {'papers': 0, 'datasets': 0, 'repositories': 0, 'authors': 0}
    
    def clear_papers_only(self) -> int:
        """Clear only papers and their relationships"""
        try:
            session = Paper.get_session()
            
            # Count existing papers
            result = session.run("MATCH (p:Paper) RETURN count(p) as count")
            record = result.single()
            paper_count = record['count'] if record else 0
            
            if paper_count > 0:
                logger.info(f"🗑️  Clearing {paper_count} existing papers from Neo4j...")
                
                # Delete paper relationships and papers
                delete_queries = [
                    "MATCH (p:Paper)-[r]-() DELETE r",  # Delete paper relationships
                    "MATCH (p:Paper) DELETE p"          # Delete paper nodes
                ]
                
                for query in delete_queries:
                    session.run(query)
                
                logger.info(f"✅ Cleared {paper_count} papers")
            
            return paper_count
            
        except Exception as e:
            logger.error(f"Failed to clear papers: {e}")
            return 0
    
    def clear_datasets_only(self) -> int:
        """Clear only datasets and their relationships"""
        try:
            session = Dataset.get_session()
            
            # Count existing datasets
            result = session.run("MATCH (d:Dataset) RETURN count(d) as count")
            record = result.single()
            dataset_count = record['count'] if record else 0
            
            if dataset_count > 0:
                logger.info(f"🗑️  Clearing {dataset_count} existing datasets from Neo4j...")
                
                # Delete dataset relationships and datasets
                delete_queries = [
                    "MATCH (d:Dataset)-[r]-() DELETE r",  # Delete dataset relationships
                    "MATCH (d:Dataset) DELETE d"          # Delete dataset nodes
                ]
                
                for query in delete_queries:
                    session.run(query)
                
                logger.info(f"✅ Cleared {dataset_count} datasets")
            
            return dataset_count
            
        except Exception as e:
            logger.error(f"Failed to clear datasets: {e}")
            return 0
    
    def clear_pwc_data_only(self) -> Dict[str, int]:
        """Clear only Papers with Code specific data types and relationships"""
        try:
            session = Paper.get_session()
            
            # Define our specific node types
            pwc_node_types = ['Paper', 'Repository', 'Dataset', 'Task', 'Author']
            # Define our specific relationship types
            pwc_relationship_types = ['AUTHORED', 'HAS_CODE', 'USES_DATASET', 'ADDRESSES_TASK']
            
            # Count existing data before deletion
            existing_data = {}
            for node_type in pwc_node_types:
                try:
                    result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
                    record = result.single()
                    existing_data[node_type.lower()] = record['count'] if record else 0
                except:
                    existing_data[node_type.lower()] = 0
            
            total_nodes = sum(existing_data.values())
            
            if total_nodes > 0:
                logger.info(f"🗑️  Selectively clearing Papers with Code data from Neo4j...")
                logger.info(f"   Node types to clear: {', '.join(pwc_node_types)}")
                logger.info(f"   Relationship types to clear: {', '.join(pwc_relationship_types)}")
                
                # First, delete our specific relationships
                for rel_type in pwc_relationship_types:
                    try:
                        result = session.run(f"MATCH ()-[r:{rel_type}]-() DELETE r")
                        logger.info(f"   ✅ Cleared {rel_type} relationships")
                    except Exception as e:
                        logger.warning(f"   ⚠️  Could not clear {rel_type} relationships: {e}")
                
                # Then delete our specific node types
                for node_type in pwc_node_types:
                    try:
                        # First delete any remaining relationships to these nodes
                        session.run(f"MATCH (n:{node_type})-[r]-() DELETE r")
                        # Then delete the nodes
                        result = session.run(f"MATCH (n:{node_type}) DELETE n")
                        if existing_data[node_type.lower()] > 0:
                            logger.info(f"   ✅ Cleared {existing_data[node_type.lower()]} {node_type} nodes")
                    except Exception as e:
                        logger.warning(f"   ⚠️  Could not clear {node_type} nodes: {e}")
                
                logger.info(f"✅ Selective clearing complete - cleared {total_nodes} PWC nodes")
            else:
                logger.info("ℹ️  No Papers with Code data found to clear")
            
            return existing_data
            
        except Exception as e:
            logger.error(f"Failed to selectively clear PWC data: {e}")
            return {'papers': 0, 'datasets': 0, 'repositories': 0, 'authors': 0, 'tasks': 0}
    
    def clear_pwc_indexes_only(self):
        """Clear only Papers with Code specific indexes"""
        try:
            session = Paper.get_session()
            
            # Define our specific indexes
            pwc_indexes = [
                "paper_id_index",
                "paper_arxiv_index", 
                "repo_url_index",
                "author_name_index",
                "dataset_id_index",
                "task_id_index"
            ]
            
            logger.info("🗑️  Clearing Papers with Code specific indexes...")
            
            for index_name in pwc_indexes:
                try:
                    session.run(f"DROP INDEX {index_name} IF EXISTS")
                    logger.info(f"   ✅ Dropped index: {index_name}")
                except Exception as e:
                    logger.debug(f"   ℹ️  Index {index_name} may not exist: {e}")
            
            logger.info("✅ PWC indexes cleared")
            
        except Exception as e:
            logger.warning(f"Could not clear PWC indexes: {e}")
    
    def check_existing_data(self) -> Dict[str, int]:
        """Check what data already exists in Neo4j"""
        try:
            session = Paper.get_session()
            
            counts = {}
            
            # Check papers
            result = session.run("MATCH (p:Paper) RETURN count(p) as count")
            record = result.single()
            counts['papers'] = record['count'] if record else 0
            
            # Check datasets  
            result = session.run("MATCH (d:Dataset) RETURN count(d) as count")
            record = result.single()
            counts['datasets'] = record['count'] if record else 0
            
            # Check repositories
            result = session.run("MATCH (r:Repository) RETURN count(r) as count")
            record = result.single()
            counts['repositories'] = record['count'] if record else 0
            
            # Check authors
            result = session.run("MATCH (a:Author) RETURN count(a) as count")
            record = result.single()
            counts['authors'] = record['count'] if record else 0
            
            return counts
            
        except Exception as e:
            logger.error(f"Failed to check existing data: {e}")
            return {'papers': 0, 'datasets': 0, 'repositories': 0, 'authors': 0}
    
    def get_graph_stats(self) -> Dict[str, int]:
        """Get statistics about the knowledge graph"""
        try:
            session = Paper.get_session()
            
            # Get individual counts first to avoid complex WITH clauses
            stats = {}
            
            # Count papers
            result = session.run("MATCH (p:Paper) RETURN count(p) as count")
            record = result.single()
            stats['papers'] = record['count'] if record else 0
            
            # Count repositories
            result = session.run("MATCH (r:Repository) RETURN count(r) as count")
            record = result.single()
            stats['repositories'] = record['count'] if record else 0
            
            # Count authors
            result = session.run("MATCH (a:Author) RETURN count(a) as count")
            record = result.single()
            stats['authors'] = record['count'] if record else 0
            
            # Count datasets
            result = session.run("MATCH (d:Dataset) RETURN count(d) as count")
            record = result.single()
            stats['datasets'] = record['count'] if record else 0
            
            # Count tasks
            result = session.run("MATCH (t:Task) RETURN count(t) as count")
            record = result.single()
            stats['tasks'] = record['count'] if record else 0
            
            # Count paper-code relationships (only if both nodes exist)
            if stats['papers'] > 0 and stats['repositories'] > 0:
                result = session.run("MATCH (p:Paper)-[:HAS_CODE]->(r:Repository) RETURN count(*) as count")
                record = result.single()
                stats['paper_code_links'] = record['count'] if record else 0
            else:
                stats['paper_code_links'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting graph stats: {e}")
            return {
                'papers': 0,
                'repositories': 0,
                'authors': 0,
                'datasets': 0,
                'tasks': 0,
                'paper_code_links': 0
            }