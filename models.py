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
    title: str
    abstract: Optional[str] = None
    authors: List[Author] = []
    published: Optional[datetime] = None
    venue: Optional[str] = None
    citation_count: Optional[int] = 0
    
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