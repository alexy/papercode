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
        if cls._session is None:
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
            
            # Load paper with all relationships
            query = """
            MATCH (p:Paper {id: $id})
            OPTIONAL MATCH (a:Author)-[:AUTHORED]->(p)
            OPTIONAL MATCH (p)-[:HAS_CODE]->(r:Repository)
            OPTIONAL MATCH (p)-[:USES_DATASET]->(d:Dataset)
            OPTIONAL MATCH (p)-[:ADDRESSES_TASK]->(t:Task)
            RETURN p,
                   collect(DISTINCT a) as authors,
                   collect(DISTINCT r) as repositories,
                   collect(DISTINCT d.id) as datasets,
                   collect(DISTINCT t.id) as tasks
            """
            
            result = session.run(query, id=paper_id)
            record = result.single()
            
            if record:
                paper_data = dict(record['p'])
                
                # Convert datetime
                if paper_data.get('published'):
                    paper_data['published'] = datetime.fromisoformat(paper_data['published'])
                
                # Add relationships
                paper_data['authors'] = [Author(**dict(a)) for a in record['authors'] if a]
                paper_data['repositories'] = [Repository(**dict(r)) for r in record['repositories'] if r]
                paper_data['datasets'] = [d for d in record['datasets'] if d]
                paper_data['tasks'] = [t for t in record['tasks'] if t]
                
                return cls(**paper_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading paper from Neo4j: {e}")
            return None
    
    @classmethod
    def search_papers_by_code(cls, repo_url: str) -> List['Paper']:
        """Find all papers that use a specific code repository"""
        try:
            session = cls.get_session()
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
            logger.error(f"Error creating indexes: {e}")
    
    def close(self):
        """Close all Neo4j connections"""
        Paper.close_connection()
    
    def get_graph_stats(self) -> Dict[str, int]:
        """Get statistics about the knowledge graph"""
        try:
            session = Paper.get_session()
            
            stats_query = """
            MATCH (p:Paper) WITH count(p) as papers
            MATCH (r:Repository) WITH papers, count(r) as repos
            MATCH (a:Author) WITH papers, repos, count(a) as authors
            MATCH (d:Dataset) WITH papers, repos, authors, count(d) as datasets
            MATCH (t:Task) WITH papers, repos, authors, datasets, count(t) as tasks
            MATCH (p:Paper)-[:HAS_CODE]->(r:Repository) WITH papers, repos, authors, datasets, tasks, count(*) as paper_code_links
            RETURN papers, repos, authors, datasets, tasks, paper_code_links
            """
            
            result = session.run(stats_query)
            record = result.single()
            
            return {
                'papers': record['papers'],
                'repositories': record['repos'],
                'authors': record['authors'],
                'datasets': record['datasets'],
                'tasks': record['tasks'],
                'paper_code_links': record['paper_code_links']
            }
            
        except Exception as e:
            logger.error(f"Error getting graph stats: {e}")
            return {}