# Papers with Code Knowledge Graph

A comprehensive system for building a knowledge graph of research papers and their associated code repositories using data from Papers with Code, implemented with Pydantic models and Neo4j.

## Features

- **Pydantic Data Models**: Type-safe models for Papers, Repositories, Datasets, Tasks, and Authors
- **Neo4j Integration**: Built-in graph database operations within Pydantic models
- **Papers with Code API Loader**: Automated data fetching from Papers with Code API
- **Knowledge Graph**: Rich relationships between papers, code, datasets, and research tasks
- **Rate Limiting**: Respectful API usage with configurable rate limits

## Architecture

### Core Models

1. **Paper**: Research papers with metadata, authors, and relationships
2. **Repository**: Code repositories with framework detection and metrics
3. **Dataset**: Research datasets used by papers
4. **Task**: Research tasks addressed by papers
5. **Author**: Paper authors with affiliations

### Relationships

- `Paper -[:HAS_CODE]-> Repository`
- `Paper -[:USES_DATASET]-> Dataset`
- `Paper -[:ADDRESSES_TASK]-> Task`
- `Author -[:AUTHORED]-> Paper`

### Neo4j Schema

```cypher
# Node Types
(:Paper {id, title, abstract, published, venue, citation_count})
(:Repository {url, owner, name, description, stars, framework, language})
(:Dataset {id, name, full_name, description, paper_count})
(:Task {id, name, description, area})
(:Author {name, email, affiliation})

# Relationships
(:Paper)-[:HAS_CODE]->(:Repository)
(:Paper)-[:USES_DATASET]->(:Dataset)
(:Paper)-[:ADDRESSES_TASK]->(:Task)
(:Author)-[:AUTHORED]->(:Paper)
```

## Setup

### Prerequisites

1. **Python 3.8+**
2. **Neo4j Database** (local or cloud)
   - Download from [neo4j.com](https://neo4j.com/download/)
   - Default credentials: username=`neo4j`, password=`password`

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install manually
pip install pydantic>=2.0.0 neo4j>=5.0.0 requests>=2.28.0 python-dateutil>=2.8.0
```

### Neo4j Setup

1. Start Neo4j:
   ```bash
   # Using Neo4j Desktop or
   # Using Docker
   docker run --publish=7474:7474 --publish=7687:7687 --env=NEO4J_AUTH=neo4j/password neo4j:latest
   ```

2. Access Neo4j Browser at `http://localhost:7474`

## Usage

### Basic Usage

```python
from models import Paper, Repository, Dataset, PapersWithCodeGraph
from pwc_loader import PapersWithCodeLoader

# Initialize knowledge graph
graph = PapersWithCodeGraph(
    neo4j_uri="bolt://localhost:7687",
    username="neo4j",
    password="password"
)

# Load data from Papers with Code API
loader = PapersWithCodeLoader()
stats = loader.load_and_save_to_neo4j(
    graph=graph,
    paper_limit=50,
    repo_limit=100,
    dataset_limit=50,
    task_limit=50
)

# Query relationships
papers_using_pytorch = Paper.search_papers_by_code("https://github.com/pytorch/pytorch")
code_for_paper = Paper.search_code_by_paper("paper_id")

# Get graph statistics
stats = graph.get_graph_stats()
print(f"Papers: {stats['papers']}, Repositories: {stats['repositories']}")

graph.close()
```

### Manual Data Creation

```python
from models import Paper, Repository, Author, Framework
from datetime import datetime

# Create a repository
repo = Repository(
    url="https://github.com/pytorch/pytorch",
    owner="pytorch",
    name="pytorch",
    description="Tensors and Dynamic neural networks in Python",
    stars=50000,
    framework=Framework.PYTORCH,
    language="Python"
)

# Create a paper
paper = Paper(
    id="unique_paper_id",
    title="Attention Is All You Need",
    abstract="The dominant sequence transduction models...",
    authors=[Author(name="Ashish Vaswani")],
    published=datetime(2017, 6, 12),
    repositories=[repo],
    datasets=["wmt2014"],
    tasks=["machine-translation"]
)

# Save to Neo4j
repo.save_to_neo4j()
paper.save_to_neo4j()

# Load from Neo4j
loaded_paper = Paper.load_from_neo4j("unique_paper_id")
```

### Running the Data Pipeline

```bash
# Online API Pipeline
python pwc_loader.py --neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-password mypassword

# Offline Data Pipeline
python pwc_dataset_downloader.py --datasets papers links --output-dir /path/to/data
python pwc_offline_loader.py /path/to/data/pwc-20250610 --neo4j-password mypassword

# Run tests
python test_pipeline.py
```

## Configuration

### Neo4j Connection

Update the connection details in your scripts:

```python
NEO4J_URI = "bolt://localhost:7687"  # or your Neo4j cloud URI
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "your_password"
```

### Rate Limiting

Adjust API rate limiting in `PapersWithCodeLoader`:

```python
loader = PapersWithCodeLoader()
loader.request_delay = 2.0  # seconds between requests
```

## Example Queries

### Neo4j Cypher Queries

```cypher
// Find papers using PyTorch
MATCH (p:Paper)-[:HAS_CODE]->(r:Repository)
WHERE r.framework = "PyTorch"
RETURN p.title, r.url

// Most cited papers with code
MATCH (p:Paper)-[:HAS_CODE]->(r:Repository)
RETURN p.title, p.citation_count, count(r) as repo_count
ORDER BY p.citation_count DESC
LIMIT 10

// Authors with most papers
MATCH (a:Author)-[:AUTHORED]->(p:Paper)
RETURN a.name, count(p) as paper_count
ORDER BY paper_count DESC
LIMIT 10

// Papers addressing specific tasks
MATCH (p:Paper)-[:ADDRESSES_TASK]->(t:Task {name: "image-classification"})
RETURN p.title, p.published
ORDER BY p.published DESC
```

### Python API Queries

```python
# Find all papers using a specific repository
papers = Paper.search_papers_by_code("https://github.com/huggingface/transformers")

# Find all code for a paper
repositories = Paper.search_code_by_paper("paper_id")

# Get graph statistics
stats = graph.get_graph_stats()
```

## Testing

Run the test suite:

```bash
python test_pipeline.py
```

Tests include:
- Pydantic model validation
- Neo4j integration (requires running Neo4j)
- Papers with Code API integration
- Data pipeline functionality

## Data Sources

- **Papers with Code API**: `https://paperswithcode.com/api/v1/`
- **Rate Limits**: Respectful 1-second delays between requests
- **Data License**: CC BY-SA (as per Papers with Code)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project follows the same CC BY-SA license as Papers with Code data.

## Troubleshooting

### Common Issues

1. **Neo4j Connection Error**
   - Ensure Neo4j is running
   - Check connection URI and credentials
   - Verify firewall settings

2. **API Rate Limiting**
   - Increase `request_delay` in loader
   - Papers with Code may have usage limits

3. **Memory Usage**
   - Start with small limits for testing
   - Process data in batches for large datasets

4. **Framework Detection**
   - Framework enum may need updates for new frameworks
   - Custom framework parsing in Repository model

### Logging

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```