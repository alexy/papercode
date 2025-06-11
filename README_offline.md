# Papers with Code Offline Data Pipeline

Extension to the Papers with Code Knowledge Graph system that downloads and processes the official JSON datasets instead of using the API.

## New Components

### `pwc_dataset_downloader.py` - Official Dataset Downloader

Downloads the complete Papers with Code datasets as provided by the official website:

- **Papers with abstracts** (papers-with-abstracts.json.gz)
- **Links between papers and code** (links-between-papers-and-code.json.gz)
- **Evaluation tables** (evaluation-tables.json.gz)
- **Methods** (methods.json.gz)
- **Datasets** (datasets.json.gz)

### `pwc_offline_loader.py` - Offline Data Processor

Processes the downloaded JSON files and loads them into the Neo4j knowledge graph without API calls.

### `test_offline_pipeline.py` - Offline Pipeline Tests

Tests the complete offline data pipeline from download to Neo4j loading.

## Usage

### 1. Download Official Datasets

```bash
# Download all datasets (creates pwc-YYYYMMDD directory)
python pwc_dataset_downloader.py

# Or download specific datasets
from pwc_dataset_downloader import PapersWithCodeDatasetDownloader
downloader = PapersWithCodeDatasetDownloader()
downloader.download_file('papers')
downloader.download_file('links')
```

### 2. Process Offline Data

```bash
# Use most recent download directory
python pwc_offline_loader.py

# Or specify directory
python pwc_offline_loader.py pwc-20250610
```

### 3. Complete Pipeline

```python
from pwc_dataset_downloader import PapersWithCodeDatasetDownloader
from pwc_offline_loader import PapersWithCodeOfflineLoader
from models import PapersWithCodeGraph

# Step 1: Download datasets
downloader = PapersWithCodeDatasetDownloader()
downloader.download_all(delay_seconds=2.0)
downloader.extract_all()

# Step 2: Process offline data
loader = PapersWithCodeOfflineLoader(str(downloader.download_dir))

# Step 3: Load into Neo4j
graph = PapersWithCodeGraph("bolt://localhost:7687", "neo4j", "password")
stats = loader.load_and_save_to_neo4j(
    graph=graph,
    paper_limit=1000,
    dataset_limit=500,
    include_repositories=True
)
```

## Data Sources

The offline pipeline uses the official Papers with Code datasets:

### Available Datasets

1. **Papers with Abstracts** (~18 MB extracted)
   - Contains research papers with titles, abstracts, authors
   - Publication dates, venues, citation counts

2. **Links Between Papers and Code** 
   - Connections between papers and GitHub repositories
   - Repository metadata (stars, framework, language)

3. **Evaluation Tables**
   - Benchmark results and performance metrics
   - Task-specific evaluations

4. **Methods**
   - Machine learning methods and techniques
   - Method descriptions and categories

5. **Datasets**
   - Research dataset information
   - Dataset descriptions and usage statistics

### Data Format

The JSON files use different formats:
- **JSON Arrays**: Single large array of objects
- **Line-delimited JSON**: One JSON object per line
- **Mixed formats**: Handled automatically by the loader

## Directory Structure

```
pwc-YYYYMMDD/                    # Download directory with date
├── papers-with-abstracts.json.gz     # Compressed papers data
├── papers-with-abstracts.json        # Extracted papers data
├── links-between-papers-and-code.json.gz
├── links-between-papers-and-code.json
├── evaluation-tables.json.gz
├── evaluation-tables.json
├── methods.json.gz
├── methods.json
├── datasets.json.gz
└── datasets.json
```

## Advantages of Offline Pipeline

1. **Complete Data Access**: Gets the full dataset, not just API samples
2. **No Rate Limiting**: Process data as fast as your system allows
3. **Reproducible**: Exact snapshot of data at download time
4. **Offline Processing**: No internet required after download
5. **Bulk Operations**: Efficient for large-scale processing

## Features

### Smart JSON Parsing
- Automatically detects JSON format (array vs line-delimited)
- Handles malformed lines gracefully
- Memory-efficient streaming for large files

### Progress Tracking
- Download progress with file sizes
- Processing progress with record counts
- Error tracking and reporting

### Data Validation
- Pydantic validation for all parsed objects
- Framework detection and normalization
- URL validation and cleaning

### Neo4j Integration
- Batch processing for performance
- Relationship building between papers and code
- Index creation for optimal querying

## Example Queries After Loading

```cypher
// Papers with most GitHub repositories
MATCH (p:Paper)-[:HAS_CODE]->(r:Repository)
RETURN p.title, count(r) as repo_count
ORDER BY repo_count DESC
LIMIT 10

// Most popular frameworks
MATCH (r:Repository)
WHERE r.framework IS NOT NULL
RETURN r.framework, count(*) as count, avg(r.stars) as avg_stars
ORDER BY count DESC

// Papers by year with code
MATCH (p:Paper)-[:HAS_CODE]->(r:Repository)
WHERE p.published IS NOT NULL
RETURN substring(p.published, 0, 4) as year, count(p) as papers_with_code
ORDER BY year DESC
```

## Configuration

### Download Settings

```python
# Adjust rate limiting
downloader = PapersWithCodeDatasetDownloader()
downloader.download_all(delay_seconds=1.0)  # 1 second between downloads

# Custom download directory
downloader = PapersWithCodeDatasetDownloader(base_dir="/path/to/data")
```

### Processing Limits

```python
# Limit processing for testing
loader.load_and_save_to_neo4j(
    graph=graph,
    paper_limit=100,     # Process only 100 papers
    dataset_limit=50,    # Process only 50 datasets
    include_repositories=True
)
```

## Testing

```bash
# Test the complete offline pipeline
python test_offline_pipeline.py

# Test individual components
from test_offline_pipeline import test_dataset_downloader, test_offline_loader
test_dataset_downloader()
test_offline_loader()
```

## Troubleshooting

### Common Issues

1. **Large File Sizes**
   - Papers dataset can be >100MB extracted
   - Links dataset can be >50MB extracted
   - Ensure sufficient disk space

2. **Memory Usage**
   - Use limits for testing: `paper_limit=100`
   - Process in batches for production

3. **JSON Parsing Errors**
   - Some files may have invalid JSON lines
   - Parser skips invalid lines and continues

4. **Neo4j Performance**
   - Create indexes before loading large datasets
   - Use batch processing (implemented by default)

### Performance Tips

```python
# For large datasets, process in chunks
for i in range(0, total_papers, 1000):
    papers_chunk = papers[i:i+1000]
    # Process chunk
```

## License

Data is licensed under CC BY-SA, same as Papers with Code.

## Future Enhancements

- Incremental updates (download only new data)
- Parallel processing for multiple files
- Data compression in Neo4j
- Advanced relationship extraction from evaluation tables