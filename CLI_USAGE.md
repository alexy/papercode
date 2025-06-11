# Command Line Usage Guide

## Papers with Code Knowledge Graph CLI Tools

### `pwc_dataset_downloader.py` - Download Official Datasets

Downloads the complete Papers with Code JSON datasets from their official source.

```bash
# Download all datasets to current directory
python pwc_dataset_downloader.py

# Download specific datasets
python pwc_dataset_downloader.py --datasets papers links datasets

# Download to specific directory
python pwc_dataset_downloader.py --output-dir /path/to/data

# Download with custom delay and cleanup
python pwc_dataset_downloader.py --delay 1.0 --cleanup

# Skip extraction (keep only compressed files)
python pwc_dataset_downloader.py --no-extract
```

**Options:**
- `--output-dir`: Output directory (default: current directory)
- `--delay`: Delay between downloads in seconds (default: 2.0)
- `--datasets`: Specific datasets to download: papers, links, evaluations, methods, datasets
- `--extract`: Extract files after download (default: True)
- `--no-extract`: Skip extraction
- `--cleanup`: Remove compressed files after extraction

### `pwc_offline_loader.py` - Process Offline Data

Processes downloaded JSON files and loads them into Neo4j knowledge graph.

```bash
# Auto-detect most recent download directory
python pwc_offline_loader.py

# Specify data directory
python pwc_offline_loader.py pwc-20250610

# Custom Neo4j connection
python pwc_offline_loader.py --neo4j-uri bolt://myserver:7687 --neo4j-user admin --neo4j-password secret

# Limit data processing
python pwc_offline_loader.py --paper-limit 100 --dataset-limit 50

# Exclude repository links
python pwc_offline_loader.py --no-repositories
```

**Options:**
- `data_dir`: Path to PWC data directory (optional - auto-detects if not provided)
- `--neo4j-uri`: Neo4j database URI (default: bolt://localhost:7687)
- `--neo4j-user`: Neo4j username (default: neo4j)
- `--neo4j-password`: Neo4j password (default: password)
- `--paper-limit`: Maximum papers to load (default: 50)
- `--dataset-limit`: Maximum datasets to load (default: 30)
- `--include-repositories`: Include repository links (default: True)
- `--no-repositories`: Exclude repository links

### `pwc_loader.py` - Online API Pipeline

Loads data directly from Papers with Code API into Neo4j.

```bash
# Basic usage with defaults
python pwc_loader.py

# Custom Neo4j connection and limits
python pwc_loader.py --neo4j-uri bolt://myserver:7687 --neo4j-password secret --paper-limit 100

# Full production load
python pwc_loader.py --paper-limit 1000 --repo-limit 2000 --dataset-limit 500 --task-limit 500
```

**Options:**
- `--neo4j-uri`: Neo4j database URI (default: bolt://localhost:7687)
- `--neo4j-user`: Neo4j username (default: neo4j)
- `--neo4j-password`: Neo4j password (default: password)
- `--paper-limit`: Maximum papers to load (default: 20)
- `--repo-limit`: Maximum repositories to load (default: 50)
- `--dataset-limit`: Maximum datasets to load (default: 30)
- `--task-limit`: Maximum tasks to load (default: 30)

## Usage Examples

### Quick Start - Offline Pipeline

```bash
# 1. Download sample datasets
python pwc_dataset_downloader.py --datasets papers links datasets

# 2. Load into Neo4j
python pwc_offline_loader.py --neo4j-password mypassword --paper-limit 10
```

### Production Setup

```bash
# 1. Download all datasets to dedicated directory
python pwc_dataset_downloader.py --output-dir /data/pwc --cleanup

# 2. Load full dataset into Neo4j cluster
python pwc_offline_loader.py /data/pwc/pwc-20250610 \
  --neo4j-uri bolt://neo4j-cluster:7687 \
  --neo4j-user admin \
  --neo4j-password production_password \
  --paper-limit 10000 \
  --dataset-limit 1000
```

### Development Testing

```bash
# API pipeline with small limits
python pwc_loader.py --paper-limit 5 --repo-limit 10 --neo4j-password dev

# Offline pipeline with minimal data
python pwc_dataset_downloader.py --datasets datasets --no-extract
python pwc_offline_loader.py --dataset-limit 5 --no-repositories
```

### Help and Documentation

```bash
# Show help for any script
python pwc_dataset_downloader.py --help
python pwc_offline_loader.py --help
python pwc_loader.py --help
```

## Common Workflows

### Research Project Setup

```bash
# Download everything for research
python pwc_dataset_downloader.py --output-dir research_data

# Load with high limits for comprehensive analysis
python pwc_offline_loader.py research_data/pwc-* \
  --paper-limit 5000 \
  --dataset-limit 500 \
  --neo4j-password research_db
```

### CI/CD Testing

```bash
# Minimal data for testing
python pwc_dataset_downloader.py --datasets datasets --delay 0.5
python pwc_offline_loader.py --dataset-limit 5 --paper-limit 3 --no-repositories
```

### Data Updates

```bash
# Download fresh data daily
python pwc_dataset_downloader.py --output-dir daily_snapshots

# Incremental loading (replace with new data)
python pwc_offline_loader.py daily_snapshots/pwc-$(date +%Y%m%d) \
  --paper-limit 1000 \
  --neo4j-password production
```

## Environment Variables

You can also use environment variables instead of command line arguments:

```bash
export NEO4J_URI="bolt://myserver:7687"
export NEO4J_USER="admin"
export NEO4J_PASSWORD="secret"

# Now run without password arguments
python pwc_offline_loader.py
```

## Performance Tips

### For Large Datasets

```bash
# Download in chunks
python pwc_dataset_downloader.py --datasets papers
python pwc_dataset_downloader.py --datasets links
python pwc_dataset_downloader.py --datasets datasets

# Load in batches
python pwc_offline_loader.py --paper-limit 500
python pwc_offline_loader.py --paper-limit 500 --skip-processed
```

### For Development

```bash
# Use small limits during development
python pwc_offline_loader.py --paper-limit 10 --dataset-limit 5
```

### For Production

```bash
# Remove limits for full data load
python pwc_offline_loader.py --paper-limit 0 --dataset-limit 0  # 0 = no limit
```

## Troubleshooting

### Connection Issues

```bash
# Test Neo4j connection
python -c "from models import PapersWithCodeGraph; g = PapersWithCodeGraph('bolt://localhost:7687', 'neo4j', 'password'); print('Connected!'); g.close()"
```

### Data Issues

```bash
# Check what data is available
python pwc_offline_loader.py --help
ls pwc-*/  # List downloaded files
```

### Memory Issues

```bash
# Use smaller limits
python pwc_offline_loader.py --paper-limit 100 --dataset-limit 50
```