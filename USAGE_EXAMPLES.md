# Usage Examples for Updated Offline Loader

## 🔧 Configuration File Support

The loader now supports YAML configuration files for storing Neo4j credentials:

```bash
# Create example config file
cp config.yaml my-config.yaml
# Edit my-config.yaml with your credentials

# Use config file
python pwc_offline_loader.py pwc-20250610 --local --paper-limit 1000
python pwc_offline_loader.py pwc-20250610 --local --remote --clear-target
```

**See [CONFIG_USAGE.md](CONFIG_USAGE.md) for detailed configuration guide.**

### Quick Config Examples

```bash
# Load to local instance from config
python pwc_offline_loader.py pwc-20250610 --local --paper-limit 1000

# Load from local to remote (both from config)
python pwc_offline_loader.py pwc-20250610 --local --remote --clear-target --paper-limit 5000

# Compare local vs remote using config
python neo4j_diff.py --local --remote --pwc-only

# Use custom config file
python pwc_offline_loader.py pwc-20250610 --config my-config.yaml --local
```

## Loading Data to a New Neo4j Instance

### Basic Usage
```bash
# Load data to a new Neo4j instance
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://another-server:7687 \
    --new-neo4j-user neo4j \
    --new-neo4j-password your_password \
    --paper-limit 1000 \
    --dataset-limit 100
```

### Clear Target Before Loading (PWC Data Only)
```bash
# Clear ONLY Papers with Code data from target instance and load fresh data
# This preserves any other data types that might exist in the target database
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://target-server:7687 \
    --new-neo4j-password target_password \
    --clear-target \
    --paper-limit 5000
```

### Rebuild Models Before Loading
```bash
# Rebuild Pydantic models from data and load to original instance
python pwc_offline_loader.py pwc-20250610 \
    --rebuild-models \
    --force-reload \
    --paper-limit 1000
```

### Clear Local Database (PWC Data Only)
```bash
# Clear ONLY Papers with Code data from local instance and reload fresh
# This preserves any other data types that might exist in the local database
python pwc_offline_loader.py pwc-20250610 \
    --clear-all \
    --paper-limit 5000
```

### ⚠️ DANGER: Drop All Data (Complete Database Wipe)
```bash
# DESTROYS ALL DATA in local database (requires confirmation)
# Use only when you want to completely wipe the database
python pwc_offline_loader.py pwc-20250610 \
    --drop-all \
    --paper-limit 5000
```

### ⚠️ DANGER: Drop All Target Data (Complete Target Wipe)
```bash
# DESTROYS ALL DATA in target database (requires confirmation)
# Use only when you want to completely wipe the target database
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://target-server:7687 \
    --new-neo4j-password target_password \
    --drop-all-target \
    --paper-limit 5000
```

## Comparing Neo4j Instances

### Basic Comparison
```bash
# Compare two Neo4j instances
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --source-password original_password \
    --target-uri bolt://target-server:7687 \
    --target-password target_password
```

### Detailed Comparison with Output
```bash
# Compare with larger sample and save results
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --source-password original_password \
    --target-uri bolt://target-server:7687 \
    --target-password target_password \
    --sample-size 50 \
    --output comparison_result.json
```

### PWC-Only Comparison (for Mixed Databases)
```bash
# Compare only Papers with Code data types (ignores other data in database)
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --source-password original_password \
    --target-uri bolt://target-server:7687 \
    --target-password target_password \
    --pwc-only \
    --output pwc_comparison.json
```

### Same Instance Validation
```bash
# Validate data integrity by comparing instance with itself
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --source-password password \
    --target-uri bolt://localhost:7687 \
    --target-password password
```

## Python API Usage

### Rebuilding Models
```python
from pwc_offline_loader import PapersWithCodeOfflineLoader

# Initialize loader
loader = PapersWithCodeOfflineLoader("pwc-20250610")

# Rebuild models from data
rebuilt_models = loader.rebuild_models_from_data()
print(f"Rebuilt {rebuilt_models['stats']['papers_rebuilt']} papers")
```

### Loading to New Instance
```python
from pwc_offline_loader import PapersWithCodeOfflineLoader

# Initialize loader
loader = PapersWithCodeOfflineLoader("pwc-20250610")

# Load to new instance
stats = loader.load_to_new_neo4j_instance(
    new_neo4j_uri="bolt://target-server:7687",
    new_neo4j_user="neo4j",
    new_neo4j_password="target_password",
    paper_limit=1000,
    clear_target=True
)
```

### Comparing Instances
```python
from neo4j_diff import Neo4jDiff

# Initialize diff tool
diff_tool = Neo4jDiff(
    source_uri="bolt://localhost:7687",
    source_user="neo4j",
    source_password="source_password",
    target_uri="bolt://target-server:7687",
    target_user="neo4j",
    target_password="target_password"
)

# Perform comparison
comparison = diff_tool.full_comparison(sample_size=20)

# Print report
diff_tool.print_comparison_report(comparison)

# Clean up
diff_tool.close()
```

## Key Features

### 1. Safe Selective Clearing vs Dangerous Complete Clearing
**Safe Options (PWC Data Only):**
- `--clear-all` and `--clear-target` only remove Papers with Code specific data types:
  - Node types: `Paper`, `Repository`, `Dataset`, `Task`, `Author`
  - Relationship types: `AUTHORED`, `HAS_CODE`, `USES_DATASET`, `ADDRESSES_TASK`
- Preserves all other data types that may exist in the database
- Safe to use on production databases with mixed data

**⚠️ Dangerous Options (ALL Data):**
- `--drop-all` and `--drop-all-target` **DESTROY ALL DATA** in the database:
  - All nodes of every type
  - All relationships of every type
  - All indexes and constraints
  - Everything in the database
- Require explicit confirmation: "DELETE ALL DATA"
- Use only when you want to completely wipe the database

**Flag Comparison Table:**

| Flag | Target | Scope | Safety | Confirmation Required |
|------|--------|-------|--------|--------------------|
| `--clear-all` | Local | PWC data only | ✅ Safe | No |
| `--clear-target` | Target | PWC data only | ✅ Safe | No |
| `--drop-all` | Local | **ALL data** | ⚠️ **DANGEROUS** | **Yes** |
| `--drop-all-target` | Target | **ALL data** | ⚠️ **DANGEROUS** | **Yes** |

### 2. No Duplicate Work
- The loader checks existing data and avoids repeating work
- Uses smart caching to prevent reloading the same datasets
- Builds efficient mappings between papers and repositories

### 2. Pydantic Model Rebuilding
- Rebuilds all models from scratch using the offline data
- Validates data integrity during rebuild process
- Provides detailed statistics on rebuilt models

### 3. Flexible Neo4j Targeting
- Load to any Neo4j instance with different credentials
- Option to clear target before loading
- Maintains all original loading options (limits, skip flags, etc.)

### 4. Comprehensive Diff Tool
- Compares node counts, relationship counts, and sample data
- Provides detailed reports on differences
- Supports JSON output for programmatic analysis
- Exit codes indicate whether instances are identical
- PWC-only mode for comparing mixed databases

### 5. Error Handling and Logging
- Comprehensive error handling throughout the pipeline
- Detailed logging for debugging and monitoring
- Progress tracking with estimated completion times

## Common Workflows

### 1. Migrate Data to New Server
```bash
# Step 1: Load data to new server
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://new-server:7687 \
    --new-neo4j-password new_password \
    --clear-target

# Step 2: Verify migration
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --source-password old_password \
    --target-uri bolt://new-server:7687 \
    --target-password new_password \
    --output migration_verification.json
```

### 2. Create Test Environment (Safe for Mixed Databases)
```bash
# Load subset of data to test instance (only clears PWC data, preserves other data)
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://test-server:7687 \
    --new-neo4j-password test_password \
    --paper-limit 100 \
    --dataset-limit 50 \
    --clear-target  # Only clears Papers with Code data types
```

### 3. Data Validation (Safe for Mixed Databases)
```bash
# Rebuild models and reload to same instance to validate integrity
# Only clears PWC data, preserves other data types
python pwc_offline_loader.py pwc-20250610 \
    --rebuild-models \
    --clear-all \  # Safe: only clears PWC data types
    --paper-limit 1000

# Compare PWC data only (ignores other data types in database)
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --target-uri bolt://localhost:7687 \
    --pwc-only \
    --sample-size 100
```