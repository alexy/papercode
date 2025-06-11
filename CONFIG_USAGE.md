# Configuration File Usage Guide

## 🔧 Overview

The Papers with Code loader now supports YAML configuration files to store Neo4j connection settings. This eliminates the need to specify credentials on the command line and makes it easier to switch between different environments.

## 📁 Configuration File Structure

### Default Config File: `config.yaml`

```yaml
# Neo4j Configuration for Papers with Code Loader
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "your_local_password"
  description: "Local development Neo4j instance"

remote:
  uri: "bolt://production-server:7687"
  user: "neo4j"
  password: "your_remote_password"
  description: "Remote production Neo4j instance"

# Additional environments (optional)
staging:
  uri: "bolt://staging.example.com:7687"
  user: "neo4j"
  password: "staging_password"
  description: "Staging environment"
```

### Required Fields

Each environment must have:
- **`uri`**: Neo4j connection URI (e.g., `bolt://server:7687`)
- **`user`**: Neo4j username
- **`password`**: Neo4j password

### Optional Fields

- **`description`**: Human-readable description of the environment

## 🚀 Usage Examples

### 1. Basic Local Loading with Config

```bash
# Load data to local Neo4j using config file
python pwc_offline_loader.py pwc-20250610 --local --paper-limit 1000
```

### 2. Load from Local to Remote

```bash
# Load from local config to remote config
python pwc_offline_loader.py pwc-20250610 --local --remote --clear-target --paper-limit 5000
```

### 3. Custom Config File

```bash
# Use a different config file
python pwc_offline_loader.py pwc-20250610 --config my-config.yaml --local --paper-limit 1000
```

### 4. Compare Local vs Remote

```bash
# Compare local and remote instances using config
python neo4j_diff.py --local --remote --pwc-only
```

### 5. Mix Config with Explicit Parameters

```bash
# Use local config, but specify explicit remote target
python pwc_offline_loader.py pwc-20250610 \
    --local \
    --new-neo4j-uri bolt://custom-server:7687 \
    --new-neo4j-password custom_password \
    --clear-target
```

## 📋 Command Line Flags

### Offline Loader (`pwc_offline_loader.py`)

| Flag | Description |
|------|-------------|
| `--config PATH` | Path to config file (default: `config.yaml`) |
| `--local` | Use local environment from config |
| `--remote` | Use remote environment as target |

### Neo4j Diff (`neo4j_diff.py`)

| Flag | Description |
|------|-------------|
| `--config PATH` | Path to config file (default: `config.yaml`) |
| `--local` | Use local environment as source |
| `--remote` | Use remote environment as target |

## 🔐 Security Best Practices

### 1. File Permissions
```bash
# Restrict access to config file
chmod 600 config.yaml
```

### 2. Environment Variables
For production use, consider using environment variables:

```yaml
local:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "${LOCAL_NEO4J_PASSWORD}"  # Will be replaced from env var
```

### 3. Separate Config Files
Use different config files for different environments:

```bash
# Development
python pwc_offline_loader.py --config dev-config.yaml --local

# Production
python pwc_offline_loader.py --config prod-config.yaml --local
```

### 4. Never Commit Credentials
Add config files with real passwords to `.gitignore`:

```gitignore
config.yaml
*-config.yaml
prod-*.yaml
```

## 🛠️ Config Management

### Create Example Config
```bash
# Create a template config file
python config_parser.py --create-example --config my-config.yaml
```

### Validate Config
```bash
# Validate your config file
python config_parser.py --validate --config config.yaml
```

### Test Specific Environment
```bash
# Test a specific environment
python config_parser.py --environment staging --config config.yaml
```

## 🎯 Priority Order

When multiple credential sources are specified, the priority is:

1. **Explicit command line flags** (highest priority)
2. **Config file values**
3. **Default values** (lowest priority)

### Example:
```bash
# Uses custom URI but local config for user/password
python pwc_offline_loader.py pwc-20250610 \
    --local \
    --neo4j-uri bolt://custom-server:7687
```

## 📊 Common Workflows

### 1. Development Workflow
```bash
# 1. Create config with local credentials
python config_parser.py --create-example

# 2. Edit config.yaml with your credentials

# 3. Load data locally
python pwc_offline_loader.py pwc-20250610 --local --paper-limit 1000
```

### 2. Production Deployment
```bash
# 1. Load to production server
python pwc_offline_loader.py pwc-20250610 --remote --clear-target

# 2. Verify deployment
python neo4j_diff.py --local --remote --pwc-only
```

### 3. Staging Validation
```bash
# 1. Load to staging (using custom environment)
python pwc_offline_loader.py pwc-20250610 \
    --config prod-config.yaml \
    --new-neo4j-uri bolt://staging-server:7687 \
    --new-neo4j-user staging_user \
    --new-neo4j-password staging_pass \
    --clear-target

# 2. Compare staging vs production
python neo4j_diff.py \
    --source-uri bolt://staging-server:7687 \
    --target-uri bolt://prod-server:7687 \
    --pwc-only
```

## ⚠️ Troubleshooting

### Config File Not Found
```
WARNING: Config file not found: config.yaml
```
**Solution**: Create a config file or specify a different path with `--config`

### Environment Not Found
```
ERROR: Environment 'production' not found in config
Available environments: local, remote
```
**Solution**: Check your config file and ensure the environment name is correct

### Missing Required Fields
```
ERROR: Missing required fields in 'local' config: ['password']
```
**Solution**: Add the missing fields to your config file

### Connection Failed
```
ERROR: Neo4j connection failed: Cannot resolve address localhost:7687
```
**Solution**: Check that your Neo4j URI and credentials are correct and that the server is running

## 📝 Example Scenarios

### Scenario 1: Developer Setup
1. Clone repository
2. Start local Neo4j
3. Copy and edit config.yaml
4. Load test data: `python pwc_offline_loader.py --local --paper-limit 100`

### Scenario 2: Production Migration
1. Backup production data
2. Configure production credentials in config.yaml
3. Load data: `python pwc_offline_loader.py --remote --clear-target`
4. Verify: `python neo4j_diff.py --local --remote`

### Scenario 3: Multi-Environment Testing
1. Create separate config files for each environment
2. Test on staging: `python pwc_offline_loader.py --config staging.yaml --local`
3. Deploy to production: `python pwc_offline_loader.py --config prod.yaml --local`
4. Compare environments: `python neo4j_diff.py --config staging.yaml --local --config prod.yaml --remote`