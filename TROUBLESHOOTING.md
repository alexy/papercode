# Troubleshooting Guide

## Neo4j Connection Issues

### Problem: "Authentication failure" or "Unauthorized"

**Symptoms:**
```
ERROR: Neo4j connection failed: {code: Neo.ClientError.Security.Unauthorized}
ERROR: The client is unauthorized due to authentication failure
```

**Solutions:**

1. **Test your Neo4j connection:**
   ```bash
   python test_neo4j_connection.py
   ```

2. **Check if Neo4j is running:**
   - Open http://localhost:7474 in your browser
   - You should see the Neo4j Browser interface

3. **Find your correct password:**
   - Check your Neo4j Desktop settings
   - Try common passwords: `neo4j`, `password`, `admin`, `test`
   - Use the correct password with: `--neo4j-password YOUR_PASSWORD`

4. **Reset Neo4j password:**
   ```bash
   # For Neo4j server
   neo4j-admin set-initial-password newpassword
   
   # Or delete the data directory to reset (WARNING: loses all data)
   rm -rf /path/to/neo4j/data
   ```

### Problem: "Authentication rate limit exceeded"

**Symptoms:**
```
ERROR: AuthenticationRateLimit - too many incorrect authentication attempts
```

**Solution:**
Wait 5-10 minutes for the rate limit to reset, then try again with the correct password.

### Problem: "Connection refused" or "Cannot connect"

**Symptoms:**
```
ERROR: ServiceUnavailable: Could not connect to bolt://localhost:7687
```

**Solutions:**

1. **Start Neo4j:**
   
   **Docker:**
   ```bash
   docker run --name neo4j-papers \
     -p 7474:7474 -p 7687:7687 \
     -e NEO4J_AUTH=neo4j/yourpassword \
     neo4j:latest
   ```
   
   **Neo4j Desktop:**
   - Open Neo4j Desktop
   - Start your database
   
   **System Service:**
   ```bash
   sudo systemctl start neo4j
   # or
   neo4j start
   ```

2. **Check ports:**
   ```bash
   # Check if Neo4j is listening
   netstat -tulpn | grep :7687
   lsof -i :7687
   ```

3. **Check firewall:**
   ```bash
   # Allow Neo4j ports
   sudo ufw allow 7474
   sudo ufw allow 7687
   ```

## Data Loading Issues

### Problem: "No PWC data directory found"

**Solution:**
```bash
# Download datasets first
python pwc_dataset_downloader.py

# Then run the loader
python pwc_offline_loader.py
```

### Problem: "Invalid JSON" warnings during data loading

**Symptoms:**
```
WARNING: Invalid JSON on line X: Expecting value
```

**Solution:**
This is normal - some lines in the data files may be malformed. The loader skips these automatically.

### Problem: "Error parsing paper/repository/dataset"

**Solution:**
1. Check if you have the latest data:
   ```bash
   python pwc_dataset_downloader.py --datasets papers links
   ```

2. Try with smaller limits:
   ```bash
   python pwc_offline_loader.py --paper-limit 10 --dataset-limit 5
   ```

## Performance Issues

### Problem: "Out of memory" during loading

**Solutions:**
1. Use smaller limits:
   ```bash
   python pwc_offline_loader.py --paper-limit 100 --dataset-limit 50
   ```

2. Process in batches:
   ```bash
   # Load datasets first
   python pwc_offline_loader.py --paper-limit 0 --dataset-limit 100
   
   # Then papers in batches
   python pwc_offline_loader.py --paper-limit 500 --dataset-limit 0
   ```

### Problem: Slow Neo4j operations

**Solutions:**
1. Ensure indexes are created (automatic)
2. Increase Neo4j memory:
   ```bash
   # In neo4j.conf
   dbms.memory.heap.initial_size=2G
   dbms.memory.heap.max_size=4G
   ```

## API Issues (Online Pipeline)

### Problem: "API request failed" or "Rate limited"

**Solutions:**
1. Increase delay between requests:
   ```bash
   # In pwc_loader.py, modify:
   self.request_delay = 2.0  # seconds
   ```

2. Use smaller limits:
   ```bash
   python pwc_loader.py --paper-limit 10 --repo-limit 20
   ```

## Testing and Debugging

### Test Data Parsing (No Neo4j Required)

```bash
python test_offline_data_only.py
```

### Test Neo4j Connection

```bash
python test_neo4j_connection.py bolt://localhost:7687 neo4j yourpassword
```

### Check Available Data

```bash
ls pwc-*/  # List downloaded files
python -c "
from pwc_offline_loader import PapersWithCodeOfflineLoader
loader = PapersWithCodeOfflineLoader('pwc-20250610')
print(loader.get_data_summary())
"
```

### Enable Debug Logging

```bash
# Add to your script
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Docker Setup for Testing

If you're having trouble with Neo4j setup, use Docker for quick testing:

```bash
# Start Neo4j in Docker
docker run --name neo4j-test \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/testpass \
  -d neo4j:latest

# Wait for startup (check logs)
docker logs neo4j-test

# Test connection
python test_neo4j_connection.py bolt://localhost:7687 neo4j testpass

# Run the pipeline
python pwc_offline_loader.py --neo4j-password testpass --paper-limit 10

# Clean up when done
docker stop neo4j-test
docker rm neo4j-test
```

## Common Command Patterns

### Development/Testing
```bash
# Quick test with minimal data
python pwc_dataset_downloader.py --datasets datasets
python pwc_offline_loader.py --neo4j-password test --dataset-limit 5 --no-repositories
```

### Production Loading
```bash
# Full dataset download
python pwc_dataset_downloader.py --cleanup

# Load with reasonable limits
python pwc_offline_loader.py --neo4j-password production --paper-limit 5000 --dataset-limit 1000
```

### CI/CD Testing
```bash
# Automated testing
docker run -d --name neo4j-ci -p 7687:7687 -e NEO4J_AUTH=neo4j/ci neo4j:latest
sleep 30  # Wait for startup
python test_offline_data_only.py
python pwc_offline_loader.py --neo4j-password ci --paper-limit 5
docker stop neo4j-ci && docker rm neo4j-ci
```

## Neo4j Warnings

### Problem: "UnknownRelationshipTypeWarning" about HAS_CODE

**Symptoms:**
```
WARNING: The provided relationship type is not in the database (missing: HAS_CODE)
```

**Solution:**
This warning is normal when starting with an empty Neo4j database. The system now:
- Checks if nodes exist before querying relationships
- Skips relationship queries when database is empty
- Handles missing relationships gracefully

**Fixed in version:** Latest version includes smart relationship querying.

### Problem: "UnknownLabelWarning" about node types

**Solution:**
Similar to relationship warnings, this happens with empty databases and is handled automatically.

## Getting Help

1. **Check logs:** Look for specific error messages
2. **Test components individually:** Use `test_neo4j_connection.py` and `test_offline_data_only.py`
3. **Test fixes:** Use `test_neo4j_fixes.py` to verify warning fixes
4. **Use help flags:** `python pwc_offline_loader.py --help`
5. **Check Neo4j Browser:** http://localhost:7474 for database status
6. **Verify data files:** Ensure PWC data was downloaded correctly