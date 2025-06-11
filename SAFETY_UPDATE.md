# Important Safety Update: Selective Clearing + Dangerous Drop Flags

## 🛡️ What Changed

**Safe flags** `--clear-all` and `--clear-target` now use **selective clearing** instead of wiping entire databases.

**New dangerous flags** `--drop-all` and `--drop-all-target` added for complete database destruction (with confirmation).

### Before (Dangerous ⚠️):
- `--clear-all`: Deleted **ALL** data from the local Neo4j database
- `--clear-target`: Deleted **ALL** data from the target Neo4j database

### After (Safe ✅ + Dangerous ⚠️):
- `--clear-all`: Deletes only **Papers with Code** data from the local database ✅ **SAFE**
- `--clear-target`: Deletes only **Papers with Code** data from the target database ✅ **SAFE**
- `--drop-all`: Deletes **ALL** data from the local database ⚠️ **DANGEROUS** (requires confirmation)
- `--drop-all-target`: Deletes **ALL** data from the target database ⚠️ **DANGEROUS** (requires confirmation)

## 🎯 What Gets Cleared

### Papers with Code Data Types (Removed):
- **Node types**: `Paper`, `Repository`, `Dataset`, `Task`, `Author`
- **Relationship types**: `AUTHORED`, `HAS_CODE`, `USES_DATASET`, `ADDRESSES_TASK`
- **PWC-specific indexes**: `paper_id_index`, `repo_url_index`, etc.

### Everything Else (Preserved):
- Custom node types (e.g., `User`, `Company`, `Product`)
- Custom relationships (e.g., `WORKS_FOR`, `OWNS`, `MANAGES`)
- Custom indexes not related to PWC data
- All other data schemas and relationships

## 📊 Example Scenarios

### Mixed Database Example:
```
Before clearing:
- 10,000 Paper nodes (PWC data)
- 500 User nodes (custom data)
- 50 Company nodes (custom data)
- 15,000 AUTHORED relationships (PWC data)
- 200 WORKS_FOR relationships (custom data)

After --clear-all or --clear-target:
- 0 Paper nodes ✅ (cleared)
- 500 User nodes ✅ (preserved)  
- 50 Company nodes ✅ (preserved)
- 0 AUTHORED relationships ✅ (cleared)
- 200 WORKS_FOR relationships ✅ (preserved)
```

## 🚀 Usage Examples

### Safe Local Clearing:
```bash
# Safe: Only clears PWC data from local database
python pwc_offline_loader.py pwc-20250610 --clear-all --paper-limit 1000
```

### Safe Target Clearing:
```bash
# Safe: Only clears PWC data from target database
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://target-server:7687 \
    --clear-target \
    --paper-limit 1000
```

### ⚠️ Dangerous Local Drop (Complete Wipe):
```bash
# DANGEROUS: Completely wipes local database (requires confirmation)
python pwc_offline_loader.py pwc-20250610 --drop-all --paper-limit 1000
# User must type: "DELETE ALL DATA" to confirm
```

### ⚠️ Dangerous Target Drop (Complete Wipe):
```bash
# DANGEROUS: Completely wipes target database (requires confirmation)
python pwc_offline_loader.py pwc-20250610 \
    --new-neo4j-uri bolt://target-server:7687 \
    --drop-all-target \
    --paper-limit 1000
# User must type: "DELETE ALL DATA" to confirm
```

### PWC-Only Comparison:
```bash
# Compare only PWC data types (useful for mixed databases)
python neo4j_diff.py \
    --source-uri bolt://localhost:7687 \
    --target-uri bolt://target-server:7687 \
    --pwc-only
```

## ⚠️ Confirmation Process for Dangerous Operations

When using `--drop-all` or `--drop-all-target`, you'll see this warning:

```
================================================================================
🚨 DANGER: DESTRUCTIVE OPERATION WARNING 🚨
================================================================================
Operation: DROP ALL LOCAL DATA
Target: Local Neo4j database
This will DELETE ALL DATA in the specified Neo4j database!
This action is IRREVERSIBLE and will destroy:
  • All nodes of every type
  • All relationships
  • All indexes
  • All constraints
  • Everything in the database
================================================================================
To confirm this destructive operation, type exactly: DELETE ALL DATA
Confirmation: _
```

You **MUST** type exactly `DELETE ALL DATA` to proceed. Any other input cancels the operation.

## ✅ Migration Guide

If you were using the old `--clear-all` or `--clear-target` flags:

1. **No code changes needed** - the flags work the same way
2. **Behavior is now safer** - only PWC data is cleared
3. **Test with mixed data** - verify other data types are preserved
4. **Use `--pwc-only` for diffs** - when comparing mixed databases
5. **Use new dangerous flags when needed** - `--drop-all` or `--drop-all-target` for complete wipes

## 🧪 Testing

Run the selective clearing tests to verify the behavior:

```bash
python test_selective_clearing.py
```

This will:
- Create mixed data (PWC + custom types)
- Test selective clearing
- Verify PWC data is removed and custom data is preserved

## 🔒 Production Safety

These changes make the loader **production-safe** for environments where:
- Neo4j contains multiple data schemas
- PWC data coexists with business data
- Selective updates are needed without affecting other systems
- Database downtime must be minimized

The selective clearing ensures that only Papers with Code data is affected, making it safe to use on shared databases.