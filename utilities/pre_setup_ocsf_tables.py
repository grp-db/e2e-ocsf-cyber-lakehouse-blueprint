"""
Pre-Pipeline Setup Script

Databricks Cyber Accelerator - Built by Databricks Professional Services

Run this notebook/script BEFORE your first SDP pipeline run to:
1. Create all required databases (github, slack, atlassian, ocsf)
2. Create OCSF Gold tables with minimal schema (time column only)
   - Prevents concurrent metadata update conflicts when multiple sources write simultaneously
   - Full schema will be auto-evolved by Delta sinks with mergeSchema: true

Note: SDP does not support DDL commands (CREATE DATABASE, CREATE TABLE) in spark.sql() 
within pipeline definitions, so these must be run separately before pipelines start.

Table Creation Strategy:
- Bronze/Silver tables: Auto-created by @sdp.table decorators when pipelines run
- Gold OCSF tables: Created here with minimal schema (time column), then auto-evolved by Delta sinks
  (mergeSchema: true allows dynamic column addition on first append flow write)
  (time column prevents race conditions when GitHub/Slack/Atlassian write concurrently)

Usage:
  1. Run this script in a Databricks notebook FIRST
  2. Run bronze/silver pipelines (auto-creates bronze/silver tables)
  3. Run gold pipeline (populates OCSF tables, auto-adds columns via mergeSchema)
  4. Run utilities/post_setup_ocsf_tables.py to add liquid clustering
"""

from utilities.utils import CATALOG, DATABASES, OCSF_TABLES

print("="*70)
print("OCSF Pre-Setup Script")
print("="*70)
print(f"Catalog: {CATALOG}")
print(f"OCSF Database: {DATABASES['ocsf']}")
print(f"Full OCSF Database Name: {CATALOG}.{DATABASES['ocsf']}")
print("="*70)
print()

# Verify catalog exists
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    print(f"✅ Catalog '{CATALOG}' exists and is accessible\n")
except Exception as e:
    print(f"❌ ERROR: Cannot access catalog '{CATALOG}'")
    print(f"   Error: {e}")
    print(f"   Please verify the catalog name in utilities/utils.py")
    raise

# ============================================================================
# STEP 1: Create All Databases
# ============================================================================
print("Creating databases...\n")

databases_to_create = [
    (DATABASES['github'], "GitHub source database"),
    (DATABASES['slack'], "Slack source database"),
    (DATABASES['atlassian'], "Atlassian source database"),
    (DATABASES['ocsf'], "OCSF Gold database")
]

for db_name, description in databases_to_create:
    try:
        full_db_name = f"{CATALOG}.{db_name}"
        print(f"  Creating {description}: {full_db_name}...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")
        print(f"  ✅ {db_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ℹ️  {db_name} - Already exists")
        else:
            print(f"  ❌ {db_name} - Error: {e}")
            raise

print()

# ============================================================================
# STEP 2: Create OCSF Tables with Minimal Schema
# ============================================================================
# Create tables with minimal schema (time column only - required by OCSF spec)
# This prevents concurrent metadata update conflicts when multiple append flows
# (GitHub, Slack, Atlassian) try to add columns simultaneously on first write
# mergeSchema: true in the Delta sinks will auto-add remaining columns safely

print("Creating OCSF tables with minimal schema...")
print("(Full schema will be auto-evolved by Delta sinks on first write)\n")

tables_to_create = [
    (OCSF_TABLES['account_change'], "Account Change (3001)"),
    (OCSF_TABLES['authentication'], "Authentication (3002)"),
    (OCSF_TABLES['authorize_session'], "Authorize Session (3003)"),
    (OCSF_TABLES['entity_management'], "Entity Management (3004)"),
    (OCSF_TABLES['user_access'], "User Access (3005)"),
    (OCSF_TABLES['group_management'], "Group Management (3006)")
]

for table_name, description in tables_to_create:
    try:
        full_table_name = f"{CATALOG}.{DATABASES['ocsf']}.{table_name}"
        print(f"  Creating {description}...")
        print(f"    Full name: {full_table_name}")
        # Create with minimal schema to avoid concurrent metadata conflicts
        # mergeSchema will add remaining columns without race conditions
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                time TIMESTAMP COMMENT 'Required OCSF field - prevents concurrent schema conflicts'
            ) USING delta
        """)
        # Verify table was created
        table_exists = spark.catalog.tableExists(f"{DATABASES['ocsf']}.{table_name}")
        if table_exists:
            print(f"  ✅ {table_name} - Created and verified")
        else:
            print(f"  ⚠️  {table_name} - CREATE succeeded but table not found in catalog")
    except Exception as e:
        print(f"  ❌ {table_name} - Error: {e}")
        raise

print("\n" + "="*70)
print("Pre-setup complete! All databases and OCSF tables are ready.")
print("="*70)
print("\nNext steps:")
print("  1. Run Bronze pipelines (github, slack, atlassian) - auto-creates bronze tables")
print("  2. Run Silver pipelines (github, slack, atlassian) - auto-creates silver tables")
print("  3. Run Gold pipeline (OCSF) - populates OCSF tables (mergeSchema adds columns)")
print("  4. Run utilities/post_setup_ocsf_tables.py to add liquid clustering")

# ============================================================================
# VERIFICATION: Show Created Databases and Tables
# ============================================================================
print("\nVerifying database creation...\n")
for db_name, description in databases_to_create:
    full_db_name = f"{CATALOG}.{db_name}"
    try:
        spark.sql(f"USE {full_db_name}")
        print(f"✅ {full_db_name} - Ready")
    except Exception as e:
        print(f"❌ {full_db_name} - Error: {e}")

print("\nVerifying OCSF table creation...\n")
print(f"Looking for tables in: {CATALOG}.{DATABASES['ocsf']}")
try:
    result = spark.sql(f"SHOW TABLES IN {CATALOG}.{DATABASES['ocsf']}").collect()
    if result:
        print(f"✅ Found {len(result)} table(s) in {CATALOG}.{DATABASES['ocsf']}:")
        for row in result:
            print(f"  ✅ {row['database']}.{row['tableName']}")
    else:
        print(f"⚠️  No tables found in {CATALOG}.{DATABASES['ocsf']}")
        print(f"    This might indicate the tables weren't created.")
except Exception as e:
    print(f"❌ Error querying tables: {e}")
    print(f"    The database {CATALOG}.{DATABASES['ocsf']} might not exist.")

print("\n" + "="*70)
print("OCSF tables created with minimal schema (time column).")
print("This prevents concurrent metadata conflicts during parallel writes.")
print("Full schemas will be auto-evolved by Delta sinks via mergeSchema: true.")
print("Bronze/Silver tables will be auto-created by their SDP pipelines.")
print("="*70)

