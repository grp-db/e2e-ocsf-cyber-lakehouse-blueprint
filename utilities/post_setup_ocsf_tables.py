"""
OCSF Tables Post-Pipeline Setup Script

Databricks Cyber Accelerator - Built by Databricks Professional Services

Run this notebook/script AFTER your first SDP pipeline run to:
1. Add liquid clustering to OCSF tables for query optimization

Note: SDP does not support ALTER TABLE in spark.sql() within pipeline definitions, 
so this must be run separately after the pipeline has populated the tables.

Usage:
  1. Run utilities/pre_setup_ocsf_tables.py FIRST (creates tables with minimal schema)
  2. Run your SDP pipeline (populates tables with data)
  3. Run this script in a Databricks notebook (adds liquid clustering)
  
This is a one-time setup - clustering persists after creation.
"""

from utilities.utils import CATALOG, DATABASES, OCSF_TABLES

# ============================================================================
# Add Liquid Clustering to OCSF Tables
# ============================================================================
# Liquid clustering optimizes queries filtering by time ranges (e.g., "last 7 days")
# Tables must exist and contain data before clustering can be added effectively

print("Adding liquid clustering to OCSF tables...")
print("(This optimizes time-based queries like 'last 7 days')\n")

tables_to_cluster = [
    (OCSF_TABLES['account_change'], "Account Change (3001)"),
    (OCSF_TABLES['authentication'], "Authentication (3002)"),
    (OCSF_TABLES['authorize_session'], "Authorize Session (3003)"),
    (OCSF_TABLES['entity_management'], "Entity Management (3004)"),
    (OCSF_TABLES['user_access'], "User Access (3005)"),
    (OCSF_TABLES['group_management'], "Group Management (3006)")
]

for table_name, description in tables_to_cluster:
    try:
        full_table_name = f"{CATALOG}.{DATABASES['ocsf']}.{table_name}"
        print(f"  Clustering {description} by (time)...")
        spark.sql(f"ALTER TABLE {full_table_name} CLUSTER BY (time)")
        print(f"  ✅ {table_name}")
    except Exception as e:
        error_msg = str(e).lower()
        if "does not exist" in error_msg:
            print(f"  ⚠️  {table_name} - Table doesn't exist. Run pre_setup_ocsf_tables.py first.")
        elif "already clustered" in error_msg or "liquid" in error_msg:
            print(f"  ℹ️  {table_name} - Already clustered")
        else:
            print(f"  ❌ {table_name} - Error: {e}")

print("\n" + "="*70)
print("Post-setup complete! OCSF tables are optimized for time-based queries.")
print("="*70)

# ============================================================================
# VERIFICATION: Show Table Properties
# ============================================================================
print("\nVerifying liquid clustering configuration...\n")

for table_name, description in tables_to_cluster:
    try:
        full_table_name = f"{CATALOG}.{DATABASES['ocsf']}.{table_name}"
        result = spark.sql(f"DESCRIBE DETAIL {full_table_name}").select("name", "clusteringColumns").collect()
        if result:
            clustering_cols = result[0]["clusteringColumns"]
            if clustering_cols:
                print(f"✅ {table_name}: CLUSTER BY {clustering_cols}")
            else:
                print(f"⚠️  {table_name}: No clustering configured")
    except Exception as e:
        print(f"❌ {table_name}: {e}")

print("\n" + "="*70)
print("Query optimization tips:")
print("  • Use WHERE time >= ... for efficient time-range filtering")
print("  • Liquid clustering automatically optimizes file layout")
print("  • No manual OPTIMIZE commands needed!")
print("="*70)

