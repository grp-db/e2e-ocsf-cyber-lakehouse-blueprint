"""
OCSF IAM Gold Layer - Unified Audit Logs

This file creates 6 UNIFIED OCSF IAM tables (Gold layer) by combining data from 
GitHub, Slack, and Atlassian Silver tables using Spark Declarative Pipelines.

Architecture:
- Reads from: Silver tables in github, slack, atlassian databases
- Writes to: 6 UNIFIED OCSF Delta tables in ocsf database (each table contains all sources)
- Pattern: 6 sinks + 15 append flows → 6 unified tables

OCSF Event Classes (IAM Category UID 3) - Source Coverage:
1. Account Change (3001) - GitHub ✅, Slack ✅, Atlassian ✅ → 1 unified table
2. Authentication (3002) - GitHub ✅, Slack ✅, Atlassian ✅ → 1 unified table
3. Authorize Session (3003) - GitHub ✅, Slack ✅, Atlassian ✅ → 1 unified table
4. Entity Management (3004) - Atlassian ONLY ✅ → 1 unified table
5. User Access (3005) - GitHub ✅, Slack ✅ → 1 unified table
6. Group Management (3006) - GitHub ✅, Slack ✅, Atlassian ✅ → 1 unified table

Sink Architecture (Multi-Append to Unified Tables):
- Total sinks: 6 (one per OCSF class)
- Total tables: 6 (one per OCSF class - contains data from ALL sources)
- Total append flows: 15 (5 GitHub + 5 Slack + 5 Atlassian)
- Result: Query one table, get all sources (use _source column to filter)

Example:
  ocsf_iam_account_change (ONE SINK, ONE TABLE):
    ← github_account_change (append flow)
    ← slack_account_change (append flow)
    ← atlassian_account_change (append flow)

Notes:
- Entity Management (3004): Only Atlassian contributes (workspace/project/webhook lifecycle)
- User Access (3005): GitHub and Slack only (Atlassian permissions → Authorize Session)
- CHANGE ME! Update CATALOG and DATABASES in utilities/utils.py
"""

from pyspark import pipelines as sdp

from utilities.utils import (
    CATALOG, DATABASES, SILVER_TABLES, OCSF_TABLES,
    OCSF_CLASS_UIDS, OCSF_VERSION, OCSF_CATEGORY_NAME
)

from transformations.mappings.ocsf.iam.gold_github_audit_logs import (
    transform_github_to_account_change,
    transform_github_to_authentication,
    transform_github_to_authorize_session,
    transform_github_to_user_access,
    transform_github_to_group_management
)

from transformations.mappings.ocsf.iam.gold_slack_audit_logs import (
    transform_slack_to_account_change,
    transform_slack_to_authentication,
    transform_slack_to_authorize_session,
    transform_slack_to_user_access,
    transform_slack_to_group_management
)

from transformations.mappings.ocsf.iam.gold_atlassian_audit_logs import (
    transform_atlassian_to_account_change,
    transform_atlassian_to_authentication,
    transform_atlassian_to_authorize_session,
    transform_atlassian_to_entity_management,
    transform_atlassian_to_group_management
)

# Note: SDP does not support DDL commands (CREATE DATABASE, CREATE TABLE, ALTER TABLE, etc.) 
# in spark.sql() API. Run these setup scripts in Databricks notebooks:
#   1. BEFORE pipelines: utilities/pre_setup_ocsf_tables.py (creates all databases + OCSF tables with minimal schema)
#   2. RUN Bronze/Silver pipelines (tables auto-created by SDP)
#   3. RUN THIS Gold pipeline (populates OCSF tables with data)
#   4. AFTER pipelines: utilities/post_setup_ocsf_tables.py (adds liquid clustering)


# ============================================================================
# SINK DEFINITIONS: 6 Unified OCSF Tables
# ============================================================================
# Architecture: 6 sinks → 6 unified tables (multiple append flows per sink)
#
# Each OCSF class has ONE sink, and multiple append flows (from different sources)
# write to that single sink. The sink writes all data to one unified Delta table.
#
# Example: ocsf_iam_account_change (ONE SINK, ONE TABLE)
#   ← github_account_change (append flow)
#   ← slack_account_change (append flow)
#   ← atlassian_account_change (append flow)
#
# Result: SELECT * FROM ocsf_iam_account_change returns data from all sources!
#
# Note on mergeSchema: "true"
# ---------------------------
# For DEMO/POC purposes, we enable automatic schema evolution via mergeSchema.
# This allows different sources (GitHub, Slack, Atlassian) to write to the same
# table even if they have slightly different column structures (e.g., one source
# populates a field that another doesn't).
#
# ⚠️  PRODUCTION RECOMMENDATION:
# For production deployments, consider creating OCSF tables with explicit, strict
# schemas (DDL statements) to prevent schema drift and ensure data consistency
# across all sources. This approach:
#   - Enforces uniform field names, types, and nullability across all sources
#   - Prevents accidental column additions from source-specific transformations
#   - Ensures downstream consumers have predictable, stable schemas
#   - Enables better data governance and quality controls
#
# Trade-off: Strict schemas require more upfront planning but provide better
# long-term data quality and consistency. Demo mode (mergeSchema) is faster to
# prototype but may lead to schema drift over time.
# ============================================================================

# Account Change (3001) - Multiple sources write to this sink
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/account_change?extensions=
sdp.create_sink(
    name=OCSF_TABLES['account_change'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['account_change']}",
        "mergeSchema": "true"
    }
)

# Authentication (3002) - Multiple sources write to this sink
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/authentication?extensions=
sdp.create_sink(
    name=OCSF_TABLES['authentication'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authentication']}",
        "mergeSchema": "true"
    }
)

# Authorize Session (3003) - Multiple sources write to this sink
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/authorize_session?extensions=
sdp.create_sink(
    name=OCSF_TABLES['authorize_session'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authorize_session']}",
        "mergeSchema": "true"
    }
)

# Entity Management (3004) - Atlassian only
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/entity_management?extensions=
sdp.create_sink(
    name=OCSF_TABLES['entity_management'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['entity_management']}",
        "mergeSchema": "true"
    }
)

# User Access (3005) - Multiple sources write to this sink
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/user_access?extensions=
sdp.create_sink(
    name=OCSF_TABLES['user_access'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['user_access']}",
        "mergeSchema": "true"
    }
)

# Group Management (3006) - Multiple sources write to this sink
# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/group_management?extensions=
sdp.create_sink(
    name=OCSF_TABLES['group_management'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['group_management']}",
        "mergeSchema": "true"
    }
)


# GitHub → OCSF Append Flows

@sdp.append_flow(name="github_account_change", target=OCSF_TABLES['account_change'])
def github_account_change():
    """GitHub user lifecycle → Account Change (3001)"""
    return transform_github_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_authentication", target=OCSF_TABLES['authentication'])
def github_authentication():
    """GitHub login/logout → Authentication (3002)"""
    return transform_github_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_authorize_session", target=OCSF_TABLES['authorize_session'])
def github_authorize_session():
    """GitHub repo access → Authorize Session (3003)"""
    return transform_github_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_group_management", target=OCSF_TABLES['group_management'])
def github_group_management():
    """GitHub team operations → Group Management (3006)"""
    return transform_github_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_user_access", target=OCSF_TABLES['user_access'])
def github_user_access():
    """GitHub org member management → User Access (3005)"""
    return transform_github_to_user_access(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


# Slack → OCSF Append Flows

@sdp.append_flow(name="slack_account_change", target=OCSF_TABLES['account_change'])
def slack_account_change():
    """Slack user lifecycle → Account Change (3001)"""
    return transform_slack_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_authentication", target=OCSF_TABLES['authentication'])
def slack_authentication():
    """Slack login/logout → Authentication (3002)"""
    return transform_slack_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_authorize_session", target=OCSF_TABLES['authorize_session'])
def slack_authorize_session():
    """Slack workspace settings → Authorize Session (3003)"""
    return transform_slack_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_group_management", target=OCSF_TABLES['group_management'])
def slack_group_management():
    """Slack channel/usergroup ops → Group Management (3006)"""
    return transform_slack_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_user_access", target=OCSF_TABLES['user_access'])
def slack_user_access():
    """Slack app/guest access → User Access (3005)"""
    return transform_slack_to_user_access(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


# Atlassian → OCSF Append Flows

@sdp.append_flow(name="atlassian_account_change", target=OCSF_TABLES['account_change'])
def atlassian_account_change():
    """Atlassian user lifecycle → Account Change (3001)"""
    return transform_atlassian_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_authentication", target=OCSF_TABLES['authentication'])
def atlassian_authentication():
    """Atlassian login/logout → Authentication (3002)"""
    return transform_atlassian_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_authorize_session", target=OCSF_TABLES['authorize_session'])
def atlassian_authorize_session():
    """Atlassian permission/role ops → Authorize Session (3003)"""
    return transform_atlassian_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_entity_management", target=OCSF_TABLES['entity_management'])
def atlassian_entity_management():
    """Atlassian workspace/project mgmt → Entity Management (3004)"""
    return transform_atlassian_to_entity_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_group_management", target=OCSF_TABLES['group_management'])
def atlassian_group_management():
    """Atlassian group operations → Group Management (3006)"""
    return transform_atlassian_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


# ============================================================================
# LIQUID CLUSTERING (Post-Pipeline Setup)
# ============================================================================
# SDP does not support ALTER TABLE in spark.sql() API. 
# To add liquid clustering to optimize time-based queries, run the post-setup script
# in a Databricks notebook AFTER the first pipeline run (tables must exist first):
#
#   %run ./utilities/post_setup_ocsf_tables
#
# Or manually execute these commands in a notebook:
#
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['account_change']} CLUSTER BY (time)")
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authentication']} CLUSTER BY (time)")
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authorize_session']} CLUSTER BY (time)")
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['entity_management']} CLUSTER BY (time)")
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['user_access']} CLUSTER BY (time)")
# spark.sql(f"ALTER TABLE {CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['group_management']} CLUSTER BY (time)")
