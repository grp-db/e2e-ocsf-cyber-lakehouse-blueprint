"""
OCSF IAM Gold Layer - Unified Audit Logs

This file creates 6 unified OCSF IAM tables (Gold layer) by combining data from 
GitHub, Slack, and Atlassian Silver tables using Spark Declarative Pipelines.

Architecture:
- Reads from: Silver tables in github, slack, atlassian databases
- Writes to: 6 unified OCSF tables in ocsf database
- Pattern: Uses append_flow to stream multiple sources into each OCSF table

OCSF Event Classes (IAM Category UID 3) - Source Coverage:
1. Account Change (3001) - GitHub ✅, Slack ✅, Atlassian ✅
2. Authentication (3002) - GitHub ✅, Slack ✅, Atlassian ✅
3. Authorize Session (3003) - GitHub ✅, Slack ✅, Atlassian ✅
4. Entity Management (3004) - Atlassian ONLY ✅
5. User Access (3005) - GitHub ✅, Slack ✅
6. Group Management (3006) - GitHub ✅, Slack ✅, Atlassian ✅

Notes:
- Entity Management (3004): Only Atlassian contributes (workspace/project/webhook lifecycle)
- User Access (3005): GitHub and Slack only (Atlassian permissions → Authorize Session)
- Total append flows: 15 (5 GitHub + 5 Slack + 5 Atlassian)
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
    transform_github_to_user_access_management,
    transform_github_to_group_management
)

from transformations.mappings.ocsf.iam.gold_slack_audit_logs import (
    transform_slack_to_account_change,
    transform_slack_to_authentication,
    transform_slack_to_authorize_session,
    transform_slack_to_user_access_management,
    transform_slack_to_group_management
)

from transformations.mappings.ocsf.iam.gold_atlassian_audit_logs import (
    transform_atlassian_to_account_change,
    transform_atlassian_to_authentication,
    transform_atlassian_to_authorize_session,
    transform_atlassian_to_entity_management,
    transform_atlassian_to_group_management
)

# Set catalog and database context for OCSF tables
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE DATABASE {DATABASES['ocsf']}")


# OCSF Schema: https://schema.ocsf.io/1.3.0/classes/account_change?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["account_change"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - Account Change ({OCSF_CLASS_UIDS['account_change']}) - Unified from GitHub, Slack, Atlassian",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.3.0/classes/authentication?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["authentication"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - Authentication ({OCSF_CLASS_UIDS['authentication']}) - Unified from GitHub, Slack, Atlassian",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.3.0/classes/authorize_session?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["authorize_session"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - Authorize Session ({OCSF_CLASS_UIDS['authorize_session']}) - Unified from GitHub, Slack, Atlassian",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.3.0/classes/entity_management?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["entity_management"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - Entity Management ({OCSF_CLASS_UIDS['entity_management']}) - Atlassian only",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/user_access?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["user_access"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - User Access ({OCSF_CLASS_UIDS['user_access']}) - Unified from GitHub, Slack",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.3.0/classes/group_management?extensions=
sdp.create_streaming_table(
    OCSF_TABLES["group_management"],
    comment=f"OCSF {OCSF_VERSION} {OCSF_CATEGORY_NAME} - Group Management ({OCSF_CLASS_UIDS['group_management']}) - Unified from GitHub, Slack, Atlassian",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.schemaEvolution": "true"
    }
)


# GitHub → OCSF Append Flows

@sdp.append_flow(target=OCSF_TABLES["account_change"])
def github_account_change():
    """GitHub user lifecycle → Account Change (3001)"""
    return transform_github_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authentication"])
def github_authentication():
    """GitHub login/logout → Authentication (3002)"""
    return transform_github_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authorize_session"])
def github_authorize_session():
    """GitHub repo access → Authorize Session (3003)"""
    return transform_github_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(target=OCSF_TABLES["group_management"])
def github_group_management():
    """GitHub team operations → Group Management (3006)"""
    return transform_github_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(target=OCSF_TABLES["user_access"])
def github_user_access():
    """GitHub org member management → User Access (3005)"""
    return transform_github_to_user_access_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


# Slack → OCSF Append Flows

@sdp.append_flow(target=OCSF_TABLES["account_change"])
def slack_account_change():
    """Slack user lifecycle → Account Change (3001)"""
    return transform_slack_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authentication"])
def slack_authentication():
    """Slack login/logout → Authentication (3002)"""
    return transform_slack_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authorize_session"])
def slack_authorize_session():
    """Slack workspace settings → Authorize Session (3003)"""
    return transform_slack_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(target=OCSF_TABLES["group_management"])
def slack_group_management():
    """Slack channel/usergroup ops → Group Management (3006)"""
    return transform_slack_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(target=OCSF_TABLES["user_access"])
def slack_user_access():
    """Slack app/guest access → User Access (3005)"""
    return transform_slack_to_user_access_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


# Atlassian → OCSF Append Flows

@sdp.append_flow(target=OCSF_TABLES["account_change"])
def atlassian_account_change():
    """Atlassian user lifecycle → Account Change (3001)"""
    return transform_atlassian_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authentication"])
def atlassian_authentication():
    """Atlassian login/logout → Authentication (3002)"""
    return transform_atlassian_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(target=OCSF_TABLES["authorize_session"])
def atlassian_authorize_session():
    """Atlassian permission/role ops → Authorize Session (3003)"""
    return transform_atlassian_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(target=OCSF_TABLES["entity_management"])
def atlassian_entity_management():
    """Atlassian workspace/project mgmt → Entity Management (3004)"""
    return transform_atlassian_to_entity_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(target=OCSF_TABLES["group_management"])
def atlassian_group_management():
    """Atlassian group operations → Group Management (3006)"""
    return transform_atlassian_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )

