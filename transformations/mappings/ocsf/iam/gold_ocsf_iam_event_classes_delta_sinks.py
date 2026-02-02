"""
OCSF IAM Gold Layer - Unified Audit Logs

This file creates 6 unified OCSF IAM tables (Gold layer) by combining data from 
GitHub, Slack, and Atlassian Silver tables using Spark Declarative Pipelines.

Architecture:
- Reads from: Silver tables in github, slack, atlassian databases
- Writes to: 6 unified OCSF Delta tables in ocsf database (via SDP sinks)
- Pattern: Uses SDP sinks + append_flow to stream multiple sources into each OCSF table

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
- SDP sinks enable multiple pipelines to write to the same OCSF tables
- Tables auto-create on first write with schema evolution enabled
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

# Set catalog and database context for OCSF tables
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE DATABASE {DATABASES['ocsf']}")


# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/account_change?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['account_change']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['account_change']}_sink",
        "mergeSchema": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/authentication?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['authentication']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authentication']}_sink",
        "mergeSchema": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/authorize_session?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['authorize_session']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['authorize_session']}_sink",
        "mergeSchema": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/entity_management?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['entity_management']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['entity_management']}_sink",
        "mergeSchema": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/user_access?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['user_access']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['user_access']}_sink",
        "mergeSchema": "true"
    }
)

# OCSF Schema: https://schema.ocsf.io/1.7.0/classes/group_management?extensions=
sdp.create_sink(
    name=f"{OCSF_TABLES['group_management']}_sink",
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['group_management']}_sink",
        "mergeSchema": "true"
    }
)


# GitHub → OCSF Append Flows

@sdp.append_flow(name="github_account_change", target=f"{OCSF_TABLES['account_change']}_sink")
def github_account_change():
    """GitHub user lifecycle → Account Change (3001)"""
    return transform_github_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_authentication", target=f"{OCSF_TABLES['authentication']}_sink")
def github_authentication():
    """GitHub login/logout → Authentication (3002)"""
    return transform_github_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_authorize_session", target=f"{OCSF_TABLES['authorize_session']}_sink")
def github_authorize_session():
    """GitHub repo access → Authorize Session (3003)"""
    return transform_github_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_group_management", target=f"{OCSF_TABLES['group_management']}_sink")
def github_group_management():
    """GitHub team operations → Group Management (3006)"""
    return transform_github_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


@sdp.append_flow(name="github_user_access", target=f"{OCSF_TABLES['user_access']}_sink")
def github_user_access():
    """GitHub org member management → User Access (3005)"""
    return transform_github_to_user_access(
        spark.readStream.table(f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}")
    )


# Slack → OCSF Append Flows

@sdp.append_flow(name="slack_account_change", target=f"{OCSF_TABLES['account_change']}_sink")
def slack_account_change():
    """Slack user lifecycle → Account Change (3001)"""
    return transform_slack_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_authentication", target=f"{OCSF_TABLES['authentication']}_sink")
def slack_authentication():
    """Slack login/logout → Authentication (3002)"""
    return transform_slack_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_authorize_session", target=f"{OCSF_TABLES['authorize_session']}_sink")
def slack_authorize_session():
    """Slack workspace settings → Authorize Session (3003)"""
    return transform_slack_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_group_management", target=f"{OCSF_TABLES['group_management']}_sink")
def slack_group_management():
    """Slack channel/usergroup ops → Group Management (3006)"""
    return transform_slack_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


@sdp.append_flow(name="slack_user_access", target=f"{OCSF_TABLES['user_access']}_sink")
def slack_user_access():
    """Slack app/guest access → User Access (3005)"""
    return transform_slack_to_user_access(
        spark.readStream.table(f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}")
    )


# Atlassian → OCSF Append Flows

@sdp.append_flow(name="atlassian_account_change", target=f"{OCSF_TABLES['account_change']}_sink")
def atlassian_account_change():
    """Atlassian user lifecycle → Account Change (3001)"""
    return transform_atlassian_to_account_change(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_authentication", target=f"{OCSF_TABLES['authentication']}_sink")
def atlassian_authentication():
    """Atlassian login/logout → Authentication (3002)"""
    return transform_atlassian_to_authentication(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_authorize_session", target=f"{OCSF_TABLES['authorize_session']}_sink")
def atlassian_authorize_session():
    """Atlassian permission/role ops → Authorize Session (3003)"""
    return transform_atlassian_to_authorize_session(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_entity_management", target=f"{OCSF_TABLES['entity_management']}_sink")
def atlassian_entity_management():
    """Atlassian workspace/project mgmt → Entity Management (3004)"""
    return transform_atlassian_to_entity_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )


@sdp.append_flow(name="atlassian_group_management", target=f"{OCSF_TABLES['group_management']}_sink")
def atlassian_group_management():
    """Atlassian group operations → Group Management (3006)"""
    return transform_atlassian_to_group_management(
        spark.readStream.table(f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}")
    )

