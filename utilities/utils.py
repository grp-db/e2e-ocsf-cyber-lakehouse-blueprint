"""
Configuration module for E2E Cybersecurity Lakehouse Pipeline

Databricks Cyber Accelerator - Built by Databricks Professional Services

Centralizes file paths, table names, and other constants
"""

# CHANGE ME! Set your Unity Catalog name
CATALOG = "grp"

# CHANGE ME! Set your database names per source and OCSF
DATABASES = {
    "github": "github",
    "slack": "slack",
    "atlassian": "atlassian",
    "ocsf": "ocsf"
}

# CHANGE ME! Set your Volume path where raw audit log files are stored
RAW_LOGS_BASE_PATH = "/Volumes/grp/logs/logs"

# CHANGE ME! Set your Volume path where Auto Loader schemas are stored
AL_SCHEMAS_BASE_PATH = "/Volumes/grp/schemas/schemas"

# CHANGE ME! Set your Volume path where SDP sink checkpoints are stored
SDP_CP_SINKS_BASE_PATH = "/Volumes/grp/checkpoints/checkpoints"

FILE_PATHS = {
    "github": f"{RAW_LOGS_BASE_PATH}/github_audit_logs/*.json",
    "slack": f"{RAW_LOGS_BASE_PATH}/slack_audit_logs/*.json",
    "atlassian": f"{RAW_LOGS_BASE_PATH}/atlassian_audit_logs/*.json"
}

SCHEMA_PATHS = {
    "github": f"{AL_SCHEMAS_BASE_PATH}/github_audit_logs",
    "slack": f"{AL_SCHEMAS_BASE_PATH}/slack_audit_logs",
    "atlassian": f"{AL_SCHEMAS_BASE_PATH}/atlassian_audit_logs"
}

BRONZE_TABLES = {
    "github": "github_audit_logs_brz",
    "slack": "slack_audit_logs_brz",
    "atlassian": "atlassian_audit_logs_brz"
}

SILVER_TABLES = {
    "github": "github_audit_logs_slv",
    "slack": "slack_audit_logs_slv",
    "atlassian": "atlassian_audit_logs_slv"
}

OCSF_TABLES = {
    "account_change": "ocsf_iam_account_change",
    "authentication": "ocsf_iam_authentication",
    "authorize_session": "ocsf_iam_authorize_session",
    "entity_management": "ocsf_iam_entity_management",
    "user_access": "ocsf_iam_user_access",
    "group_management": "ocsf_iam_group_management"
}

SOURCE_NAMES = {
    "github": "github",
    "slack": "slack",
    "atlassian": "atlassian"
}

SOURCE_TYPE = "audit_logs"

OCSF_CLASS_UIDS = {
    "account_change": 3001,
    "authentication": 3002,
    "authorize_session": 3003,
    "entity_management": 3004,
    "user_access": 3005,
    "group_management": 3006
}

OCSF_CATEGORY_UID = 3
OCSF_CATEGORY_NAME = "Identity & Access Management"
OCSF_VERSION = "1.7.0"

# Standard table properties for all Bronze/Silver tables
# Enables Delta optimizations, variant support, and deletion vectors
TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "pipelines.autoOptimize.managed": "true",
    "delta.minWriterVersion": "7",
    "delta.enableDeletionVectors": "true",
    "delta.minReaderVersion": "3",
    "delta.feature.variantType-preview": "supported",
    "delta.feature.variantShredding-preview": "supported",
    "delta.feature.deletionVectors": "supported",
    "delta.feature.invariants": "supported"
}
