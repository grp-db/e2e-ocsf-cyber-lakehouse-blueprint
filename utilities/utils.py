"""
Configuration module for E2E Cybersecurity Lakehouse Pipeline
Centralizes file paths, table names, and other constants
"""

CATALOG = "grp"

DATABASES = {
    "github": "github",
    "slack": "slack",
    "atlassian": "atlassian",
    "ocsf": "ocsf"
}

RAW_LOGS_BASE_PATH = "/Volumes/grp/logs/logs"

FILE_PATHS = {
    "github": f"{RAW_LOGS_BASE_PATH}/github_audit_logs/*.json",
    "slack": f"{RAW_LOGS_BASE_PATH}/slack_audit_logs/*.json",
    "atlassian": f"{RAW_LOGS_BASE_PATH}/atlassian_audit_logs/*.json"
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
    "user_access_management": "ocsf_iam_user_access_management",
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
    "user_access_management": 3005,
    "group_management": 3006
}

OCSF_CATEGORY_UID = 3
OCSF_CATEGORY_NAME = "Identity & Access Management"
OCSF_VERSION = "1.1.0"
