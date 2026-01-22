"""
OCSF IAM Slack Mappings

Slack audit log transformations to OCSF IAM event classes.

OCSF IAM Class Coverage (5 of 6):
✅ 1. Account Change (3001) - User lifecycle events
✅ 2. Authentication (3002) - Login/logout events
✅ 3. Authorize Session (3003) - Workspace settings, SSO
❌ 4. Entity Management (3004) - NOT MAPPED (workspace events mapped to Authorize Session)
✅ 5. User Access Management (3005) - App/guest access management
✅ 6. Group Management (3006) - Channel/usergroup operations
"""


from pyspark.sql.functions import array, to_json, cast

from utilities.utils import (
    OCSF_CLASS_UIDS, OCSF_CATEGORY_UID, OCSF_CATEGORY_NAME, OCSF_VERSION
)

def transform_slack_to_account_change(df):
    """
    Slack user lifecycle events → OCSF Account Change (3001)
    Actions: user_created, user_deactivated, user_reactivated, user_role_changed, user_email_changed
    Schema: https://schema.ocsf.io/1.7.0/classes/account_change
    """
    return (
        df
        .where("action RLIKE 'user_(created|deactivated|reactivated|role_changed|email_changed|permissions_assigned)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Slack', 'vendor_name', 'Slack Technologies'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(date_create_ts AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['account_change']} AS INT) as class_uid",
            "'Account Change' as class_name",
            """CASE
                WHEN action LIKE '%deactivated%' THEN 4
                WHEN action LIKE '%created%' OR action LIKE '%reactivated%' THEN 2
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%created' THEN 1
                WHEN action LIKE '%changed' OR action LIKE '%assigned' THEN 3
                WHEN action LIKE '%deactivated' THEN 4
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 3 THEN 'Update' WHEN activity_id = 4 THEN 'Delete' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """CONCAT('Slack user account ', 
                CASE 
                    WHEN action LIKE '%created' THEN 'created'
                    WHEN action LIKE '%deactivated' THEN 'deactivated'
                    WHEN action LIKE '%reactivated' THEN 'reactivated'
                    WHEN action LIKE '%role_changed' THEN 'role changed'
                    WHEN action LIKE '%email_changed' THEN 'email changed'
                    WHEN action LIKE '%permissions_assigned' THEN 'permissions assigned'
                    ELSE action
                END,
                ' for user ', entity_name
            ) as message""",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN actor_type = 'bot' THEN 'System' WHEN actor_type = 'service' THEN 'Service' ELSE 'User' END,
                    'type_id', CASE WHEN actor_type = 'bot' THEN 3 WHEN actor_type = 'service' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', location_domain,
                    'uid_alt', CAST(NULL AS STRING)
                )
            ) as actor""",
            "CASE WHEN entity_type = 'user' THEN named_struct('uid', entity_id, 'name', entity_name, 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) END as user",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'target_user', 'type', 'User Name', 'type_id', 4, 'value', entity_id)
            ) as observables""",
            """array(
                named_struct('name', 'workspace', 'value', location_domain, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'slack', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('entity_type', entity_type, 'raw_data', to_json(data)) as unmapped"
        )
    )

def transform_slack_to_authentication(df):
    """
    Slack login/logout events → OCSF Authentication (3002)
    Actions: user_login, user_logout, user_login_failed
    Schema: https://schema.ocsf.io/1.7.0/classes/authentication
    """
    return (
        df
        .where("action RLIKE 'user_(login|logout|login_failed)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Slack', 'vendor_name', 'Slack Technologies'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(date_create_ts AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authentication']} AS INT) as class_uid",
            "'Authentication' as class_name",
            "CASE WHEN action LIKE '%failed%' THEN 4 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 4 THEN 'High' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%login' AND action NOT LIKE '%logout%' THEN 1 WHEN action LIKE '%logout' THEN 2 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 1 THEN 'Logon' WHEN activity_id = 2 THEN 'Logoff' ELSE 'Other' END as activity_name",
            "CASE WHEN action LIKE '%failed%' THEN 2 ELSE 1 END as status_id",
            "CASE WHEN action LIKE '%failed%' THEN 'Failure' ELSE 'Success' END as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', 'User',
                    'type_id', 1,
                    'email_addr', actor_email,
                    'domain', location_domain,
                    'uid_alt', CAST(NULL AS STRING)
                )
            ) as actor""",
            """named_struct(
                'ip', ip_address,
                'session', named_struct('uid', session_id),
                'agent', user_agent
            ) as src_endpoint""",
            "'Password' as auth_protocol",
            "1 as auth_protocol_id",
            "named_struct('hostname', CONCAT(location_domain, '.slack.com'), 'name', 'Slack') as dst_endpoint",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'src_ip', 'type', 'IP Address', 'type_id', 2, 'value', ip_address),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email)
            ) as observables""",
            """array(
                named_struct('name', 'workspace', 'value', location_domain, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'slack', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('entity_type', entity_type, 'raw_data', to_json(data)) as unmapped"
        )
    )

def transform_slack_to_authorize_session(df):
    """
    Slack workspace settings/SSO → OCSF Authorize Session (3003)
    Actions: workspace_setting_changed, workspace_sso_enabled, workspace_sso_disabled
    Schema: https://schema.ocsf.io/1.7.0/classes/authorize_session
    """
    return (
        df
        .where("action RLIKE 'workspace_(setting_changed|sso_enabled|sso_disabled|created|name_changed|domain_changed)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Slack', 'vendor_name', 'Slack Technologies'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(date_create_ts AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authorize_session']} AS INT) as class_uid",
            "'Authorize Session' as class_name",
            "CASE WHEN action LIKE '%sso%' THEN 3 WHEN action LIKE '%changed' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%created' OR action LIKE '%enabled' THEN 1 WHEN action LIKE '%changed' THEN 3 WHEN action LIKE '%disabled' THEN 4 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 3 THEN 'Update' WHEN activity_id = 4 THEN 'Delete' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN actor_type = 'bot' THEN 'System' WHEN actor_type = 'service' THEN 'Service' ELSE 'User' END,
                    'type_id', CASE WHEN actor_type = 'bot' THEN 3 WHEN actor_type = 'service' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', location_domain,
                    'uid_alt', CAST(NULL AS STRING)
                )
            ) as actor""",
            """named_struct(
                'uid', COALESCE(location_id, entity_id),
                'name', COALESCE(location_name, entity_name),
                'type', COALESCE(location_type, entity_type),
                'owner', named_struct('name', location_domain),
                'data', map('privacy', entity_privacy, 'is_shared', CAST(entity_is_shared AS STRING))
            ) as resource""",
            "CASE WHEN entity_privacy IS NOT NULL THEN array(entity_privacy) ELSE CAST(NULL AS ARRAY<STRING>) END as privileges",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'workspace', 'type', 'Resource Name', 'type_id', 10, 'value', location_domain)
            ) as observables""",
            """array(
                named_struct('name', 'workspace', 'value', location_domain, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'slack', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('entity_type', entity_type, 'raw_data', to_json(data)) as unmapped"
        )
    )

def transform_slack_to_user_access_management(df):
    """
    Slack app/guest access → OCSF User Access Management (3005)
    Actions: app_installed, app_uninstalled, app_scopes_expanded, guest_invited, guest_removed
    Schema: https://schema.ocsf.io/1.7.0/classes/user_access_management
    """
    return (
        df
        .where("action RLIKE 'app_(installed|uninstalled|scopes_expanded)|guest_(invited|removed)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Slack', 'vendor_name', 'Slack Technologies'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(date_create_ts AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['user_access_management']} AS INT) as class_uid",
            "'User Access Management' as class_name",
            "CASE WHEN action LIKE '%removed%' OR action LIKE '%uninstalled%' THEN 3 WHEN action LIKE '%invited%' OR action LIKE '%installed%' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%installed' OR action LIKE '%invited' THEN 5 WHEN action LIKE '%uninstalled' OR action LIKE '%removed' THEN 6 WHEN action LIKE '%expanded' THEN 3 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' WHEN activity_id = 3 THEN 'Update' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN actor_type = 'bot' THEN 'System' WHEN actor_type = 'service' THEN 'Service' ELSE 'User' END,
                    'type_id', CASE WHEN actor_type = 'bot' THEN 3 WHEN actor_type = 'service' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', location_domain,
                    'uid_alt', CAST(NULL AS STRING)
                )
            ) as actor""",
            "CASE WHEN action LIKE '%guest%' THEN named_struct('uid', entity_id, 'name', entity_name, 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) END as user",
            """named_struct(
                'uid', COALESCE(location_id, entity_id),
                'name', COALESCE(location_name, entity_name),
                'type', COALESCE(location_type, entity_type),
                'owner', named_struct('name', location_domain),
                'data', map('privacy', entity_privacy, 'is_shared', CAST(entity_is_shared AS STRING))
            ) as resource""",
            "array('access') as privileges",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'entity', 'type', 'Resource Name', 'type_id', 10, 'value', entity_name)
            ) as observables""",
            """array(
                named_struct('name', 'workspace', 'value', location_domain, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'slack', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('entity_type', entity_type, 'raw_data', to_json(data)) as unmapped"
        )
    )

def transform_slack_to_group_management(df):
    """
    Slack channel/usergroup operations → OCSF Group Management (3006)
    Actions: channel_*, usergroup_*
    Schema: https://schema.ocsf.io/1.7.0/classes/group_management
    """
    return (
        df
        .where("action RLIKE 'channel_(created|deleted|archive|unarchive|rename|converted_to_private|posting_permissions_updated|member_joined)|usergroup_(created|member_added|member_removed)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Slack', 'vendor_name', 'Slack Technologies'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(date_create_ts AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['group_management']} AS INT) as class_uid",
            "'Group Management' as class_name",
            """CASE
                WHEN action LIKE '%deleted%' THEN 3
                WHEN action LIKE '%created%' THEN 2
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%created' THEN 1
                WHEN action LIKE '%changed' OR action LIKE '%updated' OR action LIKE '%rename' OR action LIKE '%converted%' THEN 3
                WHEN action LIKE '%deleted' OR action LIKE '%archive' THEN 4
                WHEN action LIKE '%added' OR action LIKE '%joined' THEN 5
                WHEN action LIKE '%removed' THEN 6
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 3 THEN 'Update' WHEN activity_id = 4 THEN 'Delete' WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN actor_type = 'bot' THEN 'System' WHEN actor_type = 'service' THEN 'Service' ELSE 'User' END,
                    'type_id', CASE WHEN actor_type = 'bot' THEN 3 WHEN actor_type = 'service' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', location_domain,
                    'uid_alt', CAST(NULL AS STRING)
                )
            ) as actor""",
            """CASE WHEN entity_type IN ('channel', 'usergroup') THEN named_struct(
                'uid', entity_id,
                'name', entity_name,
                'type', entity_type,
                'privileges', CASE WHEN entity_privacy IS NOT NULL THEN array(entity_privacy) ELSE CAST(NULL AS ARRAY<STRING>) END
            ) END as `group`""",
            # Note: user field is NULL because Slack's entity object contains the usergroup being modified,
            # not the target user. Production logs may include this in the details field requiring additional parsing.
            "CAST(NULL AS STRUCT<uid: STRING, name: STRING, type: STRING, type_id: INT, email_addr: STRING, domain: STRING, uid_alt: STRING>) as user",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'entity', 'type', 'Group Name', 'type_id', 21, 'value', entity_name)
            ) as observables""",
            """array(
                named_struct('name', 'workspace', 'value', location_domain, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'slack', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('entity_type', entity_type, 'raw_data', to_json(data)) as unmapped"
        )
    )


