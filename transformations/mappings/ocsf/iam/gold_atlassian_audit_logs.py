"""
OCSF IAM Atlassian Mappings

Atlassian audit log transformations to OCSF IAM event classes.

OCSF IAM Class Coverage (5 of 6):
✅ 1. Account Change (3001) - User lifecycle, API keys, tokens, 2FA
✅ 2. Authentication (3002) - Login/logout, session management
✅ 3. Authorize Session (3003) - Permissions, roles, SSO settings
✅ 4. Entity Management (3004) - Workspace/project/webhook management
❌ 5. User Access Management (3005) - NOT MAPPED (covered by Authorize Session)
✅ 6. Group Management (3006) - Group operations

Note: Atlassian is the only source that maps to Entity Management (3004).
"""


from pyspark.sql.functions import array, to_json, cast

from utilities.utils import (
    OCSF_CLASS_UIDS, OCSF_CATEGORY_UID, OCSF_CATEGORY_NAME, OCSF_VERSION
)

def transform_atlassian_to_account_change(df):
    """
    Atlassian user lifecycle events → OCSF Account Change (3001)
    Actions: user.created, user.deleted, user.updated, user.deactivated, user.2fa_enabled, api_key.*, token.*
    Schema: https://schema.ocsf.io/1.7.0/classes/account_change
    """
    return (
        df
        .where("action RLIKE 'user\\.(created|deleted|updated|deactivated|reactivated|2fa_enabled|2fa_disabled|password_reset)|api_key\\.|token\\.created'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Atlassian', 'vendor_name', 'Atlassian'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', event_time_iso
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['account_change']} AS INT) as class_uid",
            "'Account Change' as class_name",
            """CASE
                WHEN risk_score >= 70 OR action LIKE '%delete%' OR risk_level = 'high' THEN 4
                WHEN risk_score >= 40 OR action LIKE '%disable%' OR risk_level = 'medium' THEN 3
                WHEN risk_score >= 20 OR action LIKE '%create%' OR action LIKE '%enable%' THEN 2
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%created' OR action LIKE '%enabled' THEN 1
                WHEN action LIKE '%updated' THEN 3
                WHEN action LIKE '%deleted' OR action LIKE '%disabled' OR action LIKE '%revoked' THEN 4
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 3 THEN 'Update' WHEN activity_id = 4 THEN 'Delete' ELSE 'Other' END as activity_name",
            "CASE WHEN action LIKE '%failed%' THEN 2 ELSE 1 END as status_id",
            "CASE WHEN action LIKE '%failed%' THEN 'Failure' ELSE 'Success' END as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            "message_content as message",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN auth_type = 'api-token' THEN 'System' ELSE 'User' END,
                    'type_id', CASE WHEN auth_type = 'api-token' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', CAST(NULL AS STRING),
                    'uid_alt', actor_link
                )
            ) as actor""",
            "named_struct('uid', actor_id, 'name', actor_name, 'type', 'User', 'type_id', 1, 'email_addr', actor_email, 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) as user",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email)
            ) as observables""",
            """array(
                named_struct('name', 'auth_type', 'value', auth_type, 'type', 'authentication', 'data', map('session_id', session_id, 'token_id', token_id)),
                named_struct('name', 'risk_assessment', 'value', risk_level, 'type', 'risk', 'data', map('score', CAST(risk_score AS STRING), 'level', risk_level)),
                named_struct('name', 'source', 'value', 'atlassian', 'type', 'source_system', 'data', map('timezone', timezone, 'locale', message_locale))
            ) as enrichments""",
            """named_struct(
                'auth_type', auth_type,
                'tags', tags,
                'context', context,
                'changes', changes,
                'raw_data', to_json(data)
            ) as unmapped"""
        )
    )

def transform_atlassian_to_authentication(df):
    """
    Atlassian login/logout events → OCSF Authentication (3002)
    Actions: user.login, user.login_failed, user.logout, user.session_ended
    Schema: https://schema.ocsf.io/1.7.0/classes/authentication
    """
    return (
        df
        .where("action RLIKE 'user\\.(login|login_failed|logout|session_ended)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Atlassian', 'vendor_name', 'Atlassian'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', event_time_iso
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authentication']} AS INT) as class_uid",
            "'Authentication' as class_name",
            """CASE
                WHEN risk_score >= 70 OR action LIKE '%failed%' OR risk_level = 'high' THEN 4
                WHEN risk_score >= 40 OR risk_level = 'medium' THEN 3
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 3 THEN 'Medium' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%login' AND action NOT LIKE '%logout%' THEN 1 WHEN action LIKE '%logout' OR action LIKE '%session_ended' THEN 2 ELSE 99 END as activity_id",
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
                    'domain', CAST(NULL AS STRING),
                    'uid_alt', actor_link
                )
            ) as actor""",
            """named_struct(
                'ip', location_ip,
                'location', named_struct(
                    'city', city,
                    'region', region_name,
                    'country', country_name,
                    'coordinates', CASE WHEN location_geo IS NOT NULL THEN array(CAST(split(location_geo, ',')[0] AS DOUBLE), CAST(split(location_geo, ',')[1] AS DOUBLE)) ELSE CAST(NULL AS ARRAY<DOUBLE>) END,
                    'desc', concat_ws(', ', city, region_name, country_name)
                ),
                'agent', user_agent
            ) as src_endpoint""",
            """CASE 
                WHEN auth_type LIKE '%sso%' THEN 'SAML'
                WHEN auth_type = 'api-token' THEN 'API Key'
                ELSE 'Password'
            END as auth_protocol""",
            """CASE 
                WHEN auth_type LIKE '%sso%' THEN 4
                WHEN auth_type = 'api-token' THEN 99
                ELSE 1
            END as auth_protocol_id""",
            "named_struct('hostname', 'atlassian.com', 'name', 'Atlassian') as dst_endpoint",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email),
                named_struct('name', 'src_ip', 'type', 'IP Address', 'type_id', 2, 'value', location_ip)
            ) as observables""",
            """array(
                named_struct('name', 'auth_type', 'value', auth_type, 'type', 'authentication', 'data', map('session_id', session_id, 'token_id', token_id)),
                named_struct('name', 'risk_assessment', 'value', risk_level, 'type', 'risk', 'data', map('score', CAST(risk_score AS STRING), 'level', risk_level)),
                named_struct('name', 'source', 'value', 'atlassian', 'type', 'source_system', 'data', map('timezone', timezone, 'locale', message_locale))
            ) as enrichments""",
            """named_struct(
                'auth_type', auth_type,
                'raw_data', to_json(data)
            ) as unmapped"""
        )
    )

def transform_atlassian_to_authorize_session(df):
    """
    Atlassian permission/role operations → OCSF Authorize Session (3003)
    Actions: permission.granted, permission.revoked, role.assigned, role.removed, organization.sso_enabled, organization.sso_disabled, organization.scim_enabled
    Schema: https://schema.ocsf.io/1.7.0/classes/authorize_session
    """
    return (
        df
        .where("action RLIKE 'permission\\.(granted|revoked)|role\\.(assigned|removed)|organization\\.(sso_enabled|sso_disabled|scim_enabled)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Atlassian', 'vendor_name', 'Atlassian'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', event_time_iso
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authorize_session']} AS INT) as class_uid",
            "'Authorize Session' as class_name",
            """CASE
                WHEN risk_score >= 70 OR action LIKE '%revoked%' OR risk_level = 'high' THEN 4
                WHEN risk_score >= 40 OR action LIKE '%granted%' OR risk_level = 'medium' THEN 3
                ELSE 2
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 3 THEN 'Medium' ELSE 'Low' END as severity",
            """CASE
                WHEN action LIKE '%granted%' OR action LIKE '%assigned%' OR action LIKE '%enabled%' THEN 5
                WHEN action LIKE '%revoked%' OR action LIKE '%removed%' OR action LIKE '%disabled%' THEN 6
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN auth_type = 'api-token' THEN 'System' ELSE 'User' END,
                    'type_id', CASE WHEN auth_type = 'api-token' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', CAST(NULL AS STRING),
                    'uid_alt', actor_link
                )
            ) as actor""",
            "CASE WHEN tags IS NOT NULL THEN array(tags) ELSE CAST(NULL AS ARRAY<STRING>) END as privileges",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email)
            ) as observables""",
            """array(
                named_struct('name', 'auth_type', 'value', auth_type, 'type', 'authentication', 'data', map('session_id', session_id)),
                named_struct('name', 'risk_assessment', 'value', risk_level, 'type', 'risk', 'data', map('score', CAST(risk_score AS STRING))),
                named_struct('name', 'source', 'value', 'atlassian', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            """named_struct(
                'auth_type', auth_type,
                'tags', tags,
                'changes', changes,
                'raw_data', to_json(data)
            ) as unmapped"""
        )
    )

def transform_atlassian_to_entity_management(df):
    """
    Atlassian workspace/project management → OCSF Entity Management (3004)
    Actions: workspace.created, workspace.deleted, project.created, project.deleted, webhook.*
    Schema: https://schema.ocsf.io/1.7.0/classes/entity_management
    """
    return (
        df
        .where("action RLIKE 'workspace\\.(created|deleted)|project\\.(created|deleted)|webhook\\.'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Atlassian', 'vendor_name', 'Atlassian'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', event_time_iso
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['entity_management']} AS INT) as class_uid",
            "'Entity Management' as class_name",
            """CASE
                WHEN risk_score >= 70 OR action LIKE '%deleted%' OR risk_level = 'high' THEN 4
                WHEN risk_score >= 40 OR risk_level = 'medium' THEN 3
                WHEN action LIKE '%created%' THEN 2
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%created' THEN 1 WHEN action LIKE '%deleted' THEN 4 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 4 THEN 'Delete' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN auth_type = 'api-token' THEN 'System' ELSE 'User' END,
                    'type_id', CASE WHEN auth_type = 'api-token' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', CAST(NULL AS STRING),
                    'uid_alt', actor_link
                )
            ) as actor""",
            """named_struct(
                'uid', event_id,
                'name', CASE 
                    WHEN action LIKE '%workspace%' THEN 'Workspace'
                    WHEN action LIKE '%project%' THEN 'Project'
                    WHEN action LIKE '%webhook%' THEN 'Webhook'
                    ELSE 'Entity'
                END,
                'type', CASE 
                    WHEN action LIKE '%workspace%' THEN 'Workspace'
                    WHEN action LIKE '%project%' THEN 'Project'
                    WHEN action LIKE '%webhook%' THEN 'Webhook'
                    ELSE 'Entity'
                END,
                'owner', named_struct('name', actor_name),
                'data', CAST(NULL AS MAP<STRING, STRING>)
            ) as resource""",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email)
            ) as observables""",
            """array(
                named_struct('name', 'source', 'value', 'atlassian', 'type', 'source_system', 'data', map('timezone', timezone))
            ) as enrichments""",
            """named_struct(
                'raw_data', to_json(data)
            ) as unmapped"""
        )
    )


def transform_atlassian_to_group_management(df):
    """
    Atlassian group operations → OCSF Group Management (3006)
    Actions: group.member_added, group.member_removed, group.created, group.deleted
    Schema: https://schema.ocsf.io/1.7.0/classes/group_management
    """
    return (
        df
        .where("action RLIKE 'group\\.(member_added|member_removed|created|deleted)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'Atlassian', 'vendor_name', 'Atlassian'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', event_time_iso
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['group_management']} AS INT) as class_uid",
            "'Group Management' as class_name",
            "CASE WHEN action LIKE '%deleted%' THEN 3 WHEN action LIKE '%created%' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%created' THEN 1
                WHEN action LIKE '%deleted' THEN 4
                WHEN action LIKE '%member_added' THEN 5
                WHEN action LIKE '%member_removed' THEN 6
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 4 THEN 'Delete' WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """named_struct(
                'user', named_struct(
                    'uid', actor_id,
                    'name', actor_name,
                    'type', CASE WHEN auth_type = 'api-token' THEN 'System' ELSE 'User' END,
                    'type_id', CASE WHEN auth_type = 'api-token' THEN 3 ELSE 1 END,
                    'email_addr', actor_email,
                    'domain', CAST(NULL AS STRING),
                    'uid_alt', actor_link
                )
            ) as actor""",
            "named_struct('uid', event_id, 'name', 'Group', 'type', 'Group') as `group`",
            # Note: user field is NULL because target user details are in the context array (JSON string).
            # Production implementations may parse context to extract user info if needed.
            "CAST(NULL AS STRUCT<uid: STRING, name: STRING, type: STRING, type_id: INT, email_addr: STRING, domain: STRING, uid_alt: STRING>) as user",
            """array(
                named_struct('name', 'actor_id', 'type', 'User Name', 'type_id', 4, 'value', actor_id),
                named_struct('name', 'actor_email', 'type', 'Email Address', 'type_id', 5, 'value', actor_email)
            ) as observables""",
            """array(
                named_struct('name', 'source', 'value', 'atlassian', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            """named_struct(
                'event_id', event_id,
                'action', action,
                'source', 'atlassian',
                'raw_data', to_json(data)
            ) as unmapped"""
        )
    )


