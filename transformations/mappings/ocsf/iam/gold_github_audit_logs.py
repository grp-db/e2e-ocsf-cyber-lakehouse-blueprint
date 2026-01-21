"""
OCSF IAM GitHub Mappings

GitHub audit log transformations to OCSF IAM event classes.

OCSF IAM Class Coverage (5 of 6):
✅ 1. Account Change (3001) - Member management (org/team/repo)
✅ 2. Authentication (3002) - Login/logout, OAuth events
✅ 3. Authorize Session (3003) - Repository access, protected branches
❌ 4. Entity Management (3004) - NOT MAPPED (not applicable to GitHub's model)
✅ 5. User Access Management (3005) - Organization member management
✅ 6. Group Management (3006) - Team operations
"""

from pyspark.sql.functions import array, to_json, cast

from utilities.utils import (
    OCSF_CLASS_UIDS, OCSF_CATEGORY_UID, OCSF_CATEGORY_NAME, OCSF_VERSION
)


def transform_github_to_account_change(df):
    """
    GitHub user lifecycle events → OCSF Account Change (3001)
    Actions: org.add_member, team.add_member/remove_member, repo.add_member/update_member/remove_member
    Schema: https://schema.ocsf.io/1.3.0/classes/account_change
    """
    return (
        df
        .where("action RLIKE '(org|team|repo)\\.(add_member|remove_member|update_member)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'GitHub', 'vendor_name', 'GitHub Inc.'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(created_at_ms AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['account_change']} AS INT) as class_uid",
            "'Account Change' as class_name",
            """CASE
                WHEN action LIKE '%delete%' OR action LIKE '%suspend%' THEN 4
                WHEN action LIKE '%create%' OR action LIKE '%update%' THEN 2
                ELSE 1
            END as severity_id""",
            "CASE WHEN severity_id = 4 THEN 'High' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%created' THEN 1
                WHEN action LIKE '%updated' OR action LIKE '%renamed' THEN 3
                WHEN action LIKE '%deleted' THEN 4
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 3 THEN 'Update' WHEN activity_id = 4 THEN 'Delete' ELSE 'Other' END as activity_name",
            "CASE WHEN action LIKE '%failed%' THEN 2 ELSE 1 END as status_id",
            "CASE WHEN action LIKE '%failed%' THEN 'Failure' ELSE 'Success' END as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            """CONCAT('GitHub member ', 
                CASE 
                    WHEN action LIKE '%add_member' THEN 'added to'
                    WHEN action LIKE '%remove_member' THEN 'removed from'
                    WHEN action LIKE '%update_member' THEN 'updated in'
                    ELSE action
                END,
                ' ',
                CASE
                    WHEN action LIKE 'org.%' THEN 'organization'
                    WHEN action LIKE 'team.%' THEN CONCAT('team ', COALESCE(team, 'unknown'))
                    WHEN action LIKE 'repo.%' THEN CONCAT('repository ', COALESCE(repository, 'unknown'))
                    ELSE 'entity'
                END,
                ': ', COALESCE(target_login, user, actor)
            ) as message""",
            "named_struct('uid', actor, 'name', actor, 'type', 'User', 'type_id', 1, 'email_addr', email, 'domain', organization, 'uid_alt', CAST(NULL AS STRING)) as actor",
            "named_struct('uid', COALESCE(target_login, user), 'name', COALESCE(target_login, user), 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) as user",
            """array(
                named_struct('name', 'actor', 'type', 'User Name', 'type_id', 4, 'value', actor),
                named_struct('name', 'target_user', 'type', 'User Name', 'type_id', 4, 'value', COALESCE(target_login, user))
            ) as observables""",
            """array(
                named_struct('name', 'organization', 'value', organization, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'github', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('raw_data', to_json(data)) as unmapped"
        )
    )


def transform_github_to_authentication(df):
    """
    GitHub login/logout events → OCSF Authentication (3002)
    Actions: user.login, user.logout, oauth_authorization.create, oauth_authorization.destroy
    Schema: https://schema.ocsf.io/1.3.0/classes/authentication
    """
    return (
        df
        .where("action RLIKE 'user\\.(login|logout)|oauth_authorization\\.(create|destroy)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'GitHub', 'vendor_name', 'GitHub Inc.'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(created_at_ms AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authentication']} AS INT) as class_uid",
            "'Authentication' as class_name",
            "CASE WHEN action LIKE '%failed%' THEN 4 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 4 THEN 'High' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%login' OR action LIKE 'oauth_authorization.create' THEN 1 WHEN action LIKE '%logout' OR action LIKE 'oauth_authorization.destroy' THEN 2 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 1 THEN 'Logon' WHEN activity_id = 2 THEN 'Logoff' ELSE 'Other' END as activity_name",
            "CASE WHEN action LIKE '%failed%' THEN 2 ELSE 1 END as status_id",
            "CASE WHEN action LIKE '%failed%' THEN 'Failure' ELSE 'Success' END as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            "named_struct('uid', actor, 'name', actor, 'type', 'User', 'type_id', 1, 'email_addr', email, 'domain', organization, 'uid_alt', CAST(NULL AS STRING)) as actor",
            """named_struct(
                'ip', actor_ip,
                'location', named_struct('city', actor_city, 'region', actor_region, 'country', actor_country, 'coordinates', CAST(NULL AS ARRAY<DOUBLE>))
            ) as src_endpoint""",
            """array(
                named_struct('name', 'actor', 'type', 'User Name', 'type_id', 4, 'value', actor),
                named_struct('name', 'src_ip', 'type', 'IP Address', 'type_id', 2, 'value', actor_ip)
            ) as observables""",
            """array(
                named_struct('name', 'organization', 'value', organization, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'github', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('raw_data', to_json(data)) as unmapped"
        )
    )


def transform_github_to_authorize_session(df):
    """
    GitHub repo access/permissions → OCSF Authorize Session (3003)
    Actions: repo.access, repo.add_member, repo.remove_member, protected_branch operations
    Schema: https://schema.ocsf.io/1.3.0/classes/authorize_session
    """
    return (
        df
        .where("action RLIKE 'repo\\.(access|add_member|remove_member|update_member)|protected_branch\\.(create|destroy)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'GitHub', 'vendor_name', 'GitHub Inc.'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(created_at_ms AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['authorize_session']} AS INT) as class_uid",
            "'Authorize Session' as class_name",
            "CASE WHEN action LIKE '%remove%' THEN 3 WHEN action LIKE '%add%' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%add%' THEN 5 WHEN action LIKE '%remove%' THEN 6 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            "named_struct('uid', actor, 'name', actor, 'type', 'User', 'type_id', 1, 'email_addr', email, 'domain', organization, 'uid_alt', CAST(NULL AS STRING)) as actor",
            """named_struct(
                'uid', COALESCE(repository, organization),
                'name', COALESCE(repository, organization),
                'type', CASE WHEN repository IS NOT NULL THEN 'Repository' ELSE 'Organization' END,
                'owner', named_struct('name', organization),
                'data', map('ref', ref, 'visibility', visibility)
            ) as resource""",
            "CASE WHEN permission IS NOT NULL THEN array(permission) ELSE CAST(NULL AS ARRAY<STRING>) END as privileges",
            "CASE WHEN target_login IS NOT NULL THEN named_struct('uid', target_login, 'name', target_login, 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) END as user",
            """array(
                named_struct('name', 'actor', 'type', 'User Name', 'type_id', 4, 'value', actor),
                named_struct('name', 'repository', 'type', 'Resource Name', 'type_id', 10, 'value', repository)
            ) as observables""",
            """array(
                named_struct('name', 'organization', 'value', organization, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'github', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('raw_data', to_json(data)) as unmapped"
        )
    )


def transform_github_to_user_access_management(df):
    """
    GitHub org member management → OCSF User Access Management (3005)
    Actions: org.add_member, org.remove_member, org.update_member, org.add_billing_manager
    Schema: https://schema.ocsf.io/1.3.0/classes/user_access_management
    """
    return (
        df
        .where("action RLIKE 'org\\.(add_member|remove_member|update_member|add_billing_manager)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'GitHub', 'vendor_name', 'GitHub Inc.'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(created_at_ms AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['user_access_management']} AS INT) as class_uid",
            "'User Access Management' as class_name",
            "CASE WHEN action LIKE '%remove%' THEN 3 WHEN action LIKE '%add%' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            "CASE WHEN action LIKE '%add%' THEN 5 WHEN action LIKE '%remove%' THEN 6 WHEN action LIKE '%update%' THEN 3 ELSE 99 END as activity_id",
            "CASE WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' WHEN activity_id = 3 THEN 'Update' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            "named_struct('uid', actor, 'name', actor, 'type', 'User', 'type_id', 1, 'email_addr', email, 'domain', organization, 'uid_alt', CAST(NULL AS STRING)) as actor",
            "named_struct('uid', COALESCE(target_login, user), 'name', COALESCE(target_login, user), 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) as user",
            "named_struct('uid', organization, 'name', organization, 'type', 'Organization', 'owner', named_struct('name', organization), 'data', CAST(NULL AS MAP<STRING, STRING>)) as resource",
            "CASE WHEN permission IS NOT NULL THEN array(permission) ELSE array('member') END as privileges",
            """array(
                named_struct('name', 'actor', 'type', 'User Name', 'type_id', 4, 'value', actor),
                named_struct('name', 'target_user', 'type', 'User Name', 'type_id', 4, 'value', COALESCE(target_login, user))
            ) as observables""",
            """array(
                named_struct('name', 'organization', 'value', organization, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'github', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('raw_data', to_json(data)) as unmapped"
        )
    )


def transform_github_to_group_management(df):
    """
    GitHub team operations → OCSF Group Management (3006)
    Actions: team.add_member, team.remove_member, team.create, team.destroy
    Schema: https://schema.ocsf.io/1.3.0/classes/group_management
    """
    return (
        df
        .where("action RLIKE 'team\\.(add_member|remove_member|create|destroy)'")
        .selectExpr(
            "_event_date",
            "CAST(_event_time AS TIMESTAMP) as _event_time",
            "_source",
            "_source_type",
            f"""named_struct(
                'version', '{OCSF_VERSION}',
                'product', named_struct('name', 'GitHub', 'vendor_name', 'GitHub Inc.'),
                'profiles', array('cloud', 'datetime'),
                'uid', event_id,
                'event_code', action,
                'logged_time', _ingest_time,
                'original_time', CAST(created_at_ms AS STRING)
            ) as metadata""",
            f"CAST({OCSF_CATEGORY_UID} AS INT) as category_uid",
            f"'{OCSF_CATEGORY_NAME}' as category_name",
            f"CAST({OCSF_CLASS_UIDS['group_management']} AS INT) as class_uid",
            "'Group Management' as class_name",
            "CASE WHEN action LIKE '%destroy%' THEN 3 WHEN action LIKE '%create%' THEN 2 ELSE 1 END as severity_id",
            "CASE WHEN severity_id = 3 THEN 'Medium' WHEN severity_id = 2 THEN 'Low' ELSE 'Informational' END as severity",
            """CASE
                WHEN action LIKE '%create' THEN 1
                WHEN action LIKE '%destroy' THEN 4
                WHEN action LIKE '%add_member' THEN 5
                WHEN action LIKE '%remove_member' THEN 6
                ELSE 99
            END as activity_id""",
            "CASE WHEN activity_id = 1 THEN 'Create' WHEN activity_id = 4 THEN 'Delete' WHEN activity_id = 5 THEN 'Add' WHEN activity_id = 6 THEN 'Remove' ELSE 'Other' END as activity_name",
            "1 as status_id",
            "'Success' as status",
            "CAST(_event_time AS TIMESTAMP) as time",
            "named_struct('uid', actor, 'name', actor, 'type', 'User', 'type_id', 1, 'email_addr', email, 'domain', organization, 'uid_alt', CAST(NULL AS STRING)) as actor",
            "named_struct('uid', team, 'name', team, 'type', 'Team') as `group`",
            "CASE WHEN target_login IS NOT NULL THEN named_struct('uid', target_login, 'name', target_login, 'type', 'User', 'type_id', 1, 'email_addr', CAST(NULL AS STRING), 'domain', CAST(NULL AS STRING), 'uid_alt', CAST(NULL AS STRING)) END as user",
            """array(
                named_struct('name', 'actor', 'type', 'User Name', 'type_id', 4, 'value', actor),
                named_struct('name', 'team', 'type', 'Group Name', 'type_id', 21, 'value', team)
            ) as observables""",
            """array(
                named_struct('name', 'organization', 'value', organization, 'type', 'context', 'data', CAST(NULL AS MAP<STRING, STRING>)),
                named_struct('name', 'source', 'value', 'github', 'type', 'source_system', 'data', CAST(NULL AS MAP<STRING, STRING>))
            ) as enrichments""",
            "named_struct('raw_data', to_json(data)) as unmapped"
        )
    )

