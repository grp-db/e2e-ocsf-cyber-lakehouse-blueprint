"""
Atlassian Audit Logs - Silver Layer

Parses key fields from the Bronze variant column and extracts structured data
for easier querying and OCSF mapping.
"""

from pyspark import pipelines as sdp

from utilities.utils import (
    CATALOG, DATABASES, BRONZE_TABLES, SILVER_TABLES,
    SOURCE_NAMES, SOURCE_TYPE, TABLE_PROPERTIES
)

@sdp.table(
    name=f"{CATALOG}.{DATABASES['atlassian']}.{SILVER_TABLES['atlassian']}",
    cluster_by=["_event_date"],
    comment=f"Parsed {SOURCE_NAMES['atlassian']} {SOURCE_TYPE} with extracted fields",
    table_properties=TABLE_PROPERTIES
)
def silver_atlassian_audit_logs():
    """
    Silver layer: Parse key fields from bronze variant column.
    Extracts important fields for easier querying and OCSF mapping.
    """
    return (
        spark.readStream
            .table(f"{CATALOG}.{DATABASES['atlassian']}.{BRONZE_TABLES['atlassian']}")
            .selectExpr(
                # Metadata fields FIRST
                "_event_time",
                "_event_date",
                "_file_path",
                "_source",
                "_source_type",
                "_ingest_time",
                "_hostname",
                
                # Event identification
                "try_variant_get(data, '$.id', 'STRING') as event_id",
                "try_variant_get(data, '$.attributes.action', 'STRING') as action",
                "try_variant_get(data, '$.attributes.time', 'STRING') as event_time_iso",
                
                # Actor information
                "try_variant_get(data, '$.attributes.actor.id', 'STRING') as actor_id",
                "try_variant_get(data, '$.attributes.actor.name', 'STRING') as actor_name",
                "try_variant_get(data, '$.attributes.actor.email', 'STRING') as actor_email",
                "try_variant_get(data, '$.attributes.actor.links.self', 'STRING') as actor_link",
                
                # Authentication information (under attributes.actor.auth)
                "try_variant_get(data, '$.attributes.actor.auth.authType', 'STRING') as auth_type",
                "try_variant_get(data, '$.attributes.actor.auth.sessionId', 'STRING') as session_id",
                "try_variant_get(data, '$.attributes.actor.auth.tokenId', 'STRING') as token_id",
                
                # Location information
                "try_variant_get(data, '$.attributes.location.ip', 'STRING') as location_ip",
                "try_variant_get(data, '$.attributes.location.city', 'STRING') as city",
                "try_variant_get(data, '$.attributes.location.regionName', 'STRING') as region_name",
                "try_variant_get(data, '$.attributes.location.countryName', 'STRING') as country_name",
                "try_variant_get(data, '$.attributes.location.geo', 'STRING') as location_geo",
                
                # Additional attributes
                "try_variant_get(data, '$.attributes.userAgent', 'STRING') as user_agent",
                "try_variant_get(data, '$.attributes.location.timezone', 'STRING') as timezone",
                "try_variant_get(data, '$.message.content', 'STRING') as message_content",
                "try_variant_get(data, '$.message.i18n.locale', 'STRING') as message_locale",
                "try_variant_get(data, '$.message.format', 'STRING') as message_format",
                
                # Risk assessment (at root level)
                "try_variant_get(data, '$.risk.score', 'INT') as risk_score",
                "try_variant_get(data, '$.risk.level', 'STRING') as risk_level",
                
                # Container and context information (arrays - get first element or whole array as string)
                "try_variant_get(data, '$.attributes.container[0]', 'STRING') as container_id",
                "CAST(NULL AS STRING) as container_type",
                "try_variant_get(data, '$.tags', 'STRING') as tags",
                "try_variant_get(data, '$.attributes.context[0]', 'STRING') as context",
                "try_variant_get(data, '$.attributes.changes[0]', 'STRING') as changes",
                
                # Raw data column LAST
                "data"
            )
    )

