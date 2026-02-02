"""
Slack Audit Logs - Silver Layer

Parses key fields from the Bronze variant column and extracts structured data
for easier querying and OCSF mapping.
"""

from pyspark import pipelines as sdp

from utilities.utils import (
    CATALOG, DATABASES, BRONZE_TABLES, SILVER_TABLES,
    SOURCE_NAMES, SOURCE_TYPE, TABLE_PROPERTIES
)

@sdp.table(
    name=f"{CATALOG}.{DATABASES['slack']}.{SILVER_TABLES['slack']}",
    cluster_by=["_event_date"],
    comment=f"Parsed {SOURCE_NAMES['slack']} {SOURCE_TYPE} with extracted fields",
    table_properties=TABLE_PROPERTIES
)
def silver_slack_audit_logs():
    """
    Silver layer: Parse key fields from bronze variant column.
    Extracts important fields for easier querying and OCSF mapping.
    """
    return (
        spark.readStream
            .table(f"{CATALOG}.{DATABASES['slack']}.{BRONZE_TABLES['slack']}")
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
                "try_variant_get(data, '$.action', 'STRING') as action",
                "try_variant_get(data, '$.date_create', 'BIGINT') as date_create_ts",
                
                # Actor information
                "try_variant_get(data, '$.actor.user.id', 'STRING') as actor_id",
                "try_variant_get(data, '$.actor.user.name', 'STRING') as actor_name",
                "try_variant_get(data, '$.actor.user.email', 'STRING') as actor_email",
                "try_variant_get(data, '$.actor.type', 'STRING') as actor_type",
                
                # Entity information
                "try_variant_get(data, '$.entity.type', 'STRING') as entity_type",
                "try_variant_get(data, '$.entity.id', 'STRING') as entity_id",
                "try_variant_get(data, '$.entity.name', 'STRING') as entity_name",
                "try_variant_get(data, '$.entity.privacy', 'STRING') as entity_privacy",
                "try_variant_get(data, '$.entity.is_shared', 'BOOLEAN') as entity_is_shared",
                
                # Context information
                "try_variant_get(data, '$.context.location.type', 'STRING') as location_type",
                "try_variant_get(data, '$.context.location.id', 'STRING') as location_id",
                "try_variant_get(data, '$.context.location.name', 'STRING') as location_name",
                "try_variant_get(data, '$.context.location.domain', 'STRING') as location_domain",
                "try_variant_get(data, '$.context.ip_address', 'STRING') as ip_address",
                "try_variant_get(data, '$.context.ua', 'STRING') as user_agent",
                "try_variant_get(data, '$.context.session_id', 'STRING') as session_id",
                
                # Additional fields
                "try_variant_get(data, '$.details', 'STRING') as details",
                
                # Raw data column LAST
                "data"
            )
    )

