"""
GitHub Audit Logs - Silver Layer

Parses key fields from the Bronze variant column and extracts structured data
for easier querying and OCSF mapping.
"""

from pyspark import pipelines as sdp

from utilities.utils import (
    CATALOG, DATABASES, BRONZE_TABLES, SILVER_TABLES,
    SOURCE_NAMES, SOURCE_TYPE, TABLE_PROPERTIES
)

@sdp.table(
    name=f"{CATALOG}.{DATABASES['github']}.{SILVER_TABLES['github']}",
    cluster_by=["_event_date"],
    comment=f"Parsed {SOURCE_NAMES['github']} {SOURCE_TYPE} with extracted fields",
    table_properties=TABLE_PROPERTIES
)
def silver_github_audit_logs():
    """
    Silver layer: Parse key fields from bronze variant column.
    Extracts important fields for easier querying and OCSF mapping.
    """
    return (
        spark.readStream
            .table(f"{CATALOG}.{DATABASES['github']}.{BRONZE_TABLES['github']}")
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
                "try_variant_get(data, '$.created_at', 'BIGINT') as created_at_ms",
                
                # Actor information
                "try_variant_get(data, '$.actor', 'STRING') as actor",
                "try_variant_get(data, '$.user', 'STRING') as user",
                "try_variant_get(data, '$.org', 'STRING') as organization",
                "try_variant_get(data, '$.repo', 'STRING') as repository",
                
                # Location information
                "try_variant_get(data, '$.actor_location.country_code', 'STRING') as actor_country_code",
                "try_variant_get(data, '$.actor_location.country', 'STRING') as actor_country",
                "try_variant_get(data, '$.actor_location.region', 'STRING') as actor_region",
                "try_variant_get(data, '$.actor_location.city', 'STRING') as actor_city",
                "try_variant_get(data, '$.actor_location.ip', 'STRING') as actor_ip",
                
                # Additional data payload
                "try_variant_get(data, '$.data.email', 'STRING') as email",
                "try_variant_get(data, '$.data.ref', 'STRING') as ref",
                "try_variant_get(data, '$.data.head', 'STRING') as head",
                "try_variant_get(data, '$.data.base', 'STRING') as base",
                "try_variant_get(data, '$.data.target_login', 'STRING') as target_login",
                "try_variant_get(data, '$.data.permission', 'STRING') as permission",
                "try_variant_get(data, '$.data.team', 'STRING') as team",
                "try_variant_get(data, '$.data.visibility', 'STRING') as visibility",
                "try_variant_get(data, '$.data.hook_id', 'INT') as hook_id",
                
                # Raw data column LAST
                "data"
            )
    )

