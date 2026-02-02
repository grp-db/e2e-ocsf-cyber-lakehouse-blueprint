"""
Slack Audit Logs - Bronze Layer

Ingests raw Slack audit logs using Auto Loader and stores them in the Bronze layer
with a variant column for semi-structured JSON data.
"""

from pyspark import pipelines as sdp
from pyspark.sql.functions import (
    col, from_unixtime, to_date, current_timestamp, lit, coalesce, expr
)

from utilities.utils import (
    CATALOG, DATABASES, FILE_PATHS, SCHEMA_PATHS, BRONZE_TABLES,
    SOURCE_NAMES, SOURCE_TYPE, TABLE_PROPERTIES
)

# Metadata fields to add to bronze layer
META_COLS = {
    "_event_time": from_unixtime(expr("try_variant_get(data, '$.date_create', 'BIGINT')")),
    "_event_date": to_date(from_unixtime(expr("try_variant_get(data, '$.date_create', 'BIGINT')"))),
    "_source": lit(SOURCE_NAMES["slack"]),
    "_source_type": lit(SOURCE_TYPE),
    "_ingest_time": current_timestamp(),
    "_hostname": coalesce(expr("try_variant_get(data, '$.context.ip_address', 'STRING')"), lit("unknown")),
    "_file_path": col("_metadata.file_path")
}


@sdp.table(
    name=f"{CATALOG}.{DATABASES['slack']}.{BRONZE_TABLES['slack']}",
    cluster_by=["_event_date"],
    comment=f"Raw {SOURCE_NAMES['slack']} {SOURCE_TYPE} streamed in from JSON files with variant column",
    table_properties=TABLE_PROPERTIES
)
def bronze_slack_audit_logs():
    """
    Bronze layer: Ingest raw Slack audit logs using Auto Loader.
    Loads raw JSON as a single variant column.
    
    Note: FILE_PATHS['slack'] is defined in utilities/utils.py - CHANGE ME!
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("singleVariantColumn", "data")
            .option("cloudFiles.schemaLocation", SCHEMA_PATHS["slack"])
            .load(FILE_PATHS["slack"])  # CHANGE ME! Set in utilities/utils.py
            .withColumns(META_COLS)
            .selectExpr(
                # Metadata fields FIRST
                "_event_time",
                "_event_date",
                "_file_path",
                "_source",
                "_source_type",
                "_ingest_time",
                "_hostname",
                # Raw data column LAST
                "data"
            )
    )

