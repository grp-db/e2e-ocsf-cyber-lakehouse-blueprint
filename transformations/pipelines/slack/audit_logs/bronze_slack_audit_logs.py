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
    CATALOG, DATABASES, FILE_PATHS, BRONZE_TABLES,
    SOURCE_NAMES, SOURCE_TYPE
)

# Set catalog and database context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE DATABASE {DATABASES['slack']}")

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
    name=BRONZE_TABLES["slack"],
    cluster_by=["_event_date"],
    comment=f"Raw {SOURCE_NAMES['slack']} {SOURCE_TYPE} streamed in from JSON files with variant column",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_slack_audit_logs():
    """
    Bronze layer: Ingest raw Slack audit logs using Auto Loader.
    Loads raw JSON as a single variant column.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("singleVariantColumn", "data")
            .option("cloudFiles.schemaLocation", f"{FILE_PATHS['slack']}_checkpoint")
            .load(FILE_PATHS["slack"])
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

