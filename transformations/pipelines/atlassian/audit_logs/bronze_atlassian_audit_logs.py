"""
Atlassian Audit Logs - Bronze Layer

Ingests raw Atlassian audit logs using Auto Loader and stores them in the Bronze layer
with a variant column for semi-structured JSON data.
"""

from pyspark import pipelines as sdp
from pyspark.sql.functions import (
    col, to_date, to_timestamp, current_timestamp, lit, coalesce, expr
)

from utilities.utils import (
    CATALOG, DATABASES, FILE_PATHS, SCHEMA_PATHS, BRONZE_TABLES,
    SOURCE_NAMES, SOURCE_TYPE, TABLE_PROPERTIES
)

# Metadata fields to add to bronze layer
META_COLS = {
    "_event_time": to_timestamp(expr("try_variant_get(data, '$.attributes.time', 'STRING')")),
    "_event_date": to_date(to_timestamp(expr("try_variant_get(data, '$.attributes.time', 'STRING')"))),
    "_source": lit(SOURCE_NAMES["atlassian"]),
    "_source_type": lit(SOURCE_TYPE),
    "_ingest_time": current_timestamp(),
    "_hostname": coalesce(expr("try_variant_get(data, '$.attributes.location.ip', 'STRING')"), lit("unknown")),
    "_file_path": col("_metadata.file_path")
}


@sdp.table(
    name=f"{CATALOG}.{DATABASES['atlassian']}.{BRONZE_TABLES['atlassian']}",
    cluster_by=["_event_date"],
    comment=f"Raw {SOURCE_NAMES['atlassian']} {SOURCE_TYPE} streamed in from JSON files with variant column",
    table_properties=TABLE_PROPERTIES
)
def bronze_atlassian_audit_logs():
    """
    Bronze layer: Ingest raw Atlassian audit logs using Auto Loader.
    Loads raw JSON as a single variant column.
    
    Note: FILE_PATHS['atlassian'] is defined in utilities/utils.py - CHANGE ME!
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("singleVariantColumn", "data")
            .option("cloudFiles.schemaLocation", SCHEMA_PATHS["atlassian"])
            .load(FILE_PATHS["atlassian"])  # CHANGE ME! Set in utilities/utils.py
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

