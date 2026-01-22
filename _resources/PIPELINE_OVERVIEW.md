# E2E Cybersecurity Lakehouse - Pipeline Overview

## 🎯 Purpose

**Medallion Architecture** (Bronze → Silver → Gold) that transforms raw audit logs into **OCSF-normalized** security events for SIEM integration and analytics.

---

## 🏗️ Architecture

```
┌──────────────────────────┐    ┌─────────────────────────────────────┐
│   AWS S3 / Azure ADLS /  │    │   KAFKA TOPICS                      │
│   Google Cloud Storage   │    │   (Real-time Streams)               │
│   (Auto Loader)          │    │   • <source>-audit-logs             │
│   /Volumes/<catalog>/    │    │                                     │
│   logs/<source>/         │    │                                     │
│   <source_type>/         │    │                                     │
└────────────┬─────────────┘    └──────────────┬──────────────────────┘
             │                                 │
             └────────────┬────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   🥉 BRONZE           |
              │   Raw Ingestion       │
              │   • Variant column    │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   🥈 SILVER           │
              │   Parsed & Validated  │
              │   • Extract fields    │
              └───────────┬───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   🥇 GOLD             │
              │   OCSF Normalized     │
              │   • 6 unified tables  │
              └───────────────────────┘
```

---

## 📂 Repository Structure

```
e2e-cyber-lakehouse/
├── transformations/
│   ├── pipelines/
│   │   └── <source>/<source_type>/
│   │       ├── bronze_<source>_<source_type>.py
│   │       └── silver_<source>_<source_type>.py
│   │
│   └── mappings/
│       └── ocsf/iam/
│           ├── gold_<source>_<source_type>.py
│           └── gold_ocsf_iam_event_classes.py  # Unified tables
│
├── utilities/
│   └── utils.py
│
└── _resources/
    ├── PIPELINE_OVERVIEW.md
    └── OCSF_ARCHITECTURE.md
```

---

## 📊 Unity Catalog Structure

```
<catalog>
├── <source>                        # Per-source databases
│   ├── <source>_<source_type>_brz
│   └── <source>_<source_type>_slv
│
└── ocsf                            # OCSF Gold database
    ├── ocsf_iam_account_change         # 3001
    ├── ocsf_iam_authentication         # 3002
    ├── ocsf_iam_authorize_session      # 3003
    ├── ocsf_iam_entity_management      # 3004
    ├── ocsf_iam_user_access_management # 3005
    └── ocsf_iam_group_management       # 3006
```

---

## 🔄 Pipeline Layers

### 🥉 Bronze - Raw Ingestion
- Ingest raw JSON with variant column (`data`)
- Minimal transformation, preserve original
- Extract metadata (_event_time, _event_date, _source)

### 🥈 Silver - Parsed & Validated
- Extract and flatten key fields from variant
- Data quality validation
- Source-specific schemas

### 🥇 Gold - OCSF Normalized
- Transform to OCSF v1.7.0 IAM schema
- 6 unified tables (one per OCSF class)
- Multi-source append flows (GitHub + Slack + Atlassian → single table)
- SIEM-ready output

---

## 💾 Ingestion Patterns

### Auto Loader (Batch/Micro-batch)

```python
from pyspark import pipelines as sdp

@sdp.table(
    name="<source>_<source_type>_brz",
    cluster_by=["_event_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("singleVariantColumn", "data")
        .load("/Volumes/<catalog>/logs/<source>/<source_type>/")
        .selectExpr(
            "CAST(try_variant_get(data, '$.timestamp', 'BIGINT') AS TIMESTAMP) as _event_time",
            "CAST(_event_time AS DATE) as _event_date",
            "'<source>' as _source",
            "'<source_type>' as _source_type",
            "data"
        )
    )
```

**Note**: SDP handles checkpointing and schema evolution automatically.

### Kafka (Real-time Streaming)

#### Configuration (Confluent Cloud Example)

```python
from pyspark import pipelines as sdp

# Confluent Kafka settings
confluent_bootstrap_servers = '<cluster-id>.us-east-1.aws.confluent.cloud:9092'
confluent_kafka_api_key = dbutils.secrets.get(scope='<scope>', key='<api-key>')
confluent_kafka_secret_key = dbutils.secrets.get(scope='<scope>', key='<secret-key>')

kafka_conf = {
    'kafka.bootstrap.servers': confluent_bootstrap_servers,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.jaas.config': f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{confluent_kafka_api_key}' password='{confluent_kafka_secret_key}';",
    'kafka.ssl.endpoint.identification.algorithm': 'https',
    'kafka.sasl.mechanism': 'PLAIN',
    'startingOffsets': 'earliest',
    'failOnDataLoss': 'false',
}

topic_name = "<topic-name>"
```

#### Bronze Options

**Option 1: Binary (Raw)**
```python
@sdp.table(name="binary_logs_brz", cluster_by=["_event_date"])
def kafka_bronze_binary():
    return spark.readStream.format("kafka").option("subscribe", topic_name).options(**kafka_conf).load()
```

**Option 2: String (Decoded)**
```python
@sdp.table(name="string_logs_brz", cluster_by=["_event_date"])
def kafka_bronze_string():
    return (
        spark.readStream.format("kafka").option("subscribe", topic_name).options(**kafka_conf).load()
        .selectExpr("cast(key as string) as key", "cast(value as string) as value", "topic", "partition", "offset", "timestamp")
    )
```

**Option 3: Variant (Recommended)**
```python
@sdp.table(name="variant_logs_brz", cluster_by=["_event_date"])
def kafka_bronze_variant():
    return (
        spark.readStream.format("kafka").option("subscribe", topic_name).options(**kafka_conf).load()
        .selectExpr(
            "cast(key as string) as key",
            "from_json(cast(value as string), 'variant') as data",  # Parse to variant
            "topic", "partition", "offset", "timestamp"
        )
    )
```

**Recommendation**: Use **Variant** for audit logs - provides schema-on-read flexibility with `try_variant_get()`.

---

## 🛡️ OCSF IAM Event Classes

| OCSF Class | UID | Purpose | Sources |
|------------|-----|---------|---------|
| Account Change | 3001 | User lifecycle | GitHub, Slack, Atlassian |
| Authentication | 3002 | Login/logout | GitHub, Slack, Atlassian |
| Authorize Session | 3003 | Access authorization | GitHub, Slack, Atlassian |
| Entity Management | 3004 | Resource lifecycle | Atlassian only |
| User Access Management | 3005 | Permission management | GitHub, Slack |
| Group Management | 3006 | Group/team operations | GitHub, Slack, Atlassian |

**Total**: 15 append flows (5 GitHub + 5 Slack + 5 Atlassian) → 6 unified tables

**OCSF Category**: Identity & Access Management (UID: 3)  
**OCSF Version**: 1.7.0  
**Docs**: https://schema.ocsf.io/1.7.0/categories/iam

---

## 🔑 Key Technologies

- **[Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/4.1.0-preview1/declarative-pipelines-programming-guide.html)** - Declarative streaming ETL
- **Auto Loader / Kafka** - Batch or real-time ingestion
- **Delta Lake** - ACID transactions, auto-optimization
- **Unity Catalog** - Data governance
- **Variant Data Type** - Schema-on-read for JSON
- **OCSF v1.7.0** - Open security schema standard
