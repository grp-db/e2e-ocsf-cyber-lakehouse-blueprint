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
e2e-ocsf-cyber-lakehouse-blueprint/
├── transformations/
│   ├── pipelines/                              # Bronze & Silver layers
│   │   ├── github/audit_logs/
│   │   │   ├── bronze_github_audit_logs.py         # Auto Loader ingestion
│   │   │   └── silver_github_audit_logs.py         # Variant JSON parsing
│   │   ├── slack/audit_logs/
│   │   │   ├── bronze_slack_audit_logs.py
│   │   │   └── silver_slack_audit_logs.py
│   │   └── atlassian/audit_logs/
│   │       ├── bronze_atlassian_audit_logs.py
│   │       └── silver_atlassian_audit_logs.py
│   │
│   └── mappings/ocsf/iam/                      # Gold layer (OCSF)
│       ├── __init__.py
│       ├── gold_github_audit_logs.py               # 5 OCSF transformations
│       ├── gold_slack_audit_logs.py                # 5 OCSF transformations
│       ├── gold_atlassian_audit_logs.py            # 5 OCSF transformations
│       └── gold_ocsf_iam_event_classes_delta_sinks.py  # 6 OCSF Delta sinks
│
├── utilities/
│   ├── __init__.py
│   ├── utils.py                                # Shared constants
│   ├── pre_setup_ocsf_tables.py                # Pre-pipeline: Create OCSF tables with minimal schema
│   └── post_setup_ocsf_tables.py               # Post-pipeline: Add liquid clustering
│
├── _resources/
│   ├── OCSF_ARCHITECTURE.md
│   └── PIPELINE_OVERVIEW.md
│
├── _images/
│   └── pipeline_graph.png
│
└── _raw_logs/                                  # AI-generated samples
    ├── github-audit-logs.json
    ├── slack-audit-logs.json
    └── atlassian-audit-logs.json
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
    ├── ocsf_iam_user_access            # 3005
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
- **Uses Delta Lake sinks** (not streaming tables) to enable multiple pipelines writing to same table
- SIEM-ready output
- **Pre-setup**: Run `utilities/pre_setup_ocsf_tables.py` before first pipeline run to create all databases and OCSF tables with minimal schema (time column)
- **Post-setup**: Run `utilities/post_setup_ocsf_tables.py` after pipeline run to add liquid clustering

---

## 💾 Ingestion Patterns

### Auto Loader (Micro-batch Streaming)

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
| User Access | 3005 | Permission management | GitHub, Slack |
| Group Management | 3006 | Group/team operations | GitHub, Slack, Atlassian |

**Total**: 15 append flows (5 GitHub + 5 Slack + 5 Atlassian) → 6 unified tables

**OCSF Category**: Identity & Access Management (UID: 3)  
**OCSF Version**: 1.7.0  
**Docs**: https://schema.ocsf.io/1.7.0/categories/iam

### 🔄 Multi-Source Write Pattern (Unified Tables)

**Challenge**: Multiple sources need to write to the **same unified OCSF table** (e.g., GitHub, Slack, and Atlassian all write to `ocsf_iam_account_change`).

**SDP Streaming Table Limitation**: A streaming table can only be written to by the pipeline that created it. If you try to write to it from another pipeline, you'll get an error.

**Solution**: Use **SDP Delta sinks** with `@sdp.append_flow(target="sink_name")`. Multiple append flows (one per source) write to the **same sink**, which outputs to one unified Delta table.

**Architecture**:
- **Total sinks**: 6 (one per OCSF class)
- **Total tables**: 6 (one unified table per OCSF class - contains all sources)
- **Total append flows**: 15 (multiple sources writing to each sink)
- **Result**: Single table contains data from all sources (query once, get all sources)

**Implementation**:
```python
from utilities.utils import CATALOG, DATABASES, OCSF_TABLES

# Create ONE sink per OCSF table
# mergeSchema enables dynamic schema evolution from multiple sources
sdp.create_sink(
    name=OCSF_TABLES['account_change'],
    format="delta",
    options={
        "tableName": f"{CATALOG}.{DATABASES['ocsf']}.{OCSF_TABLES['account_change']}",
        "mergeSchema": "true"
    }
)

# Multiple append flows target the SAME sink
@sdp.append_flow(name="github_account_change", target=OCSF_TABLES['account_change'])
def github_account_change():
    return transform_github_to_account_change(
        spark.readStream.table(f"{CATALOG}.github.github_audit_logs_slv")
    )

@sdp.append_flow(name="slack_account_change", target=OCSF_TABLES['account_change'])
def slack_account_change():
    return transform_slack_to_account_change(
        spark.readStream.table(f"{CATALOG}.slack.slack_audit_logs_slv")
    )

@sdp.append_flow(name="atlassian_account_change", target=OCSF_TABLES['account_change'])
def atlassian_account_change():
    return transform_atlassian_to_account_change(
        spark.readStream.table(f"{CATALOG}.atlassian.atlassian_audit_logs_slv")
    )
```

**Result**: Multiple streaming queries (append flows) write to the same sink, which outputs to one unified Delta table.

**Query Example** (Unified Table):
```sql
-- Single query returns data from ALL sources
SELECT _source, class_name, actor.user.name, COUNT(*) 
FROM grp.ocsf.ocsf_iam_account_change
GROUP BY _source, class_name, actor.user.name;

-- Results:
-- _source    | class_name      | name         | count
-- github     | Account Change  | octocat      | 45
-- slack      | Account Change  | john.doe     | 32
-- atlassian  | Account Change  | admin@corp   | 18
```

No JOINs needed - it's one unified table! 🎯

---

## 🔑 Key Technologies

- **[Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/4.1.0-preview1/declarative-pipelines-programming-guide.html)** - Declarative streaming ETL
- **Auto Loader / Kafka** - Batch or real-time ingestion
- **Delta Lake** - ACID transactions, auto-optimization
- **Unity Catalog** - Data governance
- **Variant Data Type** - Schema-on-read for JSON
- **OCSF v1.7.0** - Open security schema standard
