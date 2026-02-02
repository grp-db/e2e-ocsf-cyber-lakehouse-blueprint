# E2E OCSF Cyber Lakehouse Blueprint

> **Databricks Cyber Accelerator** - Built by Databricks Professional Services to accelerate cybersecurity lakehouse implementations for customers

**Medallion Architecture** (Bronze → Silver → Gold) that transforms audit logs from GitHub, Slack, and Atlassian into **OCSF 1.7.0 IAM-normalized** security events.

## 🎯 What This Does

Unifies audit logs from multiple sources into **6 OCSF IAM tables** for cross-platform security analytics and SIEM integration.

**Problem**: Schema chaos, duplicate data, 100+ tables, pipeline jungles  
**Solution**: 6 unified tables, single source of truth, one query across all sources

## 🏗️ Pipeline Architecture

![Pipeline Architecture](_images/pipeline_graph.png)

*Pipeline output using sample audit log files from `_raw_logs/` folder*

**15 Append Flows** → **6 Unified OCSF Tables** (Delta Lake Sinks)

### Why Delta Lake Sinks for OCSF Tables?

OCSF tables use **SDP Delta sinks** instead of streaming tables because:

- **Multiple sources write to the same unified table** (e.g., GitHub, Slack, and Atlassian all write to `ocsf_iam_account_change`)
- **SDP streaming tables limitation**: Only the pipeline that creates a streaming table can write to it
- **Delta sinks enable multi-source writes**: Multiple append flows (one per source) write to the same sink, which outputs to one unified Delta table

**Architecture Pattern**:
```
Bronze/Silver: SDP Streaming Tables (single pipeline per table)
Gold (OCSF):   6 Sinks + 15 Append Flows → 6 Unified Tables
               
Example: ocsf_iam_account_change (ONE SINK, ONE TABLE)
  ├─ github_account_change (append flow)    ──┐
  ├─ slack_account_change (append flow)     ──┼─→ ocsf_iam_account_change (sink)
  └─ atlassian_account_change (append flow) ──┘  └─→ grp.ocsf.ocsf_iam_account_change (table)
```

**Result**: Query `SELECT * FROM ocsf_iam_account_change` returns data from **all sources** (use `_source` column to filter by source)

> **📝 Note on Pipeline Design**: This example demonstrates all transformations within a single SDP pipeline file (`gold_ocsf_iam_event_classes_delta_sinks.py`). For enterprise deployments, consider separating this into multiple SDP pipelines per source (e.g., `github_to_ocsf_pipeline.py`, `slack_to_ocsf_pipeline.py`, `atlassian_to_ocsf_pipeline.py`) for improved data management architecture, independent deployment, and fault isolation.

> **⚠️ Note on Sample Data**: The raw logs in `_raw_logs/` are AI-generated samples based on Atlassian, Slack, and GitHub audit log API documentation. They are simplified for demonstration purposes. Production audit logs typically contain additional fields (e.g., target user emails, detailed entity metadata). The transformation logic is production-ready—adjust field mappings based on your actual data structure.

> **🔧 Deployment Steps**: SDP does not support DDL commands within pipeline definitions. Run these setup scripts in Databricks notebooks:
> 1. **Before pipelines**: `utilities/pre_setup_ocsf_tables.py` - Creates all databases (github, slack, atlassian, ocsf) and OCSF tables with minimal schema (time column)
> 2. **Run your pipelines**: Bronze → Silver → Gold
>    - Bronze/Silver tables: Auto-created by `@sdp.table` decorators
>    - Gold OCSF tables: Auto-populated by Delta sinks with schema evolution (`mergeSchema: true`)
> 3. **After pipelines**: `utilities/post_setup_ocsf_tables.py` - Adds liquid clustering for query optimization

## 📂 Repository Structure

```
e2e-ocsf-cyber-lakehouse-blueprint/
│
├── transformations/
│   ├── pipelines/                              # Bronze & Silver layers
│   │   ├── github/audit_logs/
│   │   │   ├── bronze_github_audit_logs.py         # Auto Loader ingestion
│   │   │   └── silver_github_audit_logs.py         # JSON parsing with Variant
│   │   ├── slack/audit_logs/
│   │   │   ├── bronze_slack_audit_logs.py          # Auto Loader ingestion
│   │   │   └── silver_slack_audit_logs.py          # JSON parsing with Variant
│   │   └── atlassian/audit_logs/
│   │       ├── bronze_atlassian_audit_logs.py      # Auto Loader ingestion
│   │       └── silver_atlassian_audit_logs.py      # JSON parsing with Variant
│   │
│   └── mappings/ocsf/iam/                      # Gold layer (OCSF normalization)
│       ├── __init__.py
│       ├── gold_github_audit_logs.py               # GitHub → OCSF transformations
│       ├── gold_slack_audit_logs.py                # Slack → OCSF transformations
│       ├── gold_atlassian_audit_logs.py            # Atlassian → OCSF transformations
│       └── gold_ocsf_iam_event_classes_delta_sinks.py  # Creates 6 OCSF Delta sinks
│
├── utilities/
│   ├── __init__.py
│   ├── utils.py                                # Shared constants (catalog, databases, etc.)
│   ├── pre_setup_ocsf_tables.py                # Pre-pipeline: Create OCSF tables with minimal schema
│   └── post_setup_ocsf_tables.py               # Post-pipeline: Add liquid clustering
│
├── _resources/
│   ├── OCSF_ARCHITECTURE.md                    # OCSF overview, categories, IAM classes
│   └── PIPELINE_OVERVIEW.md                    # Pipeline patterns, ingestion examples
│
├── _images/
│   └── pipeline_graph.png                      # Pipeline visualization screenshot
│
└── _raw_logs/                                  # AI-generated sample audit logs
    ├── github-audit-logs.json
    ├── slack-audit-logs.json
    └── atlassian-audit-logs.json
```

## 🛡️ OCSF IAM Event Classes

6 unified tables mapping 15 source flows:

| OCSF Class | UID | Sources |
|------------|-----|---------|
| **account_change** | 3001 | GitHub, Slack, Atlassian |
| **authentication** | 3002 | GitHub, Slack, Atlassian |
| **authorize_session** | 3003 | GitHub, Slack, Atlassian |
| **entity_management** | 3004 | Atlassian only |
| **user_access** | 3005 | GitHub, Slack |
| **group_management** | 3006 | GitHub, Slack, Atlassian |

**OCSF Version**: 1.7.0 | **Category**: IAM (UID 3) | **Docs**: https://schema.ocsf.io/1.7.0/categories/iam

---

**Tech Stack Built with 💜**: OCSF v1.7.0 📖 • Databricks 🚀 • Spark Declarative Pipelines 🧠 • Spark Streaming 🌊 • Auto Loader ⚓ • Delta Lake 🏞️ • Unity Catalog 📚

---

## 📝 About This Accelerator

This is a **Databricks Cyber Accelerator** built by Databricks Professional Services to help customers rapidly implement OCSF-normalized security data pipelines on the Databricks platform. Customers are encouraged to clone, modify, and extend this solution to meet their specific requirements.
