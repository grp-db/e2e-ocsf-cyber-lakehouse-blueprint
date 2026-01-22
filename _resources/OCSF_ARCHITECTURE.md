# OCSF IAM Architecture

## 🎯 What is OCSF?

**OCSF (Open Cybersecurity Schema Framework)** - Open standard for security event normalization

**Key Benefits:**
- Vendor-agnostic security data representation
- Consistent schemas across tools and platforms  
- SIEM-ready (Splunk, Sentinel, Chronicle, etc.)
- Cross-source threat detection and correlation

**Version**: 1.7.0 | **Site**: https://schema.ocsf.io/

---

## 📊 OCSF Categories

OCSF organizes events into **categories**. This project uses **Category 3 (IAM)** for audit logs because they track authentication, authorization, account lifecycle, groups, and resource access.

| Category | UID | Purpose | Common Sources |
|----------|-----|---------|----------------|
| **System Activity** | 1 | OS/host events | EDR, syslog, Windows logs |
| **Findings** | 2 | Alerts & detections | SIEM, vulnerability scanners |
| **Identity & Access Management** | 3 | Auth, authz, accounts | **Audit logs**, SSO, Active Directory |
| **Network Activity** | 4 | Network communications | Firewalls, proxies, flow logs |
| **Discovery** | 5 | Inventory & config | Asset mgmt, scanners, CMDB |
| **Application Activity** | 6 | App-specific events | Web apps, SaaS, APIs |
| **Remediation** | 7 | Response actions | SOAR, ticketing systems |

<details>
<summary><b>Example Event Classes by Category</b> (click to expand)</summary>

**System Activity (UID 1)**: Process Activity (1007), File System Activity (1001), Module Activity (1005), Memory Activity (1006), Kernel Activity (1003), Scheduled Job Activity (1004)

**Findings (UID 2)**: Detection Finding (2004), Security Finding (2001), Vulnerability Finding (2002), Compliance Finding (2003), Incident Finding (2005)

**Identity & Access Management (UID 3)**: Account Change (3001), Authentication (3002), Authorize Session (3003), Entity Management (3004), User Access Management (3005), Group Management (3006)

**Network Activity (UID 4)**: Network Activity (4001), HTTP Activity (4002), DNS Activity (4003), Email Activity (4009), SSH Activity (4007)

**Discovery (UID 5)**: Device Inventory (5001), Config State (5002), User Inventory (5004)

**Application Activity (UID 6)**: Web Resources Activity (6001), API Activity (6003), File Hosting (6005)

**Remediation (UID 7)**: File Remediation (7001), Process Remediation (7002), Network Remediation (7003)

</details>

---

## 🏗️ IAM Event Classes (Category 3)

| OCSF Class | UID | Maps From | Key Fields |
|------------|-----|-----------|------------|
| **Account Change** | 3001 | User create/update/delete events | actor, user, activity_id |
| **Authentication** | 3002 | Login/logout events | actor, src_endpoint, status_id |
| **Authorize Session** | 3003 | Permission/access grants | actor, resource, privileges |
| **Entity Management** | 3004 | Workspace/project lifecycle | actor, resource, activity_id |
| **User Access Management** | 3005 | Permission changes | actor, user, resource |
| **Group Management** | 3006 | Team/group operations | actor, group, user |

**Architecture**: Multiple sources → 6 unified OCSF tables (one per event class)

---

## 💡 The OCSF Advantage

### Without OCSF: Data Management Chaos
```
❌ github_audit_logs          slack_audit_logs          atlassian_audit_logs
   ├─ actor (string)           ├─ user.id (struct)       ├─ actor.accountId (struct)
   ├─ action (string)          ├─ action (string)        ├─ action (string)
   └─ created_at (long)        └─ date_create (long)     └─ created (string)

   Problems:
   • Schema fragmentation = Different schemas, complex JOINs, manual field mapping
   • Duplicate data copies = Intermediate views/tables to normalize fields (10x+ data volume)
   • Object sprawl = Duplicate views per use case (logins_unified, failed_logins, etc.)
   • Query performance = Full scans across 3+ tables, expensive unions, repeated transformations
   • Pipeline jungle = Separate ETL per source, duplicate logic, brittle dependencies
   • Team silos = Each team builds own pipelines/tables, no standardization
   • Governance nightmare = Who owns what? Which table is source of truth?
   • High costs = Storage bloat from duplicates + compute for repeated transformations
   • Time drain = Engineers maintain 100+ objects instead of building features
   • Hard to scale = Every new source multiplies complexity
```

### With OCSF: Unified Event Classes
```
✅ ocsf.authentication (3002)
   ├─ actor (STRUCT), time (TIMESTAMP), src_endpoint (STRUCT)
   ├─ category_uid: 3, class_uid: 3002, activity_id, severity_id
   └─ _source: github | slack | atlassian

   Benefits:
   ✓ Single schema = One standardized table per event type
   ✓ No duplicate data = Source data mapped once, no intermediate copies
   ✓ Minimal objects = 6 tables (vs 100+ views/tables in traditional approach)
   ✓ Query performance = Single table scans, standard indexes, no unions
   ✓ Clean pipelines = Reusable mapping functions, single ETL pattern
   ✓ Team collaboration = All teams use same tables, no silos
   ✓ Clear governance = Single source of truth per event class
   ✓ Cost savings = Less storage + compute (no duplicates, efficient queries)
   ✓ Time savings = Maintain 6 tables instead of 100+, engineers focus on features
   ✓ Infinite scale = 1 table stores unlimited sources (GitHub, Slack, Okta, Azure AD, ...)
```

**Key Insight**: Transform "data management chaos" into "unified security analytics"

---

## 📚 OCSF Schema References

- [IAM Category](https://schema.ocsf.io/1.7.0/categories/iam) - Overview
- [Account Change (3001)](https://schema.ocsf.io/1.7.0/classes/account_change) - User lifecycle
- [Authentication (3002)](https://schema.ocsf.io/1.7.0/classes/authentication) - Login/logout (requires `src_endpoint`)
- [Authorize Session (3003)](https://schema.ocsf.io/1.7.0/classes/authorize_session) - Authorization (requires `resource`)
- [Entity Management (3004)](https://schema.ocsf.io/1.7.0/classes/entity_management) - Resource lifecycle (requires `resource`)
- [User Access Management (3005)](https://schema.ocsf.io/1.7.0/classes/user_access_management) - Permissions (requires `user`, `resource`)
- [Group Management (3006)](https://schema.ocsf.io/1.7.0/classes/group_management) - Groups (requires `group`)

**Implementation Note**: Keep core OCSF fields consistent (category_uid, class_uid, activity_id, severity_id, time, actor). Delta Lake handles schema evolution for source-specific fields.

> **⚠️ Sample Data Limitations**: The raw audit logs in `_raw_logs/` are AI-generated samples based on Atlassian, Slack, and GitHub audit log API documentation. They are simplified for demonstration purposes. Production logs often include additional fields:
> - **Email addresses** for target users in group/permission changes
> - **Detailed metadata** in entity/resource objects
> - **Extended authentication** details (MFA, device info)
> 
> The transformation logic is production-ready and follows OCSF 1.7.0 specs. Mappings set optional fields to NULL when source data is unavailable—adjust based on your actual log structure.
