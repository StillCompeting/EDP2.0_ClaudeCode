# QNXT Enterprise Data Platform (EDP) 2.0

## Project Walkthrough

This document provides a comprehensive guide to the QNXT EDP dbt project, which transforms raw healthcare claims data into an analytics-ready data warehouse using the medallion architecture pattern.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Directory Structure](#directory-structure)
4. [Data Sources](#data-sources)
5. [Medallion Layers](#medallion-layers)
6. [Key Models](#key-models)
7. [MDM Integration](#mdm-integration)
8. [Testing Strategy](#testing-strategy)
9. [Getting Started](#getting-started)
10. [Common Operations](#common-operations)

---

## Project Overview

The QNXT EDP project is a dbt-based data transformation layer that:

- **Ingests** raw healthcare data from QNXT claims processing system
- **Transforms** it through Bronze → Silver → Gold medallion layers
- **Integrates** with MDM (Master Data Management) for identity resolution
- **Delivers** analytics-ready dimensions and facts for reporting

### Technology Stack

| Component | Technology |
|-----------|------------|
| Data Warehouse | Snowflake |
| Transformation | dbt Core 1.10+ |
| Testing | dbt-expectations |
| Date Utils | dbt-date |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MEDALLION ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         RAW DATA SOURCES                            │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │   │
│  │  │  RAW_QNXT   │  │  RAW_MDM    │  │  (Future)   │                 │   │
│  │  │  - member   │  │  - golden_  │  │  - LABS     │                 │   │
│  │  │  - mbrelig  │  │    member   │  │  - CRM      │                 │   │
│  │  │  - clclaim  │  │  - golden_  │  │  - CAQH     │                 │   │
│  │  │  - clline   │  │    provider │  │             │                 │   │
│  │  │  - provider │  │  - cross-   │  │             │                 │   │
│  │  │  - diagcode │  │    walks    │  │             │                 │   │
│  │  └──────┬──────┘  └──────┬──────┘  └─────────────┘                 │   │
│  └─────────┼────────────────┼───────────────────────────────────────────┘   │
│            │                │                                               │
│            ▼                ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    BRONZE LAYER (Staging)                           │   │
│  │  Schema: BRONZE  |  Materialization: Views                          │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ QNXT Staging          │ MDM Staging                          │   │   │
│  │  │ - stg_qnxt__member    │ - stg_mdm__golden_member             │   │   │
│  │  │ - stg_qnxt__mbrelig   │ - stg_mdm__golden_provider           │   │   │
│  │  │ - stg_qnxt__clclaim   │ - stg_mdm__member_crosswalk          │   │   │
│  │  │ - stg_qnxt__clline    │ - stg_mdm__provider_crosswalk        │   │   │
│  │  │ - stg_qnxt__provider  │                                       │   │   │
│  │  │ - stg_qnxt__diagcode  │                                       │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SILVER LAYER (Core)                              │   │
│  │  Schema: SILVER  |  Materialization: Tables                         │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ Dimensions              │ Facts                              │   │   │
│  │  │ - dim_date              │ - fct_claim                        │   │   │
│  │  │ - dim_member            │ - fct_claim_conformed             │   │   │
│  │  │ - dim_golden_member     │                                    │   │   │
│  │  │ - dim_provider          │                                    │   │   │
│  │  │ - dim_golden_provider   │                                    │   │   │
│  │  │ - dim_diagnosis         │                                    │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     GOLD LAYER (Marts)                              │   │
│  │  Schema: GOLD  |  Materialization: Tables                           │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ Claims       │ Membership      │ Finance                    │   │   │
│  │  │ - mart_      │ - mart_         │ - (future)                 │   │   │
│  │  │   claims_    │   member_       │                            │   │   │
│  │  │   summary    │   months        │                            │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
qnxt_edp/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt package dependencies
├── profiles.yml.example     # Template for connection profile
│
├── models/
│   ├── docs.md              # dbt docs overview and doc blocks
│   │
│   ├── staging/             # BRONZE LAYER
│   │   ├── _sources.yml     # QNXT source definitions
│   │   ├── _staging.yml     # Staging model tests
│   │   │
│   │   ├── members/         # Member staging models
│   │   │   ├── stg_qnxt__member.sql
│   │   │   └── stg_qnxt__mbrelig.sql
│   │   │
│   │   ├── claims/          # Claims staging models
│   │   │   ├── stg_qnxt__clclaim.sql
│   │   │   ├── stg_qnxt__clline.sql
│   │   │   ├── stg_qnxt__cldiag.sql
│   │   │   ├── stg_qnxt__clproc.sql
│   │   │   └── stg_qnxt__cladjust.sql
│   │   │
│   │   ├── providers/       # Provider staging models
│   │   │   └── stg_qnxt__provider.sql
│   │   │
│   │   ├── reference/       # Reference data staging
│   │   │   └── stg_qnxt__diagcode.sql
│   │   │
│   │   └── mdm/             # MDM staging models
│   │       ├── _mdm_sources.yml
│   │       ├── stg_mdm__golden_member.sql
│   │       ├── stg_mdm__golden_provider.sql
│   │       ├── stg_mdm__member_crosswalk.sql
│   │       └── stg_mdm__provider_crosswalk.sql
│   │
│   └── marts/               # SILVER & GOLD LAYERS
│       ├── core/            # Silver layer dimensions & facts
│       │   ├── dim_date.sql
│       │   ├── dim_member.sql
│       │   ├── dim_golden_member.sql
│       │   ├── dim_provider.sql
│       │   ├── dim_golden_provider.sql
│       │   ├── dim_diagnosis.sql
│       │   ├── fct_claim.sql
│       │   └── fct_claim_conformed.sql
│       │
│       ├── claims/          # Gold layer - claims analytics
│       │   └── mart_claims_summary.sql
│       │
│       └── membership/      # Gold layer - membership analytics
│           └── mart_member_months.sql
│
├── macros/
│   ├── generate_schema_name.sql   # Medallion schema routing
│   └── qnxt_helpers.sql           # QNXT-specific transformations
│
├── seeds/                   # Sample/reference data
│   ├── sample_*.csv         # QNXT sample data (10 files)
│   ├── mdm_*.csv            # MDM sample data (4 files)
│   └── claim_status_mapping.csv
│
├── snapshots/               # SCD Type 2 tracking
│   ├── member_snapshot.sql
│   ├── eligibility_snapshot.sql
│   └── provider_snapshot.sql
│
└── docs/
    └── MDM_WALKTHROUGH.md   # MDM integration documentation
```

---

## Data Sources

### QNXT Source Tables

| Table | Description |
|-------|-------------|
| `member` | Core member demographics |
| `mbrelig` | Member eligibility segments |
| `clclaim` | Claim headers |
| `clline` | Claim line details |
| `cldiag` | Claim diagnosis codes |
| `clproc` | Claim procedure codes |
| `cladjust` | Claim adjustments |
| `provider` | Provider master data |
| `diagcode` | Diagnosis code reference |
| `proccode` | Procedure code reference |

### MDM Source Tables

| Table | Description |
|-------|-------------|
| `golden_member` | MDM-mastered member records |
| `golden_provider` | MDM-mastered provider records |
| `member_crosswalk` | Source member ID → golden ID mapping |
| `provider_crosswalk` | Source provider ID → golden ID mapping |

---

## Medallion Layers

### Bronze Layer (Staging)

**Purpose**: Clean, type, and standardize raw source data

**Characteristics**:
- Materialized as **views** for real-time freshness
- Applies data type conversions via macros
- Handles NULL/empty string cleanup
- Generates surrogate keys where needed
- Tagged with `bronze`, `staging`

**Naming Convention**: `stg_{source}__{table}`

### Silver Layer (Core)

**Purpose**: Business logic, dimensions, and facts

**Characteristics**:
- Materialized as **tables** for performance
- Applies business rules and derivations
- Creates conformed dimensions
- Builds fact tables with proper grain
- Tagged with `silver`, `core`

**Key Models**:
- `dim_date` - Calendar dimension
- `dim_member` - Member dimension with current eligibility
- `dim_golden_member` - MDM-mastered member dimension
- `fct_claim` - Claims fact with source IDs
- `fct_claim_conformed` - Claims fact with golden IDs

### Gold Layer (Marts)

**Purpose**: Analytics-ready aggregations

**Characteristics**:
- Materialized as **tables**
- Pre-aggregated for common queries
- Business-friendly naming
- Tagged with `gold`

**Key Models**:
- `mart_claims_summary` - Monthly claims aggregations
- `mart_member_months` - Member-month enrollment calculations

---

## Key Models

### dim_member
Combines member demographics with current eligibility status. Includes:
- Derived age calculations
- Current eligibility flag
- Line of business
- Surrogate key generation

### dim_golden_member
Extension of dim_member using MDM golden records for stable identity resolution. Enriched with QNXT data via crosswalk.

### fct_claim
Core claims fact table at claim header grain. Includes:
- Member and provider foreign keys
- Financial amounts (billed, allowed, paid)
- Claim status and type
- Service date ranges
- Line count aggregations

### fct_claim_conformed
Claims fact using MDM golden IDs as foreign keys. Includes resolution flags to indicate successful identity matching.

---

## MDM Integration

See [MDM_WALKTHROUGH.md](MDM_WALKTHROUGH.md) for detailed documentation.

### Summary

The MDM integration provides:
- **Golden Records**: Single source of truth for member/provider identity
- **Crosswalks**: Map source system IDs to golden IDs
- **Conformed Facts**: Use stable golden IDs for analytics

---

## Testing Strategy

### Test Categories

| Type | Package | Example |
|------|---------|---------|
| Not Null | dbt core | Primary/foreign keys |
| Unique | dbt core | Primary keys |
| Relationships | dbt core | Referential integrity |
| Accepted Values | dbt core | Status codes, gender |
| Type Validation | dbt_expectations | Date/numeric fields |
| Range Validation | dbt_expectations | Age 0-120, amounts >= 0 |
| Date Ordering | dbt_expectations | start_date <= end_date |

### Running Tests

```bash
# Run all 140 tests
dbt test

# Run tests for specific model
dbt test --select dim_member

# Run only source tests
dbt test --select source:qnxt
```

---

## Getting Started

### Prerequisites

1. Snowflake account with appropriate permissions
2. dbt Core 1.10+ installed
3. Python 3.8+

### Setup

```bash
# Clone repository
git clone https://github.com/StillCompeting/EDP2.0_ClaudeCode.git
cd EDP2.0_ClaudeCode/qnxt_edp

# Install dependencies
dbt deps

# Configure connection (copy and edit)
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials

# Verify connection
dbt debug
```

### Initial Build

```bash
# Load sample data
dbt seed --full-refresh

# Build all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## Common Operations

### Full Refresh

```bash
dbt seed --full-refresh
dbt run --full-refresh
dbt test
```

### Incremental Run

```bash
dbt run
dbt test
```

### Build Specific Layer

```bash
# Bronze only
dbt run --select tag:bronze

# Silver only
dbt run --select tag:silver

# Gold only
dbt run --select tag:gold
```

### Build with Dependencies

```bash
# Build a model and everything upstream
dbt run --select +fct_claim_conformed

# Build a model and everything downstream
dbt run --select dim_member+
```

### Snapshots

```bash
# Run all snapshots (SCD Type 2)
dbt snapshot
```

### Documentation

```bash
dbt docs generate
dbt docs serve --port 8080
```

---

## Schema Reference

| Schema | Purpose | Models |
|--------|---------|--------|
| `BRONZE` | Staging views | stg_* models |
| `BRONZE_RAW_QNXT` | QNXT source seeds | Sample data |
| `BRONZE_RAW_MDM` | MDM source seeds | Golden records |
| `SILVER` | Core dimensions/facts | dim_*, fct_* |
| `GOLD` | Business marts | mart_* |

---

## Support

For questions or issues, contact the data engineering team or open an issue in the repository.
