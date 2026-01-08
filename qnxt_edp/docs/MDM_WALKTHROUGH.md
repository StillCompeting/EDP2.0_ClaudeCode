# MDM Golden Record Integration Walkthrough

## Overview

This document describes the **Master Data Management (MDM) Golden Record Integration** implemented in the QNXT EDP dbt project. The integration provides stable, mastered identities for members and providers, enabling consistent analytics across multiple source systems.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MDM INTEGRATION PATTERN                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│   │    QNXT      │    │    LABS      │    │     CRM      │   Source        │
│   │   Members    │    │   Patients   │    │   Contacts   │   Systems       │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                 │
│          │                   │                   │                          │
│          └───────────────────┼───────────────────┘                          │
│                              ▼                                              │
│                    ┌─────────────────┐                                      │
│                    │   MDM System    │   Identity Resolution               │
│                    │   Reltio/etc    │   & Survivorship                    │
│                    └────────┬────────┘                                      │
│                             │                                               │
│        ┌────────────────────┼────────────────────┐                         │
│        ▼                    ▼                    ▼                         │
│  ┌───────────┐      ┌─────────────┐      ┌─────────────┐                   │
│  │  Golden   │      │   Member    │      │  Provider   │   MDM Outputs     │
│  │  Records  │      │  Crosswalk  │      │  Crosswalk  │                   │
│  └─────┬─────┘      └──────┬──────┘      └──────┬──────┘                   │
│        │                   │                    │                          │
│        └───────────────────┼────────────────────┘                          │
│                            ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     SNOWFLAKE / dbt                                  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────────────┐ │   │
│  │  │dim_golden_  │  │dim_golden_  │  │    fct_claim_conformed       │ │   │
│  │  │   member    │  │  provider   │  │  (uses golden IDs for FK)    │ │   │
│  │  └─────────────┘  └─────────────┘  └──────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. MDM Source Data (Seeds)

Located in `seeds/`:

| File | Description | Snowflake Location |
|------|-------------|-------------------|
| `mdm_golden_member.csv` | Mastered member golden records | `BRONZE_RAW_MDM.golden_member` |
| `mdm_golden_provider.csv` | Mastered provider golden records | `BRONZE_RAW_MDM.golden_provider` |
| `mdm_member_crosswalk.csv` | Maps source member IDs → golden IDs | `BRONZE_RAW_MDM.member_crosswalk` |
| `mdm_provider_crosswalk.csv` | Maps source provider IDs → golden IDs | `BRONZE_RAW_MDM.provider_crosswalk` |

### 2. Staging Models

Located in `models/staging/mdm/`:

| Model | Purpose |
|-------|---------|
| `stg_mdm__golden_member` | Cleans and standardizes golden member records |
| `stg_mdm__golden_provider` | Cleans and standardizes golden provider records |
| `stg_mdm__member_crosswalk` | Processes crosswalk with active/inactive flags |
| `stg_mdm__provider_crosswalk` | Processes provider crosswalk mappings |

### 3. Golden Dimensions

Located in `models/marts/core/`:

| Model | Description |
|-------|-------------|
| `dim_golden_member` | Combines golden records with QNXT enrichment |
| `dim_golden_provider` | Combines golden records with QNXT provider data |

### 4. Conformed Fact Table

| Model | Description |
|-------|-------------|
| `fct_claim_conformed` | Claims fact with golden member/provider IDs resolved |

## Key Features

### Identity Resolution Flags

The `fct_claim_conformed` table includes flags to indicate MDM resolution status:

```sql
-- In fct_claim_conformed
has_golden_member           -- TRUE if member resolved to golden ID
has_golden_billing_provider -- TRUE if billing provider resolved
has_golden_servicing_provider -- TRUE if servicing provider resolved
```

### Match Quality Metrics

Crosswalks include match quality information:

```sql
match_confidence  -- 0-100 score from MDM
match_rule        -- Rule that created the link (e.g., 'SSN_DOB_NAME')
is_primary        -- Whether this is the primary source for the golden record
```

### Multi-Source Support

Crosswalks support multiple source systems:

```sql
source_system     -- 'QNXT', 'LABS', 'CRM', 'NPPES', etc.
source_member_id  -- ID in the original source system
```

## Usage Examples

### Query Claims with Golden IDs

```sql
SELECT
    claim_id,
    member_golden_id,
    billing_provider_golden_id,
    billed_amount,
    paid_amount,
    has_golden_member
FROM PLAYGROUND.SILVER.fct_claim_conformed
WHERE has_golden_member = true;
```

### Find All Source IDs for a Golden Member

```sql
SELECT
    golden_member_id,
    source_system,
    source_member_id,
    match_confidence,
    is_primary
FROM PLAYGROUND.BRONZE.stg_mdm__member_crosswalk
WHERE golden_member_id = 'GM-000001';
```

### Join to Golden Dimension

```sql
SELECT
    c.claim_id,
    m.full_name,
    m.date_of_birth,
    m.current_age,
    c.paid_amount
FROM PLAYGROUND.SILVER.fct_claim_conformed c
JOIN PLAYGROUND.SILVER.dim_golden_member m
    ON c.member_golden_id = m.golden_member_id
WHERE c.has_golden_member = true;
```

## Configuration

### dbt_project.yml Variables

```yaml
vars:
  # MDM source configuration
  mdm_database: 'PLAYGROUND'
  mdm_schema: 'RAW_MDM'
  
  # QNXT source configuration  
  qnxt_database: 'PLAYGROUND'
  qnxt_schema: 'BRONZE_RAW_QNXT'
```

### Schema Routing

The `generate_schema_name` macro routes seeds:
- `+schema: RAW_QNXT` → loads to `BRONZE_RAW_QNXT`
- `+schema: RAW_MDM` → loads to `BRONZE_RAW_MDM`

## Testing

Run all 140 data quality tests:

```bash
dbt test
```

Key MDM-specific tests:
- Primary key uniqueness on golden IDs
- Not-null constraints on crosswalk fields
- Referential integrity between crosswalks and golden records

## Local Development

### Loading Sample Data

```bash
# Load all seeds (including MDM sample data)
dbt seed --full-refresh

# Build MDM models
dbt run --select tag:mdm
dbt run --select dim_golden_member dim_golden_provider fct_claim_conformed
```

### Sample Data

The sample data demonstrates:
- 5 golden members with IDs `GM-000001` through `GM-000005`
- 5 golden providers with IDs `GP-000001` through `GP-000005`
- Member crosswalk mapping `MEM001`-`MEM005` to golden IDs
- Provider crosswalk mapping `PROV001`-`PROV005` to golden IDs
- One cross-system link example (LABS patient → same golden member)

## Future Enhancements

1. **SCD Type 2** - Track historical changes to golden records
2. **Match Score Thresholds** - Filter low-confidence matches
3. **Additional Source Systems** - Expand crosswalks for more sources
4. **Real-time Integration** - Replace seeds with live MDM feeds
5. **Data Quality Dashboards** - Monitor MDM match rates over time
