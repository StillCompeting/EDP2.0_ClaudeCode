{% docs __overview__ %}

# QNXT Enterprise Data Platform (EDP) 2.0

## Overview

This dbt project transforms raw QNXT healthcare claims processing data into a
medallion architecture (Bronze → Silver → Gold) optimized for analytics and reporting
in Snowflake.

## Architecture

### Medallion Layers

| Layer | Schema | Description | Materialization |
|-------|--------|-------------|-----------------|
| **Bronze** | `BRONZE` | Staging models - cleaned and typed source data | Views / Incremental |
| **Silver** | `SILVER` | Core dimensions and facts - business logic applied | Tables |
| **Gold** | `GOLD` | Business marts - aggregated, analytics-ready | Tables |

### Data Flow

```
QNXT Source (RAW_QNXT)          MDM Source (RAW_MDM)
    ↓                               ↓
Bronze Layer (Staging)          Bronze Layer (MDM)
    ├── stg_qnxt__member            ├── stg_mdm__golden_member
    ├── stg_qnxt__mbrelig           ├── stg_mdm__golden_provider
    ├── stg_qnxt__clclaim           ├── stg_mdm__member_crosswalk
    ├── stg_qnxt__clline            └── stg_mdm__provider_crosswalk
    ├── stg_qnxt__cldiag
    ├── stg_qnxt__clproc
    ├── stg_qnxt__cladjust
    └── stg_qnxt__provider
            ↓                           ↓
        Silver Layer (Core) ←──Crosswalk──→
            ├── dim_member (source-aligned)
            ├── dim_golden_member (MDM-mastered)
            ├── dim_provider (source-aligned)
            ├── dim_golden_provider (MDM-mastered)
            ├── dim_date
            ├── fct_claim (source IDs)
            └── fct_claim_conformed (golden IDs)
            ↓
        Gold Layer (Marts)
            ├── Claims Analytics
            ├── Membership Analytics
            └── Financial Analytics
```

## MDM Integration

This project supports Master Data Management (MDM) integration for identity resolution.

### Golden Records

MDM publishes "golden" member and provider records that represent the single source
of truth for entity identity after match/merge processing.

### Crosswalk Tables

Crosswalk tables map source system IDs to golden IDs:
- `member_crosswalk`: QNXT member_id → golden_member_id
- `provider_crosswalk`: QNXT provider_id → golden_provider_id

### Benefits

- **Stable Identity**: Marts stay consistent even when source IDs change
- **Multi-Source Support**: Add new sources (labs, CRM, etc.) without rewriting identity logic
- **Backwards Compatibility**: Legacy dimensions remain available during migration

## Key Entities

### Members
Member demographics and eligibility from QNXT `member` and `mbrelig` tables.
Includes SCD Type 2 tracking via snapshots for historical analysis.

### Claims
Healthcare claims including headers (`clclaim`), line details (`clline`),
diagnosis codes (`cldiag`), procedure codes (`clproc`), and adjustments (`cladjust`).

### Providers
Provider master data including NPIs, specialties, and network affiliations.

## Incremental Processing

Large transaction tables (claims, claim lines, diagnoses) use incremental
materialization with merge strategy based on `_loaded_at` timestamps.

## Testing Strategy

- **Primary Keys**: `not_null` + `unique` on all surrogate and natural keys
- **Referential Integrity**: `relationships` tests between fact and dimension tables
- **Data Quality**: `dbt_expectations` tests for date ranges and amount validations
- **Business Rules**: Custom tests for healthcare-specific validations

{% enddocs %}


{% docs medallion_bronze %}
**Bronze Layer (Staging)**

The Bronze layer contains staging models that:
- Clean and standardize raw QNXT data
- Apply data type conversions
- Handle NULL and empty string values
- Rename columns to business-friendly names
- Preserve source metadata (`_loaded_at`)

Models in this layer are prefixed with `stg_qnxt__`.
{% enddocs %}


{% docs medallion_silver %}
**Silver Layer (Core)**

The Silver layer contains cleansed and conformed data:
- Dimension tables (`dim_*`) with surrogate keys
- Fact tables (`fct_*`) with foreign key relationships
- Business logic applied (status mappings, calculations)
- SCD Type 2 snapshots for historical tracking

Models in this layer follow star schema design patterns.
{% enddocs %}


{% docs medallion_gold %}
**Gold Layer (Marts)**

The Gold layer contains analytics-ready data:
- Pre-aggregated metrics
- Business-specific views
- Denormalized for query performance
- Ready for BI tool consumption

Organized by business domain (claims, membership, finance).
{% enddocs %}


{% docs claim_types %}
**QNXT Claim Type Codes**

| Code | Description |
|------|-------------|
| I | Institutional (UB-04) |
| P | Professional (CMS-1500) |
| D | Dental |
| R | Pharmacy |

{% enddocs %}


{% docs claim_status_codes %}
**QNXT Claim Status Codes**

| Code | Description |
|------|-------------|
| A | Approved - Claim has been approved for payment |
| D | Denied - Claim has been denied |
| P | Pending - Claim is pending adjudication |
| V | Void - Claim has been voided |
| S | Suspended - Claim is suspended for review |
| I | In Process - Claim is being processed |
| R | Rejected - Claim was rejected (edit failures) |

{% enddocs %}


{% docs diagnosis_code_types %}
**Diagnosis Code Types**

| Code | Description |
|------|-------------|
| ICD9 / 9 | ICD-9-CM Diagnosis Code |
| ICD10 / 10 | ICD-10-CM Diagnosis Code |

Note: ICD-10 codes are required for services on or after October 1, 2015.
{% enddocs %}


{% docs poa_indicators %}
**Present on Admission (POA) Indicators**

| Code | Description |
|------|-------------|
| Y | Yes - Diagnosis was present at time of inpatient admission |
| N | No - Diagnosis was not present at time of admission |
| U | Unknown - Documentation insufficient to determine POA status |
| W | Clinically undetermined - Provider unable to clinically determine POA |
| 1 | Unreported/Not used - Exempt from POA reporting |

{% enddocs %}


{% docs member_id %}
Unique identifier for a member in the QNXT system. This is the primary key
used to link members across all related tables (eligibility, claims, etc.).

Format: Alphanumeric, typically 9-15 characters.
{% enddocs %}


{% docs claim_id %}
Unique identifier for a claim in the QNXT system. This links the claim header
to all associated detail records (lines, diagnoses, procedures, adjustments).

Format: Alphanumeric, system-generated.
{% enddocs %}


{% docs provider_id %}
Unique identifier for a provider in the QNXT system. May be different from
the NPI (National Provider Identifier).

Format: Alphanumeric, typically 10-15 characters.
{% enddocs %}


{% docs npi %}
**National Provider Identifier (NPI)**

A unique 10-digit identification number issued to healthcare providers in the
United States by CMS. Required for all HIPAA-covered transactions.

Validation: 10 digits, passes Luhn algorithm check.
{% enddocs %}


{% docs billed_amount %}
The amount billed by the provider for services rendered. This is the provider's
charge before any adjustments, discounts, or contractual allowances are applied.
{% enddocs %}


{% docs allowed_amount %}
The maximum amount the health plan will pay for a covered service. This is
typically based on contracted rates or fee schedules and is usually less than
the billed amount.
{% enddocs %}


{% docs paid_amount %}
The actual amount paid by the health plan to the provider after applying
member cost sharing (copay, coinsurance, deductible) and any other adjustments.
{% enddocs %}


{% docs service_from_date %}
The date when the healthcare service began. For single-day services, this will
equal the service to date. For multi-day services (e.g., inpatient stays),
this is the admission or start date.
{% enddocs %}


{% docs service_to_date %}
The date when the healthcare service ended. For single-day services, this will
equal the service from date. For multi-day services (e.g., inpatient stays),
this is the discharge or end date.
{% enddocs %}


{% docs golden_member_id %}
**MDM Golden Member Identifier**

A unique identifier assigned by the Master Data Management (MDM) system to represent
a single, mastered member identity. This ID remains stable even when source system
IDs change or when multiple source records are merged.

Format: Alphanumeric, prefixed with 'GM-' (e.g., GM-000001)
{% enddocs %}


{% docs golden_provider_id %}
**MDM Golden Provider Identifier**

A unique identifier assigned by the Master Data Management (MDM) system to represent
a single, mastered provider identity. This ID remains stable across source system
changes and merges.

Format: Alphanumeric, prefixed with 'GP-' (e.g., GP-000001)
{% enddocs %}


{% docs mdm_crosswalk %}
**MDM Crosswalk Tables**

Crosswalk tables maintain the relationship between source system identifiers and
MDM golden record identifiers. Key attributes include:

| Column | Description |
|--------|-------------|
| source_system | Code identifying the source (QNXT, LABS, CRM, NPPES, etc.) |
| source_id | The identifier from the source system |
| golden_id | The MDM-assigned golden record identifier |
| match_confidence | Confidence score (0-100) for the identity match |
| match_rule | The MDM rule that created this link |
| is_primary | Whether this source is the "master" for this golden record |

{% enddocs %}

