{% docs stg_qnxt__member %}
**Staged Member Demographics**

Cleansed member demographic data from the QNXT `member` table.

**Key Transformations:**
- Member ID standardized (uppercase, trimmed)
- Date of birth parsed from QNXT format
- Gender codes standardized (M/F/U)
- SSN and other identifiers cleaned
- Status codes mapped to descriptions

**Grain:** One row per member

**Update Frequency:** Daily incremental
{% enddocs %}


{% docs stg_qnxt__mbrelig %}
**Staged Member Eligibility**

Member eligibility segments from the QNXT `mbrelig` table.

**Key Transformations:**
- Effective and termination dates parsed
- Line of business codes mapped
- Plan and product information joined
- Coverage type derived

**Grain:** One row per member eligibility segment

**Note:** Members may have multiple eligibility segments over time
(gaps, plan changes, etc.)
{% enddocs %}


{% docs stg_qnxt__clclaim %}
**Staged Claim Headers**

Claim header records from the QNXT `clclaim` table.

**Key Transformations:**
- Claim ID standardized
- All dates parsed from QNXT formats
- Financial amounts converted to NUMERIC(18,2)
- Status codes mapped to descriptions
- Claim types categorized (I/P/D/R)

**Grain:** One row per claim

**Incremental Strategy:** Merge on claim_id using _loaded_at
{% enddocs %}


{% docs stg_qnxt__clline %}
**Staged Claim Lines**

Claim line/service detail from the QNXT `clline` table.

**Key Transformations:**
- Surrogate key generated from claim_id + line_sequence
- Procedure and revenue codes cleaned
- Modifiers parsed (1-4)
- Place of service standardized
- Line-level amounts converted

**Grain:** One row per claim line

**Incremental Strategy:** Merge on claim_line_id using _loaded_at
{% enddocs %}


{% docs stg_qnxt__cldiag %}
**Staged Claim Diagnoses**

Claim diagnosis bridge table from the QNXT `cldiag` table.

**Key Transformations:**
- Surrogate key generated from claim_id + diagnosis_sequence
- ICD-9 vs ICD-10 code type identified
- Primary diagnosis flagged (sequence = 1)
- POA indicators standardized

**Grain:** One row per claim diagnosis

**Usage:** Join to claims for diagnosis-based analytics (risk adjustment, quality measures)
{% enddocs %}


{% docs stg_qnxt__clproc %}
**Staged Claim Procedures**

Claim procedure bridge table from the QNXT `clproc` table.

**Key Transformations:**
- Surrogate key generated from claim_id + procedure_sequence
- ICD-9-PCS vs ICD-10-PCS code type identified
- Principal procedure flagged (sequence = 1)
- Procedure dates parsed

**Grain:** One row per claim procedure

**Note:** These are ICD procedure codes (surgical), distinct from CPT/HCPCS on claim lines
{% enddocs %}


{% docs stg_qnxt__cladjust %}
**Staged Claim Adjustments**

Claim adjustment records from the QNXT `cladjust` table.

**Key Transformations:**
- Surrogate key generated from claim_id + adjustment_sequence
- Adjustment reason codes preserved
- Amounts can be positive or negative
- Adjustment dates parsed

**Grain:** One row per claim adjustment

**Usage:** Track payment corrections, voids, and financial reconciliation
{% enddocs %}


{% docs stg_qnxt__provider %}
**Staged Provider Master**

Provider master data from the QNXT `provider` table.

**Key Transformations:**
- Provider ID standardized
- NPI validated (10-digit check)
- Entity type categorized (Individual/Organization)
- Provider status mapped
- Effective/termination dates parsed

**Grain:** One row per provider

**Note:** Use provider snapshots for SCD Type 2 historical tracking
{% enddocs %}
