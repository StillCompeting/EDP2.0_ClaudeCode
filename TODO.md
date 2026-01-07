# QNXT EDP 2.0 - Project Tasks

## Overview
This document tracks the remaining tasks to complete the QNXT Enterprise Data Platform 2.0 dbt project.

**Current Status:** ~75% Complete

---

## High Priority

### 1. Validate dbt project compiles and runs
Execute the dbt build process to validate all models compile and run correctly.

- [ ] Run `dbt deps` to install package dependencies
- [ ] Run `dbt seed` to load sample data
- [ ] Run `dbt run` to build all models
- [ ] Verify models create successfully in Snowflake

---

### 2. Run dbt tests to verify test definitions
Execute dbt tests to validate that all test definitions work correctly.

- [ ] Run `dbt test` to execute all configured tests
- [ ] Review test results and fix any failures
- [ ] Validate dbt_expectations tests are working
- [ ] Ensure source freshness tests are configured properly

**Depends on:** Task 1

---

## Medium Priority

### 3. Create README.md with setup instructions
Create a comprehensive README.md file with project documentation and setup instructions.

- [ ] Document project overview and purpose
- [ ] Add architecture diagram (medallion layers)
- [ ] Include prerequisites (dbt, Snowflake access, Python)
- [ ] Add installation/setup instructions
- [ ] Document how to configure profiles.yml
- [ ] Add usage examples (dbt run, dbt test, dbt docs)
- [ ] Include contribution guidelines

---

### 4. Add custom singular tests for business logic validation
Create custom dbt tests in the `/tests` directory to validate business logic.

- [ ] Create `/tests` directory
- [ ] Add test: service_from_date <= service_to_date on claims
- [ ] Add test: claim amounts are non-negative
- [ ] Add test: PMPM calculations are accurate
- [ ] Add test: member_months aggregations are correct
- [ ] Add test: foreign key relationships between facts and dimensions
- [ ] Add test: no orphaned claim lines

---

### 5. Set up CI/CD pipeline for automated testing
Configure CI/CD pipeline for automated dbt testing on pull requests.

- [ ] Choose CI/CD platform (GitHub Actions, dbt Cloud, etc.)
- [ ] Configure workflow to run on PR
- [ ] Add dbt deps step
- [ ] Add dbt compile step (syntax validation)
- [ ] Add dbt test step (with slim CI if possible)
- [ ] Configure notifications for failures
- [ ] Add branch protection rules

---

### 6. Configure source freshness monitoring
Source freshness is defined in _sources.yml but not actively monitored.

- [ ] Verify freshness thresholds are appropriate (currently 48-hour error)
- [ ] Run `dbt source freshness` to test configuration
- [ ] Set up scheduled freshness checks
- [ ] Configure alerting for stale sources
- [ ] Document expected data refresh schedules

---

## Low Priority

### 7. Generate and host dbt documentation site
Generate dbt docs and set up hosting for the documentation site.

- [ ] Run `dbt docs generate` to create documentation
- [ ] Review generated lineage graph
- [ ] Set up hosting (GitHub Pages, S3, or dbt Cloud)
- [ ] Configure auto-regeneration on main branch updates
- [ ] Add link to docs in README

---

### 8. Expand seed data for better test coverage
The current seed files have minimal data (4-9 rows each). Expand for better coverage.

- [ ] Add more diverse member demographics (age ranges, genders, states)
- [ ] Add edge case claims (zero amounts, adjustments, denials)
- [ ] Add claims spanning date boundaries (month/year end)
- [ ] Add provider data with various specialties and NPIs
- [ ] Add multi-line claims with multiple diagnoses
- [ ] Document seed data scenarios

---

### 9. Add dbt exposures for BI dashboard integration
Define dbt exposures to document downstream BI dashboards and reports.

- [ ] Identify downstream consumers (Tableau, Power BI, Looker, etc.)
- [ ] Create exposures.yml file
- [ ] Define exposure for claims analytics dashboard
- [ ] Define exposure for membership reporting
- [ ] Define exposure for financial/PMPM reports
- [ ] Add owner information and maturity levels

---

### 10. Create analyses directory with exploratory queries
Add an `/analyses` directory with useful exploratory SQL queries.

- [ ] Create `/analyses` directory
- [ ] Add data profiling queries
- [ ] Add data quality check queries
- [ ] Add sample analytics queries (top claims, member tenure, etc.)
- [ ] Add troubleshooting queries for common issues

---

## Completed Tasks

- [x] Initialize dbt project structure
- [x] Create Bronze layer (8 staging models)
- [x] Create Silver layer (5 core dimension/fact models)
- [x] Create Gold layer (3 analytics marts)
- [x] Build macro library (21 macros)
- [x] Configure SCD Type 2 snapshots (3 snapshots)
- [x] Add sample seed data (8 CSV files)
- [x] Write comprehensive documentation (docs.md)
- [x] Configure dbt packages (dbt_utils, dbt_expectations, dbt_date)
- [x] Set up multi-environment profiles template

---

## Known Issues / Technical Debt

1. **Minimal seed data** - Only 4-9 rows per file limits test coverage
2. **Missing date validation** - No tests for service_from_date <= service_to_date
3. **No FK relationship tests** - Facts not validated against dimensions
4. **Source freshness untested** - 48-hour error threshold defined but not monitored
5. **No error handling in macros** - `try_to_date()` used without fallback logic
