-- ============================================
-- Setup Script for QNXT Source Tables
-- This creates the RAW_QNXT schema and populates
-- source tables with sample data for dbt testing
-- ============================================

-- Create the source schema
CREATE SCHEMA IF NOT EXISTS PLAYGROUND.RAW_QNXT;

-- ============================================
-- MEMBER TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.MEMBER AS
SELECT
    memid,
    fname,
    mname,
    lname,
    dob,
    sex,
    ssn,
    status,
    email,
    language,
    NULL::VARCHAR AS medid,
    NULL::VARCHAR AS mcrid,
    NULL::VARCHAR AS altid,
    CURRENT_TIMESTAMP::VARCHAR AS createdate,
    CURRENT_TIMESTAMP::VARCHAR AS modifydate,
    _loaded_at
FROM PLAYGROUND.SILVER.SAMPLE_MEMBERS;

-- ============================================
-- MEMBER ELIGIBILITY TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.MBRELIG (
    memid VARCHAR,
    eligid VARCHAR,
    effdate VARCHAR,
    termdate VARCHAR,
    lobid VARCHAR,
    planid VARCHAR,
    planname VARCHAR,
    productid VARCHAR,
    groupid VARCHAR,
    subgroupid VARCHAR,
    covtype VARCHAR,
    relation VARCHAR,
    subscriberid VARCHAR,
    pcpid VARCHAR,
    pcpassigndate VARCHAR,
    eligstatus VARCHAR,
    createdate VARCHAR,
    modifydate VARCHAR,
    _loaded_at TIMESTAMP
);

-- Insert sample eligibility records for each member
INSERT INTO PLAYGROUND.RAW_QNXT.MBRELIG
SELECT
    memid,
    memid || '-ELIG001' as eligid,
    '20240101' as effdate,
    '20241231' as termdate,
    'COM' as lobid,
    'PLAN001' as planid,
    'Commercial Plan 001' as planname,
    'PROD001' as productid,
    'GRP001' as groupid,
    NULL as subgroupid,
    'MED' as covtype,
    'S' as relation,
    memid as subscriberid,
    NULL as pcpid,
    NULL as pcpassigndate,
    'A' as eligstatus,
    CURRENT_TIMESTAMP::VARCHAR as createdate,
    CURRENT_TIMESTAMP::VARCHAR as modifydate,
    CURRENT_TIMESTAMP as _loaded_at
FROM PLAYGROUND.SILVER.SAMPLE_MEMBERS;

-- ============================================
-- PROVIDER TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.PROVIDER AS
SELECT
    provid,
    npi,
    taxid,
    provname,
    provtype,
    provstatus,
    entitytype,
    NULL::VARCHAR AS license,
    NULL::VARCHAR AS dea,
    NULL::VARCHAR AS upin,
    NULL::VARCHAR AS provlname,
    NULL::VARCHAR AS provfname,
    NULL::VARCHAR AS provmname,
    NULL::VARCHAR AS suffix,
    NULL::VARCHAR AS credential,
    NULL::VARCHAR AS provtypedesc,
    NULL::VARCHAR AS taxonomy,
    NULL::VARCHAR AS effdate,
    NULL::VARCHAR AS termdate,
    CURRENT_TIMESTAMP::VARCHAR AS createdate,
    CURRENT_TIMESTAMP::VARCHAR AS modifydate,
    _loaded_at
FROM PLAYGROUND.SILVER.SAMPLE_PROVIDERS;

-- ============================================
-- CLAIM HEADER TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.CLCLAIM AS
SELECT
    claimid,
    memid,
    provid,
    svcfromdate,
    svctodate,
    recvddate,
    paiddate,
    claimstatus,
    billedamt,
    allowedamt,
    paidamt,
    claimtype,
    NULL::VARCHAR AS servprovid,
    NULL::VARCHAR AS refprovid,
    NULL::VARCHAR AS facilityid,
    NULL::VARCHAR AS formtype,
    NULL::VARCHAR AS billtype,
    NULL::VARCHAR AS admitdate,
    NULL::VARCHAR AS dischargedate,
    NULL::VARCHAR AS processdate,
    NULL::VARCHAR AS adjuddate,
    NULL::NUMBER(18,2) AS copayamt,
    NULL::NUMBER(18,2) AS coinsamt,
    NULL::NUMBER(18,2) AS deductamt,
    NULL::NUMBER(18,2) AS cobamt,
    NULL::NUMBER(18,2) AS withholdamt,
    NULL::VARCHAR AS drg,
    NULL::VARCHAR AS admittype,
    NULL::VARCHAR AS admitsource,
    NULL::VARCHAR AS dischargestatus,
    NULL::INTEGER AS los,
    NULL::VARCHAR AS authid,
    NULL::VARCHAR AS refnum,
    NULL::VARCHAR AS icn,
    NULL::VARCHAR AS dcn,
    CURRENT_TIMESTAMP::VARCHAR AS createdate,
    CURRENT_TIMESTAMP::VARCHAR AS modifydate,
    _loaded_at
FROM PLAYGROUND.SILVER.SAMPLE_CLAIMS;

-- ============================================
-- CLAIM LINE TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.CLLINE AS
SELECT
    claimid,
    lineseq,
    proccode,
    revcode,
    svcunits,
    billedamt,
    allowedamt,
    paidamt,
    pos,
    linestatus,
    NULL::VARCHAR AS modifier1,
    NULL::VARCHAR AS modifier2,
    NULL::VARCHAR AS modifier3,
    NULL::VARCHAR AS modifier4,
    NULL::VARCHAR AS ndc,
    NULL::VARCHAR AS svcfromdate,
    NULL::VARCHAR AS svctodate,
    NULL::NUMBER(18,2) AS copayamt,
    NULL::NUMBER(18,2) AS coinsamt,
    NULL::NUMBER(18,2) AS deductamt,
    NULL::NUMBER(18,2) AS cobamt,
    NULL::NUMBER(18,2) AS withholdamt,
    NULL::VARCHAR AS diagptr1,
    NULL::VARCHAR AS diagptr2,
    NULL::VARCHAR AS diagptr3,
    NULL::VARCHAR AS diagptr4,
    NULL::VARCHAR AS servprovid,
    CURRENT_TIMESTAMP::VARCHAR AS createdate,
    CURRENT_TIMESTAMP::VARCHAR AS modifydate,
    _loaded_at
FROM PLAYGROUND.SILVER.SAMPLE_CLAIM_LINES;

-- ============================================
-- DIAGNOSIS CODE REFERENCE TABLE
-- ============================================
CREATE OR REPLACE TABLE PLAYGROUND.RAW_QNXT.DIAGCODE (
    diagcode VARCHAR,
    diagdesc VARCHAR,
    diagshortdesc VARCHAR,
    diagtype VARCHAR,
    category VARCHAR,
    categorydesc VARCHAR,
    chapter VARCHAR,
    chapterdesc VARCHAR,
    effdate VARCHAR,
    termdate VARCHAR,
    billable VARCHAR,
    header VARCHAR,
    createdate VARCHAR,
    modifydate VARCHAR,
    _loaded_at TIMESTAMP
);

-- Insert sample diagnosis codes
INSERT INTO PLAYGROUND.RAW_QNXT.DIAGCODE VALUES
('E11.9', 'Type 2 diabetes mellitus without complications', 'Type 2 DM w/o comp', 'ICD10', 'E11', 'Type 2 diabetes mellitus', '04', 'Endocrine, nutritional and metabolic diseases', '20151001', NULL, 'Y', 'N', CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP),
('I10', 'Essential (primary) hypertension', 'Essential hypertension', 'ICD10', 'I10', 'Essential (primary) hypertension', '09', 'Diseases of the circulatory system', '20151001', NULL, 'Y', 'N', CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP),
('J06.9', 'Acute upper respiratory infection, unspecified', 'Acute URI', 'ICD10', 'J06', 'Acute upper respiratory infections of multiple and unspecified sites', '10', 'Diseases of the respiratory system', '20151001', NULL, 'Y', 'N', CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP),
('M79.3', 'Panniculitis, unspecified', 'Panniculitis', 'ICD10', 'M79', 'Other and unspecified soft tissue disorders', '13', 'Diseases of the musculoskeletal system and connective tissue', '20151001', NULL, 'Y', 'N', CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP),
('Z00.00', 'Encounter for general adult medical examination without abnormal findings', 'General adult exam', 'ICD10', 'Z00', 'Encounter for general examination without complaint', '21', 'Factors influencing health status and contact with health services', '20151001', NULL, 'Y', 'N', CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP::VARCHAR, CURRENT_TIMESTAMP);

-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Uncomment these to verify the tables were created successfully

-- SELECT 'MEMBER' as table_name, COUNT(*) as row_count FROM PLAYGROUND.RAW_QNXT.MEMBER
-- UNION ALL
-- SELECT 'MBRELIG', COUNT(*) FROM PLAYGROUND.RAW_QNXT.MBRELIG
-- UNION ALL
-- SELECT 'PROVIDER', COUNT(*) FROM PLAYGROUND.RAW_QNXT.PROVIDER
-- UNION ALL
-- SELECT 'CLCLAIM', COUNT(*) FROM PLAYGROUND.RAW_QNXT.CLCLAIM
-- UNION ALL
-- SELECT 'CLLINE', COUNT(*) FROM PLAYGROUND.RAW_QNXT.CLLINE
-- UNION ALL
-- SELECT 'DIAGCODE', COUNT(*) FROM PLAYGROUND.RAW_QNXT.DIAGCODE;
