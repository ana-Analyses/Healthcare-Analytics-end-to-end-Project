IF OBJECT_ID('dbo.dim_patient', 'U') IS NOT NULL
DROP TABLE dbo.dim_patient;

CREATE TABLE dbo.dim_patient AS
SELECT 
    patient_id,
    gender,
    age,
    effective_from,
    surrogate_key,
    effective_to,
    is_current
FROM LH_gold.gold_layer.dim_patient;

-----------------------------------------------------
IF OBJECT_ID('dbo.dim_department', 'U') IS NOT NULL
DROP TABLE dbo.dim_department;

CREATE TABLE dbo.dim_department AS
SELECT 
    surrogate_key,
    department,
    hospital_id
FROM LH_gold.gold_layer.dim_department;

------------------------------------------------------
IF OBJECT_ID('dbo.fact_patient_flow', 'U') IS NOT NULL
DROP TABLE dbo.fact_patient_flow;

CREATE TABLE dbo.fact_patient_flow AS
SELECT 
    fact_id,
    patient_sk,
    department_sk,
    admission_time,
    discharge_time,
    admission_date,
    length_of_stay_hours,
    is_currently_admitted,
    bed_id,
    event_time AS event_ingestion_time
FROM LH_gold.gold_layer.fact_patient;

SELECT TOP 10 * FROM dbo.fact_patient_flow;
