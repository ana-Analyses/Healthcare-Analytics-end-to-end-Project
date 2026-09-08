--1.Beds occupied total
 
CREATE VIEW dbo.vw_bed_occupancy AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN 1 END) * 1.0 
    / COUNT(*) * 100 AS bed_occupancy_percent
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p 
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;


--2.Total bed turnover
CREATE VIEW vw_bed_turnover_rate AS
SELECT 
    p.gender,
    COUNT(DISTINCT f.fact_id) * 1.0 / COUNT(DISTINCT f.bed_id) AS bed_turnover_rate
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;

--3.Total patients
CREATE VIEW vw_patient_demographics AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS total_patients
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;

--4.Avg treatment duration
CREATE VIEW vw_avg_treatment_duration AS
SELECT 
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;

-- select * from  vw_avg_treatment_duration

--Chart's
--1.Total patients count over time
CREATE VIEW vw_patient_volume_trend AS
SELECT 
    f.admission_date,
    p.gender,
    COUNT(DISTINCT f.fact_id) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY f.admission_date, p.gender;

--2. Total patients over department
CREATE VIEW vw_department_inflow AS
SELECT 
    d.department,
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;

--3.Total overstay patients count
CREATE VIEW vw_overstay_patients AS
SELECT 
    d.department,
    p.gender,
    COUNT(f.fact_id) AS overstay_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
WHERE f.length_of_stay_hours > 50
GROUP BY d.department, p.gende
