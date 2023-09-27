DROP TABLE IF EXISTS eicu_crd.forecast_cases;
CREATE TABLE eicu_crd.forecast_cases AS
WITH
vital_entries AS (

  SELECT
    patientunitstayid,
    COUNT(*) AS n_obs
  FROM eicu_crd.vitalperiodic
  GROUP BY 1

)
SELECT
  uniquepid AS patient_id,
  patientunitstayid AS case_id,
  CASE WHEN gender = 'Female' THEN 1 ELSE 0 END AS sex_female,
  CASE WHEN gender = 'Male' THEN 1 ELSE 0 END AS sex_male,
  (CASE WHEN age = '> 89' THEN '90'
   WHEN age = '' THEN NULL
   ELSE age END)::NUMERIC AS age,
  admissionheight AS height,
  admissionweight AS weight,
  -- assign patients a random date of ICU admission (Year and Time are known)
  TIMESTAMP '1970-01-01 00:00:00' +
            (hospitaldischargeyear - 1970) * INTERVAL '1 YEAR' +
            FLOOR((RANDOM() * 364 + 1)) * INTERVAL '1 DAY' +
            EXTRACT(HOUR FROM unitadmittime24::TIME) * INTERVAL '1 HOUR' +
            EXTRACT(MIN FROM unitadmittime24::TIME) * INTERVAL '1 MIN' +
            EXTRACT(SECOND FROM unitadmittime24::TIME) * INTERVAL '1 SECOND'
        AS datetime_icu_admission
FROM eicu_crd.patient
LEFT JOIN vital_entries USING( patientunitstayid )
-- filter for unit types
WHERE TRUE
   AND unittype IN ('Cardiac ICU', 'CSICU', 'CCU-CTICU', 'CTICU')
   AND age NOT IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '')
   -- filter patients with less than min_obs_required entries
   AND COALESCE( n_obs, 0 ) >= {min_obs_required}
   -- filter for post-operative patients
   AND hospitaladmitsource IN ('Operating Room', 'Recovery Room', 'PACU')
-- select only as many patients as desired (useful in early stages of research)
LIMIT {eicu_patient_limit}
;
