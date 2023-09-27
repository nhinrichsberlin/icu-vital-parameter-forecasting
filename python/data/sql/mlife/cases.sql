CREATE OR REPLACE VIEW cases AS (
WITH
patients AS (

    SELECT
      mlife_PID,
      CASE WHEN Sex = 1 THEN 1 ELSE 0 END AS sex_male,
      CASE WHEN Sex = 2 THEN 1 ELSE 0 END AS sex_female,
      GebDat_anonymized AS date_of_birth
    FROM 'data/raw/v_Patienten__anonymized/*.parquet'

),
vital_entries AS (

    SELECT
      mlife_FallNr,
      COUNT(*) AS n_obs
    FROM 'data/raw/v_Vitalwerte_unvalidiert__anonymized/*.parquet'
    GROUP BY 1

),
body_weight AS (

    SELECT
      mlife_ANR,
      Wert AS weight,
      ROW_NUMBER() OVER(PARTITION BY mlife_ANR ORDER BY Von) AS _row_number
    FROM 'data/raw/v_Verlaufsdokumentation__anonymized/*.parquet'
    WHERE TRUE
    AND StatusName = 'Gewicht / kg'
    AND Wert IS NOT NULL
    AND Wert NOT LIKE ''
    AND CAST( CASE WHEN Wert NOT LIKE '' THEN Wert ELSE NULL END AS FLOAT ) > 30 -- get rid of cases where the first entry is a typo
    AND Von <= '{datetime_end}'

),
body_height AS (

    SELECT
      mlife_ANR,
      Wert AS height,
      ROW_NUMBER() OVER(PARTITION BY mlife_ANR ORDER BY Von) AS _row_number
    FROM 'data/raw/v_Verlaufsdokumentation__anonymized/*.parquet'
    WHERE TRUE
    AND StatusName = 'Grösse in cm'
    AND Wert IS NOT NULL
    AND Wert NOT LIKE ''
    AND CAST( CASE WHEN Wert NOT LIKE '' THEN Wert ELSE NULL END AS FLOAT ) > 100 -- get rid of cases where the first entry is a typo
    AND Von <= '{datetime_end}'

),
-- table used to later filter for post-operative patients
post_operation AS (

    SELECT
      mlife_PID AS patient_id,
      Station AS op_room,
      Bis AS datetime_op_end
    FROM 'data/raw/v_Aufenthaltsort__anonymized/*.parquet'
    WHERE TRUE
      AND Station = 'OP-ANAES'
      AND Bis IS NOT NULL

),
deaths AS (

    SELECT
      mlife_FallNr,
      Von AS datetime_death
    FROM 'data/raw/v_Verlaufsdokumentation__anonymized/*.parquet'
    WHERE TRUE
    AND ModulName = 'Exitus'
    AND Von BETWEEN '{datetime_start}' AND '{datetime_end}'

)
SELECT
  admissions.mlife_ANR AS admission_id,
  admissions.mlife_PID AS patient_id,
  admissions.mlife_FallNr AS case_id,
  admissions.FB_Bez AS hospital_department,
  admissions.Station AS hospital_ward,
  pop.op_room,
  CAST( admissions.Zeitpunkt AS TIMESTAMP ) AS datetime_admission,
  CAST( pop.datetime_op_end AS TIMESTAMP ) AS datetime_op_end,
  DATE_SUB('minute', pop.datetime_op_end, admissions.Zeitpunkt) AS min_since_operation,
  CAST( deaths.datetime_death AS TIMESTAMP ) AS datetime_death,
  CAST( bw.weight AS FLOAT ) AS weight,
  CAST( bh.height AS FLOAT ) AS height,
  patients.sex_male,
  patients.sex_female,
  EXTRACT( YEARS FROM AGE( admissions.Zeitpunkt, patients.date_of_birth ) ) AS age,
  ROW_NUMBER() OVER (
    PARTITION BY admissions.mlife_PID
    ORDER BY admissions.mlife_FallNr, admissions.Zeitpunkt, pop.datetime_op_end DESC
  ) AS _row_number
FROM parquet_scan('data/raw/v_Aufnahmen__anonymized/*.parquet') admissions
-- Make sure to select the first entered body weight and height
LEFT JOIN (SELECT * FROM body_weight WHERE _row_number = 1) bw USING ( mlife_ANR )
LEFT JOIN (SELECT * FROM body_height WHERE _row_number = 1) bh USING ( mlife_ANR )
LEFT JOIN post_operation pop
  ON pop.patient_id = admissions.mlife_PID
  AND pop.datetime_op_end <= admissions.Zeitpunkt
LEFT JOIN vital_entries ve
  USING ( mlife_FallNr )
LEFT JOIN patients USING ( mlife_PID )
LEFT JOIN deaths USING ( mlife_FallNr )
-- Filters
WHERE TRUE
AND admissions.Zeitpunkt BETWEEN '{datetime_start}' AND '{datetime_end}'
-- Limit to certain Stationen
AND admissions.Station IN ('IPS I', 'OP-AWR')
AND admissions.FB_Bez IN ('HTGNP', 'C')
AND pop.datetime_op_end <= admissions.Zeitpunkt
AND DATE_SUB('minute', pop.datetime_op_end, admissions.Zeitpunkt) <= 120
AND EXTRACT( YEARS FROM AGE( admissions.Zeitpunkt, patients.date_of_birth ) ) >= 18
-- Exclude cases with insufficient number of observations
AND ve.n_obs >= {min_obs_required}
-- Exclude cases where patient_id or case_id are unknown
AND admissions.mlife_PID IS NOT NULL
AND admissions.mlife_FallNr IS NOT NULL

);

COPY( SELECT * FROM cases WHERE _row_number = 1 LIMIT {patient_limit}) TO '{cases_output_path}' (FORMAT 'parquet');
DROP VIEW cases;
