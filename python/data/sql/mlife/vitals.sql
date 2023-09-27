CREATE OR REPLACE VIEW vital_parameters AS (
    WITH
    vitals AS (

        SELECT
          mlife_FallNr AS case_id,
          CAST ( Zeitpunkt AS TIMESTAMP ) AS datetime,
          -- drop rare cases of duplicate rows
          AVG( COALESCE(BPsys, sNiB) )  AS blood_pressure_systolic,
          AVG( COALESCE(BPdias, dNiB) ) AS blood_pressure_diastolic,
          AVG( COALESCE(BPM, mNiB) ) AS blood_pressure_mean,
          AVG( ZVD ) AS central_venous_pressure,
          AVG( Herzfrequenz ) AS heart_rate,
          AVG( SPO2 ) AS oxygen_saturation
        FROM 'data/raw/v_Vitalwerte_unvalidiert__anonymized/*.parquet'
        WHERE TRUE
          AND mlife_FallNr = '{case_id}'
        GROUP BY 1, 2

    )
    SELECT
      c.case_id,
      c.admission_id,
      c.patient_id,
      c.hospital_department,
      c.hospital_ward,
      c.datetime_admission,
      c.min_since_operation,
      c.age,
      c.weight,
      c.height,
      c.sex_male,
      c.sex_female,
      v.datetime,
      -- targets
      v.blood_pressure_systolic,
      v.blood_pressure_diastolic,
      v.blood_pressure_mean,
      v.central_venous_pressure,
      v.heart_rate,
      v.oxygen_saturation
    FROM '{cases_output_path}' c
    LEFT JOIN vitals v
      ON c.case_id = v.case_id
    WHERE TRUE
      AND v.datetime BETWEEN c.datetime_admission AND c.datetime_admission + INTERVAL {max_hours} HOUR

);


COPY( SELECT * FROM vital_parameters ) TO '{output_path}' (FORMAT 'parquet');
DROP VIEW vital_parameters;
