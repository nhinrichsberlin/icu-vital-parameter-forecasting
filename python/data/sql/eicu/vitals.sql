WITH
vitals_with_duplicates AS (

    SELECT
      -- identifiers
      patient_id,
      case_id,
      -- patient characteristics
      age,
      height,
      weight,
      sex_male,
      sex_female,
      -- timestamps
      datetime_icu_admission + vp.observationoffset * INTERVAL '1min' AS datetime,
      datetime_icu_admission + va.observationoffset * INTERVAL '1min' AS datetime_aperiodic,
      -- vital signs
      sao2 AS oxygen_saturation,
      heartrate AS heart_rate,
      cvp AS central_venous_pressure,
      COALESCE( systemicsystolic, noninvasivesystolic ) AS blood_pressure_systolic,
      COALESCE( systemicdiastolic, noninvasivediastolic ) AS blood_pressure_diastolic,
      COALESCE( systemicmean, noninvasivemean ) AS blood_pressure_mean,
      ROW_NUMBER() OVER( PARTITION BY case_id, vp.observationoffset ORDER BY va.observationoffset DESC) AS _row_number
    FROM eicu_crd.forecast_cases c
    LEFT JOIN eicu_crd.vitalperiodic vp
     ON c.case_id = vp.patientunitstayid
    LEFT JOIN eicu_crd.vitalaperiodic va
     ON c.case_id = va.patientunitstayid
     -- use noninvasive measurement if it is younger than 4 minutes
     -- otherwise it might override a previous, newer invasive measurement
     AND vp.observationoffset - va.observationoffset BETWEEN 0 AND 4
    WHERE TRUE
     AND vp.observationoffset BETWEEN 0 AND 60 * {max_hours}

)
SELECT
  patient_id,
  case_id,
  age,
  height,
  weight,
  sex_male,
  sex_female,
  datetime,
  oxygen_saturation,
  heart_rate,
  central_venous_pressure,
  blood_pressure_diastolic,
  blood_pressure_mean,
  blood_pressure_systolic
FROM vitals_with_duplicates
WHERE _row_number = 1
;
