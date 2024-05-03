from python.utils import yaml_to_dict

CONFIG = yaml_to_dict('config/config.yaml')

MIN_OBS_REQUIRED = CONFIG['min_obs_required']
MAX_HOURS = CONFIG['max_hours']

RESAMPLING_AGG_FUNC = CONFIG['resampling_agg_func']
RESAMPLING_MINUTES = CONFIG['resampling_minutes']

DATE_START = CONFIG['date_start']
DATE_END = CONFIG['date_end']

TARGETS = CONFIG['targets']
TARGET_LABELS = CONFIG['var_labels']
EXTREME_VALUE_LIMITS = CONFIG['extreme_value_limits']

EICU_PATIENT_LIMIT = CONFIG.get('eicu_patient_limit', 10 ** 10)
MLIFE_PATIENT_LIMIT = CONFIG.get('mlife_patient_limit', 10 ** 10)

FORECAST_HORIZON = CONFIG['forecast_horizon']

NEEDED_OUTPUT_COLUMNS = CONFIG['needed_output_columns']
TS_MODELS = CONFIG['ts_models']
