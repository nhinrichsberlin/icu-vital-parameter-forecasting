import pandas as pd
from joblib import Parallel, delayed

from python.utils import custom_logger

from python.data.generate_mlife import (
    check_data,
    remove_extreme_values,
    truncate_ts,
    report_nas,
    one_case_per_patient,
    sufficient_obs
)
from python.utils import (
    read_format_query,
    postgres_execute_query,
    postgres_query_to_pandas_df,
    pickle_to_object,
    to_parquet_per_case
)
from config import (
    MIN_OBS_REQUIRED,
    MAX_HOURS,
    EICU_PATIENT_LIMIT,
    TARGETS,
)
from python.data.train_val_test_split import impute_columns

LOGGER = custom_logger('data/generate_eicu.py')


def query_eicu_db(
) -> pd.DataFrame:

    LOGGER.info(f'Querying {EICU_PATIENT_LIMIT} cases from the EICU DB')

    # query the relevant cases
    query = read_format_query(
        path_to_query='python/data/sql/eicu/cases.sql',
        query_vars={'min_obs_required': MIN_OBS_REQUIRED,
                    'eicu_patient_limit': EICU_PATIENT_LIMIT}
    )
    postgres_execute_query(query)
    LOGGER.info('\tCases OK')

    # query the vital parameters
    query = read_format_query(
        path_to_query='python/data/sql/eicu/vitals.sql',
        query_vars={'max_hours': MAX_HOURS}
    )
    df = postgres_query_to_pandas_df(query)
    LOGGER.info('Data loaded to pandas.')

    return df


def ffill_data(
        df: pd.DataFrame
) -> pd.DataFrame:

    def _ffill_case_data(df_case: pd.DataFrame) -> pd.DataFrame:
        # compute what the index should be
        index_without_gaps = pd.date_range(df_case.datetime.min(), df_case.datetime.max(), freq='5min')
        # re-index, if needed
        if len(df_case.index) < len(index_without_gaps):
            # re-index to fill the gaps and ffill values
            df_case = \
                df_case\
                .set_index('datetime')\
                .reindex(index_without_gaps)\
                .reset_index()\
                .rename({'index': 'datetime'}, axis=1)
        for target in TARGETS:
            df_case[f'{target}__NA'] = df_case[target].isna().astype(int)

        # forward fill non-targets without limit (NAs appear due to re-indexing)
        non_targets = set(df_case.columns) - set(TARGETS)
        for nt in non_targets:
            df_case[nt] = df_case[nt].ffill()

        # forward fill targets with a limit of 3
        return df_case.ffill(limit=3)

    LOGGER.info('Forward filling missing values.')
    df_ffilled = pd.concat(
        Parallel(n_jobs=-3)(delayed(_ffill_case_data)(df[df.case_id == case]) for case in df.case_id.unique())
    )
    LOGGER.info('Forward filling missing values.')

    return df_ffilled


def impute_data(
        df: pd.DataFrame
) -> pd.DataFrame:

    # load fitted imputer
    median_imputer = pickle_to_object('data/processed/iterative_imputer.pkl')

    # impute targets
    df = impute_columns(
        df=df,
        imputer=median_imputer,
        cols_to_impute=TARGETS
    )

    return df


def main():

    # load the data to pandas
    df = query_eicu_db()

    # sub-set to the first case per patient
    df = one_case_per_patient(df)

    # drop cases with too few observations
    df = sufficient_obs(df, min_obs_required=MIN_OBS_REQUIRED / 5)

    # check the data
    check_data(
        df=df,
        resampled=True,
        check_start_end=False
    )

    # remove extreme values
    df = remove_extreme_values(df)

    # truncate timeseries: drop observations to start the TS if no target is not-null
    df = truncate_ts(df, min_obs_required=MIN_OBS_REQUIRED / 5)

    # some reporting on NAs
    report_nas(df, 'data/processed/na_per_target_eicu.csv')

    # make sure there are no gaps in the time index and ffill
    df = ffill_data(df)

    # check the data
    check_data(
        df=df,
        na_allowed=True,
        resampled=True,
        check_start_end=False,
        evenly_spaced_times=True
    )

    # write un-imputed data to parquet files
    to_parquet_per_case(df, 'data/processed/vitals_eicu_unimputed')

    # impute data using the stored imputers trained with MLife training data
    df = impute_data(df)

    # check the data
    check_data(
        df=df,
        na_allowed=False,
        resampled=True,
        check_start_end=False,
        evenly_spaced_times=True
    )

    # add column indicating the elapsed time
    df['minutes_elapsed'] = 5 * df.groupby(['patient_id', 'case_id']).cumcount()

    # write results to parquet files
    to_parquet_per_case(df, 'data/processed/vitals_eicu')


if __name__ == '__main__':
    main()
