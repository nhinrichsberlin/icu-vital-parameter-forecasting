import duckdb
import os

import pandas as pd
import numpy as np
import datetime
from joblib import Parallel, delayed

from python.utils import (
    custom_logger,
    read_format_query,
    to_parquet_per_case
)

from config import (
    TARGETS,
    EXTREME_VALUE_LIMITS,
    MIN_OBS_REQUIRED,
    MAX_HOURS,
    DATE_START,
    DATE_END,
    RESAMPLING_MINUTES,
    RESAMPLING_AGG_FUNC,
    MLIFE_PATIENT_LIMIT
)

LOGGER = custom_logger('data/generate_mlife.py')

QUERY_OUTPUT_FOLDER = 'data/processed/vitals_queried'
PROCESSED_DATA_PATH = 'data/processed/vitals_processed'

N_JOBS = -5


def _check_cases(
        cases: pd.DataFrame
) -> None:

    # check that all cases have an op end time
    assert not cases.datetime_op_end.isna().any(), 'NAs found in operation end time'
    # check that the operating room is correct
    assert set(cases.op_room.unique()) == {'OP-ANAES'}, 'Wrong operating room found.'
    # check that we always know the time since the operation
    assert not cases.min_since_operation.isna().any(), 'NAs found in time since op end.'
    # check that at most 2h have passed since the operation
    assert cases.min_since_operation.max() <= 120, 'Patients too long removed from operating room!'


def _query_case(
        case: int
) -> None:

    start = datetime.datetime.now()

    if N_JOBS == 1:
        LOGGER.info(f'Querying data for case {case}')

    connection = duckdb.connect(':memory:', read_only=False)

    # create a table containing the current case's vitals, lab and medication data
    output_path = f'data/processed/vitals_queried/case_{case}.parquet'
    query_vars = {
        'max_hours': MAX_HOURS,
        'cases_output_path': 'data/processed/vitals_queried/cases.parquet',
        'output_path': output_path,
        'case_id': case
    }
    query = read_format_query('python/data/sql/mlife/vitals.sql', query_vars)
    connection.execute(query)

    connection.close()

    if N_JOBS == 1:
        LOGGER.info(f'\tCase {case} done in {datetime.datetime.now() - start}')


def query_mlife(
) -> None:

    LOGGER.info(f'Querying the MLIFE DB from {DATE_START} to {DATE_END}')
    start_time = datetime.datetime.now()

    # make sure the output folder is empty
    if any(file.endswith('.parquet') for file in os.listdir(QUERY_OUTPUT_FOLDER)):
        [
            os.remove(os.path.join(QUERY_OUTPUT_FOLDER, f))
            for f in os.listdir(QUERY_OUTPUT_FOLDER)
            if f.endswith('.parquet')
        ]
        assert not any(file.endswith('.parquet') for file in os.listdir(QUERY_OUTPUT_FOLDER))

    # create a duckdb connection
    con = duckdb.connect(':memory:', read_only=False)

    # create a table of relevant cases
    output_path_cases = 'data/processed/vitals_queried/cases.parquet'
    start_cases = datetime.datetime.now()
    query_vars = {
        'datetime_start': DATE_START,
        'datetime_end': DATE_END,
        'patient_limit': MLIFE_PATIENT_LIMIT,
        'min_obs_required': MIN_OBS_REQUIRED,
        'cases_output_path': 'data/processed/vitals_queried/cases.parquet'
    }
    query = read_format_query('python/data/sql/mlife/cases.sql', query_vars)
    con = con.execute(query)
    LOGGER.info(f'\tCases OK ({int((datetime.datetime.now() - start_cases).total_seconds())} seconds)')
    con.close()

    # load the cases data
    df_cases = pd.read_parquet(output_path_cases)
    _check_cases(df_cases)

    cases = df_cases.case_id.unique()

    LOGGER.info(f'Looping over {len(cases)} cases')

    Parallel(n_jobs=N_JOBS)(delayed(_query_case)(case) for case in cases)
    LOGGER.info(f'\tDone after {datetime.datetime.now() - start_time}')


def check_data(
        df: pd.DataFrame,
        resampled: bool,
        na_allowed: bool = True,
        check_start_end: bool = True,
        evenly_spaced_times: bool = False
) -> None:

    LOGGER.info('Checking the data:')

    def _get_start_end_dates():
        # get start and end date, defined as 1 minute before/after last valid observation
        start = datetime.datetime.combine(DATE_START, datetime.time(0, 0)) - datetime.timedelta(minutes=1)
        end = datetime.datetime.combine(DATE_END, datetime.time(0, 0)) + datetime.timedelta(hours=24, minutes=1)
        # the re-sampling can lead to the exceeding of date_end by a couple of minutes
        if resampled:
            end += datetime.timedelta(minutes=4)
        return start, end

    max_time = (df.groupby('case_id').datetime.max() - df.groupby('case_id').datetime.min()).max()
    assert max_time.total_seconds() <= MAX_HOURS * 60 * 60, 'Max. observed time-frame is too long!'
    LOGGER.info('\t Max observed time frame: OK')

    if check_start_end:
        # check start and end are correct
        date_start, date_end = _get_start_end_dates()
        assert df.datetime.min() >= date_start
        assert df.datetime.max() <= date_end
        LOGGER.info('\t Start and end time: OK')

    # check that the combination of patient_id, case_id and datetime is unique
    assert df.drop_duplicates(['patient_id', 'case_id', 'datetime']).shape[0] == df.shape[0]
    LOGGER.info('\t Unique combinations of patient_id, case_id, datetime: OK')

    # check that sex remains constant
    # just a precaution, because at one point there was an issue with this
    sex_features = ['sex_male', 'sex_female']
    for s in sex_features:
        assert (df.groupby('patient_id')[s].min() == df.groupby('patient_id')[s].max()).all()
        LOGGER.info('\t Sex remains constant: OK')

    # check that age does not change more than one year
    # just a precaution, because at one point there was an issue with this
    assert (df.groupby('patient_id').age.max() - df.groupby('patient_id').age.min()).max() <= 1
    LOGGER.info('\t Age remains constant: OK')

    # check we have the required number of observations per combination of patient_id, case_id
    min_obs_required = MIN_OBS_REQUIRED if not resampled else MIN_OBS_REQUIRED / RESAMPLING_MINUTES
    assert (df.groupby(['patient_id', 'case_id']).datetime.count() >= min_obs_required).all()
    LOGGER.info('\t Required number of obs per case: OK')

    # certain columns shall not contain NAs
    no_nas = ['age', 'sex_male', 'sex_female', 'datetime',
              'hospital_department', 'hospital_ward', 'case_id', 'patient_id']
    for col in no_nas:
        if col in df.columns:
            assert not df[col].isna().any(), f'NAs found in column {col}'
    LOGGER.info('\t No NAs in relevant columns: OK')

    # check that all patients are adults
    assert df.age.min() >= 18, 'Dataset contains children!'
    LOGGER.info('\t Age of patients: OK')

    # if needed, check for NAs
    if not na_allowed:
        cols_no_na = list(set(no_nas + TARGETS))
        for col in cols_no_na:
            if col in df.columns:
                assert not df[col].isna().any(), f'NA found in column {col}'
    LOGGER.info('\t No NAs in relevant columns: OK')

    if evenly_spaced_times:
        # check that datetime is evenly and correctly spaced
        # go case by case, because pandas gets stuck otherwise

        seconds_factor = RESAMPLING_MINUTES if resampled else 1

        def _correctly_spaced(df_case: pd.DataFrame):
            case = df_case.case_id.min()
            diff = df_case.datetime.diff()
            min_diff = diff.min()
            max_diff = diff.max()
            assert min_diff == max_diff, f'Datetime index is not evenly spaced for case {case}'
            assert min_diff.seconds == seconds_factor * 60, f'Datetime index spacing is wrong for case {case}!'

        Parallel(n_jobs=-3)(delayed(_correctly_spaced)(df[df.case_id == case]) for case in df.case_id.unique())
        LOGGER.info('\t Time index spacing: OK')


def truncate_ts(
        df: pd.DataFrame,
        min_obs_required=MIN_OBS_REQUIRED
) -> pd.DataFrame:

    LOGGER.info('Truncating the Time-Series.')

    n_cases_before = len(df.case_id.unique())
    n_obs = df.shape[0]

    # store original columns, because we're going to add some that we don't want to return
    columns = df.columns

    # NAs at the start of the TS
    LOGGER.info('\tDropping cases where the TS starts with only NA targets.')

    # indicate whether any target is not na
    df['any_target_notna'] = df[TARGETS].notna().any(axis=1)
    df['any_target_notna_cumsum'] = df.groupby('case_id').any_target_notna.cumsum()

    # drop cases where timeseries starts with targets all being null
    df = df.loc[df.any_target_notna_cumsum > 0, columns]

    LOGGER.info(f'\tReduced obs. by {n_obs - df.shape[0]}')
    n_obs = df.shape[0]

    # NAs to end the TS
    LOGGER.info('\tDropping cases where the TS ends with only NA targets.')

    # Same procedure as above, but with datetime sorted the other way round
    # to find cases that end with all targets being NA
    df = df.sort_values(['patient_id', 'case_id', 'datetime'], ascending=[True, True, False])
    df['any_target_notna'] = df[TARGETS].notna().any(axis=1)
    df['any_target_notna_cumsum'] = df.groupby('case_id').any_target_notna.cumsum()

    # drop cases where timeseries ends with targets all being null
    df = df.loc[df.any_target_notna_cumsum > 0, columns]

    LOGGER.info(f'\tReduced obs. by {n_obs - df.shape[0]}')

    # subset to cases that have sufficient observations remaining
    obs_per_case = df.groupby('case_id').datetime.count()
    df = df[df.case_id.isin(obs_per_case.index[obs_per_case >= min_obs_required])]

    n_cases_after = len(df.case_id.unique())
    LOGGER.info(f'Dropped {n_cases_before - n_cases_after} cases because they contained <= {min_obs_required} obs.')

    return df.sort_values(['patient_id', 'case_id', 'datetime'])


def ffill_resample_ts(
        df: pd.DataFrame,
        minutes: int
) -> pd.DataFrame:

    def _fill_gaps(df_case: pd.DataFrame) -> pd.DataFrame:
        # compute what the index should be
        index_without_gaps = pd.date_range(df_case.index.min(), df_case.index.max(), freq='min')
        # re-index, if needed
        if len(df_case.index) < len(index_without_gaps):
            # re-index to fill the gaps and ffill values
            df_case = df_case.reindex(index_without_gaps)

        # make sure there are no gaps that cannot be re-sampled in the non-target columns
        non_target_cols = list(set(df_case.columns) - set(TARGETS))
        df_case[non_target_cols] = df_case[non_target_cols].ffill()

        return df_case

    def _resample(df_case: pd.DataFrame) -> pd.DataFrame:
        # set the time index
        df_case = df_case.set_index('datetime')
        # fill gaps in the index
        df_case = _fill_gaps(df_case)
        # find the right offset
        offset = f'{df_case.index.min().minute}min'
        first_obs = df_case.index.min()
        # define how to aggregate each column
        df_case = df_case\
            .resample(f'{minutes}min', offset=offset, closed='right', label='right')\
            .agg(agg)\
            .reset_index()\
            .rename({'index': 'datetime'}, axis=1)
        # make sure the time series still starts at the same time
        assert df_case.datetime.min() == first_obs, 'Index datetime changed!'
        # store info on missing values
        for target in TARGETS:
            df_case[f'{target}__NA'] = df_case[target].isna().astype(int)
        # forward fill missing values for up to 15 minutes
        return df_case.ffill(limit=3)

    LOGGER.info(f'Forward-fill missing values and resample to {RESAMPLING_MINUTES} minutes'
                f' using the {RESAMPLING_AGG_FUNC}.')

    # define how to aggregate the columns in the dataframe
    agg_features_and_targets = {col: RESAMPLING_AGG_FUNC for col in TARGETS}
    agg_others = {col: 'min' for col in df.columns if col not in (TARGETS + ['datetime'])}
    agg = {**agg_features_and_targets, **agg_others}
    # run in parallel for each case_id at a time
    df = pd.concat(
        Parallel(n_jobs=-3)(
            delayed(_resample)(df[df.case_id == case]) for case in df.case_id.unique()
        )
    )

    # add column indicating elapsed time since first observation
    df['minutes_elapsed'] = minutes * df.groupby(['patient_id', 'case_id']).cumcount()

    LOGGER.info(f'New shape of the data: {df.shape}')

    # make sure everything's sorted properly
    return df.sort_values(['patient_id', 'case_id', 'datetime'])


def remove_extreme_values(
        df: pd.DataFrame,
        verbose: bool = True
) -> pd.DataFrame:

    LOGGER.info('Getting rid of values exceeding plausible limits.')

    for col, cutoffs in EXTREME_VALUE_LIMITS.items():
        if col in df.columns:
            if verbose:
                LOGGER.info(f'\t{col} -- min: {cutoffs["min"]} -- max: {cutoffs["max"]} ')
            nas_before = df[col].isna().sum()

            df_col = df[col]
            too_high = df_col > cutoffs['max']
            too_low = df_col < cutoffs['min']

            df[col] = df[col].mask(too_high | too_low, np.nan)

            nas_after = df[col].isna().sum()
            if verbose:
                LOGGER.info(f'\t{col}: NAs increased from {nas_before} to {nas_after} .')

    no_extremes = [v for v in TARGETS if v not in EXTREME_VALUE_LIMITS.keys()]
    LOGGER.info(f'No extreme value limits supplied for:')
    for n in no_extremes:
        LOGGER.info(f'\t{n}')

    return df.sort_values(['patient_id', 'case_id', 'datetime'])


def report_nas(
        df: pd.DataFrame,
        output_path: str = None
) -> None:
    # compute percentage of missing values per column
    nas = df[TARGETS].isna().mean().sort_values(ascending=False)

    if output_path is not None:
        nas.reset_index().to_csv(output_path, sep=';', index=False)

    LOGGER.info('Percentage of missing values: ')
    for c in nas[nas > 0].index:
        LOGGER.info(f'\t{c}: {np.round(nas[c], 3)}')


def one_case_per_patient(
        df: pd.DataFrame
) -> pd.DataFrame:

    n_cases_before = len(df.case_id.unique())
    LOGGER.info('Sub-setting to the first case per patient')

    start_times = \
        df \
        .groupby(['patient_id', 'case_id'])['datetime']\
        .min()\
        .reset_index()\
        .sort_values(['patient_id', 'case_id', 'datetime'])

    start_times['case_number'] = start_times.groupby('patient_id').cumcount()
    relevant_cases = start_times.loc[start_times.case_number == 0, 'case_id']

    df = df[df.case_id.isin(relevant_cases)].copy()
    n_cases_after = len(df.case_id.unique())
    LOGGER.info(f'\t{n_cases_after} / {n_cases_before} remain.')

    return df


def sufficient_obs(
        df: pd.DataFrame,
        min_obs_required: int = MIN_OBS_REQUIRED
) -> pd.DataFrame:

    obs_per_case = df.groupby('case_id').datetime.count()
    relevant_vases = obs_per_case[obs_per_case >= min_obs_required].index

    df_subset = df[df.case_id.isin(relevant_vases)].copy()
    LOGGER.info(f'Kept {len(df_subset.case_id.unique())} / {len(df.case_id.unique())} with sufficient observations.')

    return df_subset


def main():

    # run queries and load relevant data to parquet files
    query_mlife()

    # get a list of all relevant case files
    files = [f'{QUERY_OUTPUT_FOLDER}/{file}'
             for file in os.listdir(QUERY_OUTPUT_FOLDER)
             if file.startswith('case_')]

    # we need to track the percentage of missing data
    missing_pre_resample = {t: 0 for t in TARGETS}
    missing_post_resample = {t: 0 for t in TARGETS}
    n_obs_pre_resample = 0
    n_obs_post_resample = 0

    # loop over sub-lists of the case files (all at once can cause problems)
    for i, file_chunk in enumerate(np.array_split(files, 10), start=1):
        LOGGER.info(f'Processing chunk number {i} / 10')

        # load the output to dataframe
        df = pd.read_parquet(path=list(file_chunk))

        # reduce to one case per patient
        df = one_case_per_patient(df)

        # drop cases with too few observations
        df = sufficient_obs(df)

        # check the output of the query
        check_data(df, resampled=False)

        # cast values exceeding plausible limits to NA
        df = remove_extreme_values(df)

        # truncate timeseries: drop observations to start the TS if no target is not-null
        df = truncate_ts(df)

        # some reporting on NAs
        report_nas(df)

        n_obs_pre_resample += df.shape[0]
        for t in TARGETS:
            missing_pre_resample[t] += df[t].isna().sum()

        # forward-fill missing values and re-sample to 5 minutes
        df = ffill_resample_ts(df, minutes=RESAMPLING_MINUTES)

        n_obs_post_resample += df.shape[0]
        for t in TARGETS:
            missing_post_resample[t] += df[t].isna().sum()

        # check that everything's still fine after processing
        check_data(df, resampled=True, evenly_spaced_times=True)

        # store final result as parquet
        to_parquet_per_case(df, PROCESSED_DATA_PATH, clear_first=(i == 1))

    # store fraction of missing data
    na_per_target_pre = (pd.Series(missing_pre_resample) / n_obs_pre_resample).reset_index()
    na_per_target_post = (pd.Series(missing_post_resample) / n_obs_post_resample).reset_index()

    na_per_target_pre.to_csv('data/processed/na_per_target_mlife_pre_resampling.csv', sep=';', index=False)
    na_per_target_post.to_csv('data/processed/na_per_target_mlife_post_resampling.csv', sep=';', index=False)


if __name__ == '__main__':
    main()
