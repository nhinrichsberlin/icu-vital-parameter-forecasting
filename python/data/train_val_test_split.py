from typing import Tuple
import pandas as pd

from sklearn.impute import SimpleImputer

from python.utils import (
    custom_logger,
    object_to_pickle,
    to_parquet_per_case
)

from python.data.generate_mlife import (
    TARGETS,
    PROCESSED_DATA_PATH,
    check_data
)

from python.data.data_utils import mean_std_per_var


LOGGER = custom_logger('data/train_val_test_split.py')


def train_val_test_patients(
        df: pd.DataFrame,
        random_state: int = 123
) -> Tuple[pd.Series, pd.Series, pd.Series]:

    # get all patients
    patients = pd.Series(df.patient_id.unique())

    # randomly select 80% into the training cases
    patients_train = patients.sample(frac=0.8, random_state=random_state)

    # split the remaining 50/50 into validation and test cases
    patients_validate = patients[-patients.isin(patients_train)].sample(frac=0.5, random_state=random_state)

    patients_test = patients[(-patients.isin(patients_train)) & (-patients.isin(patients_validate))]

    # make sure there is no overlap
    assert set(patients_test) & set(patients_validate) & set(patients_train) == set()

    return patients_train, patients_validate, patients_test


def split_by_patients(
        data: pd.DataFrame,
        patients: pd.Series
) -> pd.DataFrame:

    return data[data.patient_id.isin(patients)]


def train_val_test_split(
        df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    LOGGER.info(f'Splitting the DF of shape {df.shape} into train, val, test by patient_id.')
    n_patients = len(df.patient_id.unique())

    # split the data
    data_train, data_validate, data_test = (split_by_patients(df, cases) for cases in train_val_test_patients(df))

    LOGGER.info(f'Train: {len(data_train.patient_id.unique())} / {n_patients} patients.')
    LOGGER.info(f'Validate: {len(data_validate.patient_id.unique())} / {n_patients} patients.')
    LOGGER.info(f'Test: {len(data_test.patient_id.unique())} / {n_patients} patients.')

    # some checks, just as a precaution
    assert set(data_train.case_id) & set(data_validate.case_id) & set(data_test.case_id) == set()
    assert set(data_train.patient_id) & set(data_validate.patient_id) & set(data_test.patient_id) == set()

    for df in data_train, data_validate, data_test:
        check_data(df, resampled=True)

    return data_train, data_validate, data_test


def fit_imputer(
        df_train: pd.DataFrame,
        cols_to_impute: list
) -> SimpleImputer:

    LOGGER.info(f'Fitting a median imputer on training data of shape {df_train.shape}')
    imputer = SimpleImputer(strategy='median')
    imputer.fit(df_train[cols_to_impute])
    return imputer


def impute_columns(
        df: pd.DataFrame,
        imputer: SimpleImputer,
        cols_to_impute: list
) -> pd.DataFrame:

    # sort, just to be sure everything's in order
    df = df.sort_values(['patient_id', 'case_id', 'datetime'])

    # impute one case at a time (memory issues)
    for i, case in enumerate(df.case_id.unique(), start=1):

        df_case = df[df.case_id == case].copy()

        if i % 500 == 0 or i == 1:
            LOGGER.info(f'Imputing case {i} / {len(df.case_id.unique())}')

        if df_case.isna().any().any():
            imputed_features = pd.DataFrame(
                data=imputer.transform(df_case[cols_to_impute]),
                columns=cols_to_impute,
                index=df_case.index
            )
            # fill missing values with imputed features
            df.loc[df.case_id == case, cols_to_impute] = \
                df.loc[df.case_id == case, cols_to_impute].fillna(imputed_features)

    return df


def store_mu_sigma_train(
        df_train: pd.DataFrame
) -> None:

    # compute mean and std per variable
    mean_std_train = mean_std_per_var(df_train)
    # store the output for later use
    object_to_pickle(mean_std_train, 'data/processed/mu_sigma_train.pkl')


def store_imputer(
        strategy: str,
        imputer: SimpleImputer
) -> None:

    LOGGER.info(f'Storing {strategy} imputer.')
    object_to_pickle(imputer, f'data/processed/{strategy}_imputer.pkl')


def main():

    # load the processed and re-sampled data
    df_processed = pd.read_parquet(PROCESSED_DATA_PATH)

    # split into train, validation, and test set
    df_train, df_val, df_test = train_val_test_split(df_processed)

    # impute missing values using the median
    # fit an imputer on the training data
    imputer = fit_imputer(
        df_train=df_train,
        cols_to_impute=TARGETS
    )

    # store the imputer for later use (EICU dataset)
    store_imputer('median', imputer)

    # insert the imputed values
    df_train = impute_columns(df_train, imputer, TARGETS)
    df_val = impute_columns(df_val, imputer, TARGETS)
    df_test = impute_columns(df_test, imputer, TARGETS)

    # check whether the data is still in good shape
    check_data(
        df=pd.concat([df_train, df_val, df_test]),
        resampled=True,
        na_allowed=False
    )

    # store mean and variance per target/feature in the training set for standardizing
    store_mu_sigma_train(df_train)

    # store the un-standardized data to parquet files
    for name, df in [('train', df_train), ('validate', df_val), ('test', df_test)]:
        to_parquet_per_case(df, f'data/processed/vitals_{name}')


if __name__ == '__main__':
    main()
