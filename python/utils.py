from contextlib import contextmanager, redirect_stderr, redirect_stdout
from typing import Any
import yaml
import datetime
import logging
import pickle
import os
import pandas as pd
import duckdb
import psycopg2
from joblib import Parallel, delayed


def load_vitals_from_parquet(
        train_val_test_eicu: str,
        cols_to_read=None
) -> pd.DataFrame:

    folder = f'data/processed/vitals_{train_val_test_eicu}'
    files = [f'{folder}/{f}' for f in os.listdir(folder) if f.endswith('.parquet')]
    df = pd.concat([pd.read_parquet(f, columns=cols_to_read) for f in files])

    return df



def yaml_to_dict(
        file: str
) -> dict:

    with open(file) as f:
        output_dict = yaml.load(f, Loader=yaml.FullLoader)
    return output_dict


def object_to_pickle(
        obj: Any,
        filename: str
) -> None:

    with open(filename, 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)


def pickle_to_object(
        filename: str
) -> Any:

    with open(filename, 'rb') as input_file:
        return pickle.load(input_file)


def custom_logger(
        file: str,
        output_path: str = None
) -> logging.Logger:

    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)

    if output_path is not None:
        logging.basicConfig(filename=output_path)

    # avoid duplications when running the same thing multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # some formatting
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def read_format_query(
        path_to_query: str,
        query_vars: dict = None
) -> bytes:

    # open raw query
    with open(path_to_query, 'r') as sql_file:
        query = sql_file.read()

    query = query if not query_vars else query.format(**query_vars)
    query = query.encode('utf-8')

    return query


def to_parquet_per_case(
        df: pd.DataFrame,
        output_folder: str,
        clear_first: bool = True
) -> None:

    start_time = datetime.datetime.now()
    logger = custom_logger('to_parquet_per_case')
    logger.info(f'Storing {len(df.case_id.unique())} cases to folder {output_folder}.')

    if clear_first and any(file.endswith('.parquet') for file in os.listdir(output_folder)):
        logger.info(f'Emptying previous entries in {output_folder}')
        [
            os.remove(os.path.join(output_folder, f))
            for f in os.listdir(output_folder)
            if f.endswith('.parquet')
        ]
        logger.info('\tEmptying complete')

    def _case_to_parquet(dfc: pd.DataFrame):
        assert len(dfc.case_id.unique()) == 1, 'More than one case!'
        c = str(dfc.case_id.min())
        if c.isdigit() or (c.endswith('.0') and c.split('.')[0].isdigit()):
            c = int(float(c))
        dfc.to_parquet(f'{output_folder}/case_{c}.parquet')

    Parallel(n_jobs=-3)(delayed(_case_to_parquet)(df[df.case_id == case]) for case in df.case_id.unique())

    logger.info(f'\tAll done in {datetime.datetime.now() - start_time}')


def duckdb_query_to_pandas_df(
        query: str
) -> pd.DataFrame:

    con = duckdb.connect(':memory:', read_only=False)

    df = con.execute(query).fetchdf()

    con.close()

    if '__null_dask_index__' in df.columns:
        df = df.drop('__null_dask_index__', axis=1)

    return df


def _postgres_connection():

    db_params = yaml_to_dict('config/config_eicu.yaml')

    con = psycopg2.connect(
        dbname=db_params['dbname'],
        host=db_params['host'],
        port=db_params['port'],
        user=db_params['user'],
        password=db_params['password']
    )
    return con


def postgres_query_to_pandas_df(
        query: str
) -> pd.DataFrame:

    con = _postgres_connection()

    data = pd.read_sql_query(query, con)

    con.close()

    return data


def postgres_execute_query(
        query: str
) -> None:

    con = _postgres_connection()

    cursor = con.cursor()

    cursor.execute(query)

    con.commit()

    con.close()


def silence_ray():
    import ray
    ray.init(log_to_driver=False)
    os.environ['RAY_AIR_NEW_OUTPUT'] = '0'


@contextmanager
def suppress_console_output():
    """A context manager that redirects stdout and stderr to devnull"""
    with open(os.devnull, 'w') as f_null:
        with redirect_stderr(f_null) as err, redirect_stdout(f_null) as out:
            yield err, out
