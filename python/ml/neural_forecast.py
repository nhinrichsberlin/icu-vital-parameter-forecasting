import datetime
import os

import pandas as pd
import numpy as np
import torch.cuda

from joblib import Parallel, delayed

from neuralforecast import NeuralForecast
from neuralforecast.auto import AutoNHITS, AutoNBEATS
from neuralforecast.models import NHITS, NBEATS

from ray import tune
from ray.tune.search.hyperopt import HyperOptSearch
from neuralforecast.losses.pytorch import MSE

from python.utils import (
    pickle_to_object,
    object_to_pickle,
    custom_logger,
    silence_ray,
    suppress_console_output
)

from config import (
    TARGETS,
    FORECAST_HORIZON,
    NEEDED_OUTPUT_COLUMNS,
    EXTREME_VALUE_LIMITS
)


LOGGER = custom_logger('neuralforecast.py')

N_SEARCHES = 25
NUM_CPUS = os.cpu_count() - 2
NUM_GPUS = torch.cuda.is_available()
MAX_STEPS = 100
BATCH_SIZE = 128

CASES_TRAIN_LIMIT = None
CASES_TEST_LIMIT = None

MU_SIGMA = pickle_to_object('data/processed/mu_sigma_train.pkl')

MODEL_CONFIGS = {

    'n_hits': {

        'max_steps': MAX_STEPS,
        'scaler_type': None,
        'val_check_steps': 1,
        'step_size': 1,
        'random_seed': 789,
        'batch_size': BATCH_SIZE,
        'dropout_prob_theta': tune.quniform(0.0, 0.4, 0.01),
        'learning_rate': tune.quniform(5e-5, 5e-3, 1e-5),
        'input_size': tune.choice([int(i * FORECAST_HORIZON) for i in (0.5, 1, 2, 3)]),
        'mlp_units': tune.choice([3 * [[n, n]] for n in (32, 64, 128)])

    },

    'n_beats': {

        'max_steps': MAX_STEPS,
        'scaler_type': None,
        'val_check_steps': 1,
        'step_size': 1,
        'random_seed': 789,
        'batch_size': BATCH_SIZE,
        'stack_types': ['identity', 'trend'],
        'n_polynomials': tune.choice([2, 3, 4]),
        'learning_rate': tune.quniform(5e-5, 5e-3, 1e-5),
        'input_size': tune.choice([int(i * FORECAST_HORIZON) for i in (0.5, 1, 2, 3)]),
        'mlp_units': tune.choice([3 * [[n, n]] for n in (32, 64, 128)])

    }
}

MODELS = [

    AutoNHITS(h=FORECAST_HORIZON,
              loss=MSE(),
              search_alg=HyperOptSearch(random_state_seed=789),
              config=MODEL_CONFIGS['n_hits'],
              num_samples=N_SEARCHES,
              gpus=NUM_GPUS,
              cpus=NUM_CPUS),

    AutoNBEATS(h=FORECAST_HORIZON,
               loss=MSE(),
               search_alg=HyperOptSearch(random_state_seed=789),
               config=MODEL_CONFIGS['n_beats'],
               num_samples=N_SEARCHES,
               gpus=NUM_GPUS,
               cpus=NUM_CPUS)
]


def model_alias(model):
    if isinstance(model, (NHITS, AutoNHITS)):
        return 'n_hits'
    if isinstance(model, (NBEATS, AutoNBEATS)):
        return 'n_beats'
    return KeyError, f'{model} does not have a specified name.'


def load_neuralforecast_data(dataset_name: str,
                             target: str,
                             case_id: str = None,
                             case_limit: int = None) -> pd.DataFrame:

    LOGGER.info(f'Loading {dataset_name} data for target {target}.')

    # loads training, validation or test data
    # in the required format for neuralforecast
    # and scales using the scaler fitted on the training data

    folder = f'data/processed/vitals_{dataset_name}'

    if case_id is None:

        cases = [f for f in os.listdir(folder) if f.endswith('.parquet')]

        # if needed, limit the number of cases
        if case_limit is not None:
            cases = cases[:case_limit]

        LOGGER.info(f'Loaded {len(cases)} cases.')

        df = pd.concat([pd.read_parquet(f'{folder}/{f}') for f in cases])

    else:
        df = pd.read_parquet(f'{folder}/case_{case_id}.parquet')

    cols_to_keep = ['datetime',
                    'case_id',
                    target]

    new_names = {'datetime': 'ds',
                 'case_id': 'unique_id',
                 target: 'y'}

    df = df[cols_to_keep].sort_values(['case_id', 'datetime']).rename(new_names, axis=1)

    # scale the target
    df['y'] = (df.y - MU_SIGMA[target]['mean']) / (MU_SIGMA[target]['std'])

    return df


def predict_case(test_or_eicu: str,
                 case_id: str) -> None:

    # define the dataframe which eventually contains all forecasts
    predictions = pd.DataFrame(columns=NEEDED_OUTPUT_COLUMNS)

    # loop over targets
    for target in TARGETS:

        # load the fitted model
        model = NeuralForecast.load(f'python/ml/models/tuned_models/neuralforecast__{target}/')

        # suppress logging to allow multiprocessing
        for m in model.models:
            m.trainer_kwargs['logger'] = False

        # assign aliases (needed because neuralforecast does not store the AutoModel, but the Model)
        for m in model.models:
            m.alias = model_alias(m)

        # load data as input for forecasts
        df_case = load_neuralforecast_data(
            dataset_name=test_or_eicu,
            target=target,
            case_id=case_id
        )

        # re-shape to allow fast model predictions
        # use unique_id to store the time of forecast and the value at forecast
        def _reshape_neuralforecast_data(d: pd.DataFrame, h: int):
            d = d.head(h).copy()
            d['unique_id'] = d.unique_id.astype(str) + '__' + str(d.ds.max()) + '__' + str(d.y.tail(1).item())
            return d

        df_case = pd.concat([
            _reshape_neuralforecast_data(df_case, i) for i in range(1, df_case.shape[0])
        ])

        # make predictions
        with suppress_console_output():
            predictions_target = model.predict(df_case).reset_index()

        # change format of predictions and rename columns
        predictions_target = pd.melt(
            frame=predictions_target,
            id_vars=['unique_id', 'ds'],
            value_vars=[m.alias for m in model.models],
            var_name='model_name',
            value_name='prediction'
        ).rename(
            {'unique_id': 'case_id',
             'ds': 'datetime'},
            axis=1
        )

        # split the unique_id/case_id column to extract the information stored in it
        predictions_target[['case_id', 'datetime_at_forecast', 'target_value_at_forecast']] = \
            predictions_target.case_id.str.split('__', n=2, expand=True)

        # make sure the column types are correct
        predictions_target['target_value_at_forecast'] = predictions_target.target_value_at_forecast.astype(float)
        predictions_target['datetime_at_forecast'] = pd.to_datetime(predictions_target.datetime_at_forecast)

        # add columns required for later evaluation
        predictions_target['datetime_first_obs'] = df_case.ds.min()
        predictions_target['target_type'] = target

        # de-standardize the target
        for c in ['prediction', 'target_value_at_forecast']:
            predictions_target[c] = predictions_target[c] * MU_SIGMA[target]['std'] + MU_SIGMA[target]['mean']

        # load the true values and info on imputation
        true_vals = pd.read_parquet(
            path=f'data/processed/vitals_{test_or_eicu}/case_{case_id}.parquet',
            columns=['datetime', target, f'{target}__NA']) \
            .rename({target: 'target_value',
                    f'{target}__NA': 'target_imputed'},
                    axis=1) \
            .set_index('datetime')

        # join the two dataframes
        predictions_target = predictions_target.join(true_vals, on='datetime')

        # make sure we have all the required columns in the right order
        predictions_target = predictions_target[NEEDED_OUTPUT_COLUMNS]

        # drop observations without true values
        predictions_target = predictions_target[predictions_target.target_value.notna()]

        # clip predictions at the pre-defined limits of plausibility
        predictions_target['prediction'] = predictions_target.prediction.clip(EXTREME_VALUE_LIMITS[target]['min'],
                                                                              EXTREME_VALUE_LIMITS[target]['max'])

        # add predictions_target to predictions
        predictions = predictions.append(predictions_target)

    # store predictions per model
    for m in predictions.model_name.unique():
        predictions[predictions.model_name == m].to_parquet(
            f'data/predictions/{test_or_eicu}_predictions_ml/{m}__case_{case_id}.parquet'
        )


def log_best_params(nf: NeuralForecast,
                    target_type: str):

    for model in nf.models:

        LOGGER.info(f'Best hyperparameters for model {model_alias(model)}:')

        results_df = model.results.get_dataframe().sort_values('loss').reset_index()
        cols = [f'config/{k}' for k in MODEL_CONFIGS[model_alias(model)].keys()]

        best_params = {}

        for c in cols:
            param_name = c.split("/")[1]
            best_results = [results_df.loc[i, c] for i in range(min(3, N_SEARCHES))]
            best_params[param_name] = best_results[0]

            if isinstance(best_results[0], float):
                best_results = [np.round(r, 5) for r in best_results]
            if best_results.count(best_results[0]) == len(best_results):
                best_results = best_results[0]
            LOGGER.info(f'{param_name}: {best_results}')

        object_to_pickle(
            best_params,
            f'python/ml/models/tuned_models/{model_alias(model)}_{target_type}_hyperparams.pkl'
        )


def main():

    # suppress ray output
    silence_ray()

    for target in TARGETS:

        LOGGER.info(f'\n\n\t{target}\n\n')

        # load training, internal test and eicu data
        df_train = load_neuralforecast_data(
            dataset_name='train',
            target=target,
            case_limit=CASES_TRAIN_LIMIT
        )

        # create a neuralforecast object
        nf = NeuralForecast(
            models=MODELS,
            freq='5min'
        )

        # fit the model
        LOGGER.info(f'Fitting models {[model_alias(m) for m in nf.models]} for target {target}.')

        with suppress_console_output():
            time = datetime.datetime.now()
            nf.fit(df_train)
            LOGGER.info(f'Time to fit: {datetime.datetime.now() - time}')

        # show the best performing hyperparameters
        log_best_params(nf, target)

        path_to_model = f'python/ml/models/tuned_models/neuralforecast__{target}'
        nf.save(
            path=path_to_model,
            model_index=None,
            overwrite=True,
            save_dataset=True
        )

    # make predictions and store in the proper format
    for d in ['test', 'eicu']:

        LOGGER.info(f'\n\nMaking predictions on {d} data.\n\n')

        # get a list of all cases
        cases = [case_file.split('case_')[1].split('.parquet')[0]
                 for case_file in os.listdir(f'data/processed/vitals_{d}')
                 if case_file.endswith('.parquet')]

        if CASES_TEST_LIMIT is not None:
            cases = cases[:CASES_TEST_LIMIT]

        # loop over all cases
        time = datetime.datetime.now()
        Parallel(n_jobs=-1)(delayed(predict_case)(
            d,              # test_or_eicu
            case,           # case_id
        ) for case in cases)
        LOGGER.info(f'All cases done in {datetime.datetime.now() - time}')


if __name__ == '__main__':
    main()
