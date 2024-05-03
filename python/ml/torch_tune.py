import os
import pandas as pd
import datetime
import numpy as np
from joblib import Parallel, delayed

import torch
from torch import nn
from torch.utils.data import DataLoader
from torch.optim import Adam

from hyperopt import hp
import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.hyperopt import HyperOptSearch

from python.data.data_utils import build_prediction_df
from python.ml.models.gru import GRU
from python.ml.models.ts_transformer import TSTransformerEncoderClassiRegressor
from python.ml.torch_data import VitalsDataset
from python.ml.torch_evaluate import plot_losses, plot_best_loss_per_hyper_param

from config import TARGETS, FORECAST_HORIZON
from python.utils import custom_logger, pickle_to_object, object_to_pickle

time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "__")
LOGGER = custom_logger('python/ml/torch_tune.py', f'python/logs/tune_{time}.txt')

GPU = torch.cuda.is_available()

METRIC = 'loss'
METRIC_MODE = 'min'

EPOCHS = 100
GRACE_PERIOD = max(min(10, EPOCHS - 1), 1)
N_SEARCHES = 50

MU_SIGMA = pickle_to_object('data/processed/mu_sigma_train.pkl')

TRAIN_LOADER = DataLoader(
    dataset=VitalsDataset(train_validate_or_test='train',
                          mu_sigma=MU_SIGMA,
                          subsample_size=None),
    batch_size=128,
    num_workers=30 * GPU,
    pin_memory=True
)

VAL_LOADER = DataLoader(
    dataset=VitalsDataset(train_validate_or_test='validate',
                          mu_sigma=MU_SIGMA,
                          subsample_size=None),
    batch_size=32,
    num_workers=30 * GPU,
    pin_memory=True
)

MODELS = ['gru', 'transformer']

# define some parameters used for both models
PARAMS = {
    'lr': hp.quniform('lr', 5e-5, 5e-3, 1e-5),
    'weight_decay': hp.quniform('weight_decay', 1e-6, 5e-5, 1e-6),
    'dropout': hp.quniform('dropout', 0.0, 0.4, 0.01),
    'num_layers': hp.choice('num_layers', [1, 2, 3, 4]),
    'hidden_size': hp.choice('hidden_size', [128, 256, 512])
}

SEARCH_SPACES = {
    'gru': {
        'search_space': PARAMS,
        'initial_params': {
            'lr': 5e-4,
            'weight_decay': 5e-6,
            'dropout': 0.1,
            'num_layers': 3,
            'hidden_size': 256
        }
    },
    'transformer': {
        'search_space': {
            'lr': PARAMS['lr'],
            'weight_decay': PARAMS['weight_decay'],
            'dropout': PARAMS['dropout'],
            'num_layers': PARAMS['num_layers'],
            'dim_feedforward': hp.choice('dim_feedforward', [128, 256, 512]),
            'activation': hp.choice('activation', ['gelu', 'relu']),
            'pos_encoding': hp.choice('pos_encoding', ['fixed', 'learnable']),
            'd_model': hp.choice('d_model', [64, 128, 256, 512]),
            'n_heads': hp.choice('n_heads', [8, 16]),
        },
        'initial_params': {
            'lr': 1e-3,
            'weight_decay': 5e-6,
            'dropout': 0.1,
            'num_layers': 3,
            'activation': 'gelu',
            'pos_encoding': 'learnable',
            'd_model': 128,
            'dim_feedforward': 256,
            'n_heads': 16
        }
    }
}


def check_train_val_test_overlap(
) -> None:

    path = 'data/processed/vitals'
    cases_train = os.listdir(f'{path}_train')
    cases_val = os.listdir(f'{path}_validate')
    cases_test = os.listdir(f'{path}_test')
    assert set(cases_train) & set(cases_val) & set(cases_test) == {'.gitkeep'}, 'Overlap!!!'
    LOGGER.info('No overlap between train/val/test sets.')


def non_imputed_mse(
        predictions: torch.Tensor,
        true_values: torch.Tensor,
        imputation_indicators: torch.Tensor
) -> torch.Tensor:

    # compute residuals
    residuals = predictions - true_values

    # set residuals to zero in case of imputation
    residuals = residuals * (~imputation_indicators)

    # square the residuals
    residuals_sqr = residuals ** 2

    # divide the sum of squared residuals by the number of non-imputed values
    return torch.sum(residuals_sqr) / torch.sum(~imputation_indicators)


def model_factory(
        model_name: str,
        hyperparams: dict
) -> GRU | TSTransformerEncoderClassiRegressor | KeyError:

    if model_name == 'gru':
        return GRU(
            hidden_size=hyperparams['hidden_size'],
            num_layers=hyperparams['num_layers'],
            dropout=hyperparams['dropout']
        )
    elif model_name == 'transformer':
        return TSTransformerEncoderClassiRegressor(
            feat_dim=len(TARGETS),
            max_len=TRAIN_LOADER.dataset.max_seq_len,   # noqa
            horizon=FORECAST_HORIZON,
            num_classes=len(TARGETS),
            activation=hyperparams['activation'],
            dropout=hyperparams['dropout'],
            pos_encoding=hyperparams['pos_encoding'],
            d_model=hyperparams['d_model'],
            n_heads=hyperparams['n_heads'],
            num_layers=hyperparams['num_layers'],
            dim_feedforward=hyperparams['dim_feedforward']
        )
    else:
        return KeyError(f'Invalid model_name {model_name}')


def load_model(
        model_name: str,
        path_to_checkpoint: str,
        hyperparams: dict
) -> GRU | TSTransformerEncoderClassiRegressor:

    model = model_factory(model_name, hyperparams)
    model_state, _ = torch.load(path_to_checkpoint)
    model.load_state_dict(model_state)
    return model


def predictions_to_parquet(
        torch_model: nn.Module,
        model_name: str
) -> None:

    torch_model = torch_model.to(torch.device('cpu'))

    def _single_case_predictions_to_parquet(case_file: str):

        build_prediction_df(
            model=torch_model,
            model_name=model_name,
            data_true=pd.read_parquet(f'{data_dir}/{case_file}'),
            mean_std_train=MU_SIGMA
        ).to_parquet(f'{output_path}/{model_name}__{case_file}')

    # make predictions for the internal test set and the eicu dataset
    for dataset in ['test', 'eicu']:

        LOGGER.info(f'Writing {dataset} predictions to parquet.')

        # define input and output folder
        data_dir = f'data/processed/vitals_{dataset}'
        output_path = f'data/predictions/{dataset}_predictions_ml'

        # delete predictions from previous trials
        if any(file.startswith(model_name) & file.endswith('.parquet') for file in os.listdir(output_path)):
            LOGGER.info('\tEmptying previous results.')
            os.system(f'rm -r {output_path}/{model_name}_*.parquet')

        case_files = set(os.listdir(data_dir)) - {'.gitkeep'}

        Parallel(n_jobs=-3)(
            delayed(_single_case_predictions_to_parquet)(case_file)
            for case_file in case_files
        )
        LOGGER.info('\tDone.')


def train_single_epoch(
        torch_model: nn.Module,
        device: torch.device,
        optimizer: Adam,
        train_loader: DataLoader
) -> np.ndarray:

    torch_model.train()
    loss_per_batch = []

    for batch_idx, (X, y, imp_indicator) in enumerate(train_loader):
        X, y = X.to(device), y.to(device)

        optimizer.zero_grad(set_to_none=True)
        # highlight padding in X for prediction
        predictions = torch_model(X, padding_masks=(X.sum(dim=2) != 0))
        # compute loss, take imputation into account
        loss = non_imputed_mse(
            predictions=predictions.cpu(),
            true_values=y.cpu(),
            imputation_indicators=imp_indicator
        )
        loss.backward()
        optimizer.step()
        loss_per_batch.append(loss.item())
    return np.mean(loss_per_batch)


def validate(
        torch_model: nn.Module,
        device: torch.device,
        val_loader: DataLoader
) -> np.ndarray:

    torch_model.eval()
    with torch.no_grad():
        loss_per_batch = []
        for batch_idx, (X, y, imp_indicator) in enumerate(val_loader):
            X, y = X.to(device), y.to(device)
            # highlight padding in X for prediction
            predictions = torch_model(X, padding_masks=(X.sum(dim=2) != 0))
            # compute loss and take imputation into account
            loss = non_imputed_mse(
                predictions=predictions.cpu(),
                true_values=y.cpu(),
                imputation_indicators=imp_indicator
            )
            loss_per_batch.append(loss)
    return np.mean(loss_per_batch)


def extract_best_model(
        model_name: str,
        ray_results: tune.ExperimentAnalysis,
        metric: str,
        mode: str
) -> GRU | TSTransformerEncoderClassiRegressor:

    # find the best trial
    best_trial = ray_results.get_best_trial(
        metric=metric,
        mode=mode,
        scope='all'
    )

    # get the chosen hyperparameters
    best_params = best_trial.config

    LOGGER.info(f'Best params for model {model_name}:')
    for key, val in best_params.items():
        LOGGER.info(f'\t{key}: {val}')

    # from the best trial, select the best checkpoint
    ray_results._legacy_checkpoint = False
    best_checkpoint = ray_results.get_best_checkpoint(
        trial=best_trial,
        metric=metric,
        mode=mode
    )
    best_checkpoint_path = best_checkpoint._local_path  # noqa
    LOGGER.info(f'\tBest checkpoint: {int(best_checkpoint_path.split("/checkpoint_")[1][:-1])}')

    # return the optimally tuned model
    best_model = load_model(model_name=model_name,
                            path_to_checkpoint=os.path.join(best_checkpoint_path, 'checkpoint'),
                            hyperparams=best_params)
    best_model.eval()

    # store the model outside the ray_results
    torch.save(best_model.state_dict(), f'python/ml/models/tuned_models/{model_name}')
    object_to_pickle(best_params, f'python/ml/models/tuned_models/{model_name}_hyperparams.pkl')

    return best_model


def train_model(
        config: dict,
        checkpoint_dir=None
) -> dict:

    if torch.cuda.is_available():
        device = torch.device('cuda')
    else:
        device = torch.device('cpu')
    m = model_factory(config['model'], config)
    m.to(device)

    optimizer = Adam(params=m.parameters(),
                     lr=config['lr'],
                     weight_decay=config['weight_decay'])

    train_loader = TRAIN_LOADER
    val_loader = VAL_LOADER

    if checkpoint_dir:
        model_state, optimizer_state = torch.load(
            os.path.join(checkpoint_dir, "checkpoint")
        )
        m.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    for epoch in range(1, EPOCHS + 1):

        start = datetime.datetime.now()

        training_loss = train_single_epoch(torch_model=m,
                                           device=device,
                                           optimizer=optimizer,
                                           train_loader=train_loader)

        validation_loss = validate(torch_model=m,
                                   device=device,
                                   val_loader=val_loader)

        if N_SEARCHES == 1:
            print(f'\t\tEpoch {epoch}: Val: {np.round(validation_loss, 4)} '
                  f'-- Train: {np.round(training_loss, 4)} '
                  f'-- {datetime.datetime.now() - start}')

        with tune.checkpoint_dir(epoch) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            torch.save((m.state_dict(), optimizer.state_dict()), path)

        yield {METRIC: validation_loss, 'training_loss': training_loss}


def main():

    # remove previous ray_results
    if 'ray_results' in os.listdir():
        os.system('rm -r ray_results')

    # make sure there is no overlap between train/val/test sets
    check_train_val_test_overlap()

    # loop over all models defined above
    for model_name in MODELS:

        params = SEARCH_SPACES[model_name]

        LOGGER.info(f'Starting to tune model {model_name}')

        hyperopt_search = HyperOptSearch(
            space=params['search_space'],
            points_to_evaluate=[params['initial_params']],
            metric=METRIC,
            mode=METRIC_MODE,
            random_state_seed=789
        )

        scheduler = AsyncHyperBandScheduler(
            metric=METRIC,
            mode=METRIC_MODE,
            max_t=EPOCHS,
            grace_period=GRACE_PERIOD
        )

        # initialize ray
        ray.init(
            logging_level='error',
            ignore_reinit_error=True
        )
        os.environ['RAY_AIR_NEW_OUTPUT'] = '0'

        start_time = datetime.datetime.now()
        LOGGER.info(f'Trying {N_SEARCHES} hyperparameter combinations with up to {EPOCHS} epochs.')
        tuning_results = tune.run(
            run_or_experiment=train_model,
            config={**{'model': model_name}, **params['search_space']},
            search_alg=hyperopt_search,
            scheduler=scheduler,
            num_samples=N_SEARCHES,
            max_concurrent_trials=3,
            resources_per_trial={'cpu': 1 - GPU, 'gpu': GPU},
            name=model_name,
            local_dir='ray_results',
            verbose=0
        )
        LOGGER.info(f'\t Time to tune: {str(datetime.datetime.now() - start_time).split(".")[0]}')

        best_model = extract_best_model(
            model_name=model_name,
            ray_results=tuning_results,
            metric=METRIC,
            mode=METRIC_MODE
        )

        predictions_to_parquet(
            torch_model=best_model,
            model_name=model_name
        )

        plot_losses(model_name)
        plot_best_loss_per_hyper_param(model_name)


if __name__ == '__main__':
    main()
