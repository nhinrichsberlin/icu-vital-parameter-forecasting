from typing import Tuple, Union
import pandas as pd
import numpy as np

import torch.nn.functional as F # noqa
from torch import (
    nn,
    from_numpy,
    Tensor,
    no_grad,
    nan_to_num
)

from python.utils import custom_logger
from config import (
    TARGETS,
    FORECAST_HORIZON,
    NEEDED_OUTPUT_COLUMNS,
    RESAMPLING_MINUTES,
    EXTREME_VALUE_LIMITS
)


LOGGER = custom_logger('data/data_utils.py')


def mean_std_per_var(
        df_train: pd.DataFrame,
        variables: list = TARGETS
) -> dict:

    scaler = {
        v:
            {'mean': df_train[v].mean(),
             'std': df_train[v].std()}
        for v in variables
    }

    LOGGER.info('Mean and std. of targets:')
    for var, mu_sigma in scaler.items():
        LOGGER.info(f'\t{var}: m={np.round(mu_sigma["mean"], 3)}, s={np.round(mu_sigma["std"], 3)}')

    return scaler


def standardize_data(
        df: pd.DataFrame,
        std_params: dict
) -> pd.DataFrame:

    df = df.copy()

    for var in std_params.keys():
        if not var.endswith('__NA'):
            # handle weird case of a constant variable (can happen when using small samples for testing)
            if std_params[var]['std'] == 0:
                df[var] = (df[var] - std_params[var]['mean'])
            # normal case
            else:
                df[var] = (df[var] - std_params[var]['mean']) / std_params[var]['std']

    return df


def add_padding(
        tensor_to_pad: Tensor,
        n_pads: int,
        value: Union[int, float]
) -> Tensor:

    # define the padding shape
    padding_shape = [0 for _ in range(2 * len(tensor_to_pad.shape))]
    padding_shape[-1] = n_pads

    # return the padded tensor
    return F.pad(
        input=tensor_to_pad,
        pad=tuple(padding_shape),
        mode='constant',
        value=value
    )


def build_x_y_single_case(
        df_case: pd.DataFrame,
        mu_sigma: dict,
        desired_seq_len: int = None,
        targets: list = TARGETS,
        horizon: int = FORECAST_HORIZON
) -> Tuple[Tensor, Tensor, Tensor]:

    # standardize the data using the passed parameters
    df_case = standardize_data(df_case, mu_sigma)

    # creating X of shape (sequence_length, nr. of targets + nr. of features)
    X = from_numpy(df_case[targets].values)

    # creating y
    # build a data frame containing future shifts of the targets
    df_future = pd.concat([
        df_case[targets].shift(-i).rename({t: f'{t}__next_{i}' for t in targets}, axis=1) for i in range(1, horizon + 1)
    ], axis=1).ffill()
    y = from_numpy(df_future.values)

    # re-shape to shape (sequence_length, horizon, nr. of targets)
    y = y.reshape(df_case.shape[0], horizon, len(targets))

    # fill NAs (no NAs expected in X)
    y = nan_to_num(y, 0)

    # build a DF indicating which target values are results of imputation
    target_nas = [f'{t}__NA' for t in targets]
    df_y_imputed = pd.concat([
        df_case[target_nas].shift(-i) for i in range(1, horizon + 1)
    ], axis=1).fillna(1)
    y_imputed = from_numpy(df_y_imputed.values)\

    # re-shape to match the shape of y
    y_imputed = y_imputed.reshape(df_case.shape[0], horizon, len(targets))

    # add padding if sequence is "too short"
    seq_len = X.shape[0]

    if desired_seq_len is not None and seq_len < desired_seq_len:
        X = add_padding(X, desired_seq_len - seq_len, 0)
        y = add_padding(y, desired_seq_len - seq_len, 0)
        y_imputed = add_padding(y_imputed, desired_seq_len - seq_len, 1)

    # return float tensors
    return X.float(), y.float(), y_imputed.bool()


def _unpack_predictions_single_case_and_target(
        df_predict_case: pd.DataFrame,
        target_type: str,
        target_number: int,
        predictions: np.array,
        model_name: str,
        mean_std_train: dict,
        horizon: int = FORECAST_HORIZON
) -> pd.DataFrame:

    # create a prediction_df for this target
    # at every datetime we have 'horizon' number of forecasts
    prediction_df_case_target = df_predict_case.loc[
        df_predict_case.index.repeat(horizon),
        ['case_id', 'datetime']
    ].rename({'datetime': 'datetime_at_forecast'}, axis=1).sort_values(['case_id', 'datetime_at_forecast'])

    # add the target type
    prediction_df_case_target['target_type'] = target_type

    # add the model name
    prediction_df_case_target['model_name'] = model_name

    # from the predictions, separate the relevant target and de-standardize
    predictions_t = predictions[:, target_number]

    # de-standardize
    predictions_t = predictions_t * mean_std_train[target_type]['std'] + mean_std_train[target_type]['mean']

    # insert the predictions
    prediction_df_case_target['prediction'] = predictions_t

    # clip predictions at the defined extreme value limits
    prediction_df_case_target['prediction'] = \
        prediction_df_case_target.prediction.clip(
            EXTREME_VALUE_LIMITS[target_type]['min'],
            EXTREME_VALUE_LIMITS[target_type]['max']
        )

    # add time to which the forecast refers
    prediction_df_case_target['forecast_minutes_ahead'] = pd.to_timedelta(
        RESAMPLING_MINUTES
        * (prediction_df_case_target.groupby('datetime_at_forecast').cumcount() + 1),
        unit='min'
    )

    prediction_df_case_target['datetime'] = \
        prediction_df_case_target['datetime_at_forecast'] + prediction_df_case_target['forecast_minutes_ahead']

    # add the true values
    df_to_join = df_predict_case[['datetime', target_type, f'{target_type}__NA']].set_index(['datetime'])
    prediction_df_case_target = prediction_df_case_target.join(
        df_to_join,
        on='datetime'
    ).rename({target_type: 'target_value', f'{target_type}__NA': 'target_imputed'}, axis=1)

    # add the true values at forecast time
    df_to_join = df_predict_case[['datetime', target_type]]\
        .rename({'datetime': 'datetime_at_forecast'}, axis=1)\
        .set_index('datetime_at_forecast')
    prediction_df_case_target = prediction_df_case_target.join(
        df_to_join,
        on='datetime_at_forecast'
    ).rename({target_type: 'target_value_at_forecast'}, axis=1)

    return prediction_df_case_target


def _unpack_predictions_single_case(
        model: nn.Module,
        model_name: str,
        df_predict_case: pd.DataFrame,
        mean_std_train: dict,
        targets: list = TARGETS,
        horizon: int = FORECAST_HORIZON
) -> pd.DataFrame:

    X_predict, y_predict, imputation_indicators = build_x_y_single_case(
        df_case=df_predict_case,
        mu_sigma=mean_std_train,
        targets=targets,
        horizon=horizon
    )

    # add batch dimension (batch_size = 1 here, because we do this by case)
    X_predict = X_predict.reshape(1, X_predict.size(0), X_predict.size(1))

    # make predictions, shape (sequence_length, horizon, n_output_ts)
    with no_grad():
        predictions = model(X_predict).detach().numpy()

    # drop the batch dimension
    predictions = predictions[0]

    # re-shape predictions into (n_samples * horizon, n_output_ts)
    predictions = predictions.reshape(predictions.shape[0] * predictions.shape[1], predictions.shape[2])

    # create an empty prediction df which we fill target by target
    prediction_df_case = pd.DataFrame()
    for i, t in enumerate(targets):
        prediction_df_case_target = _unpack_predictions_single_case_and_target(
            df_predict_case=df_predict_case,
            target_type=t,
            target_number=i,
            predictions=predictions,
            model_name=model_name,
            mean_std_train=mean_std_train,
            horizon=horizon
        )

        # add this to the prediction_df_case
        prediction_df_case = pd.concat([prediction_df_case, prediction_df_case_target], axis=0)

    # add datetime_first_obs
    prediction_df_case['datetime_first_obs'] = prediction_df_case.datetime_at_forecast.min()

    return prediction_df_case


def build_prediction_df(
        model: nn.Module,
        model_name: str,
        data_true: pd.DataFrame,
        mean_std_train: dict,
        targets: list = TARGETS,
        horizon: int = FORECAST_HORIZON
) -> pd.DataFrame:

    prediction_df = pd.DataFrame()
    for case in data_true.case_id.unique():
        prediction_df_case = _unpack_predictions_single_case(
            model=model,
            model_name=model_name,
            df_predict_case=data_true[data_true.case_id == case].sort_values('datetime'),
            mean_std_train=mean_std_train,
            targets=targets,
            horizon=horizon
        )
        prediction_df = pd.concat([prediction_df, prediction_df_case])

    # check that we have the right columns
    assert all(col in prediction_df.columns for col in NEEDED_OUTPUT_COLUMNS), 'Missing column!'

    # drop cases with NAs (forecasts past the observed horizon)
    prediction_df = prediction_df.dropna()

    # bring columns and rows in order
    prediction_df = \
        prediction_df[NEEDED_OUTPUT_COLUMNS]\
        .sort_values(['case_id', 'target_type', 'datetime_at_forecast', 'datetime'])

    return prediction_df
