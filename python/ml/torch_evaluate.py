import os
import json
import pandas as pd
import matplotlib.pyplot as plt


def _load_hyperparams(
        trial_path: str
) -> dict:

    with open(f'{trial_path}/params.json') as param_file:
        params = json.load(param_file)
    return params


def _get_trials(
        model_name: str
) -> list:

    return [t for t in os.listdir(f'ray_results/{model_name}') if t.startswith('train_model')]


def _get_train_loss_per_trial(
        model_name: str,
        trial: str
) -> pd.Series:

    return pd.read_csv(f'ray_results/{model_name}/{trial}/progress.csv')['training_loss']


def _get_eval_loss_per_trial(
        model_name: str,
        trial: str
) -> pd.Series:

    return pd.read_csv(f'ray_results/{model_name}/{trial}/progress.csv')['loss']


def _get_loss_df(
        train_or_eval: str,
        model_name: str
) -> pd.DataFrame:

    if train_or_eval == 'eval':
        return pd.DataFrame({t: _get_eval_loss_per_trial(model_name, t) for t in _get_trials(model_name)})
    if train_or_eval == 'train':
        return pd.DataFrame({t: _get_train_loss_per_trial(model_name, t) for t in _get_trials(model_name)})
    return KeyError


def _get_best_trial(
        loss_df: pd.DataFrame
) -> str:

    return loss_df.min().sort_values().index[0]


def plot_losses(
        model_name: str
) -> None:

    eval_losses = _get_loss_df('eval', model_name)
    train_losses = _get_loss_df('train', model_name)

    best_trial = _get_best_trial(eval_losses)
    loss_best_trial = eval_losses[best_trial]

    _ = plt.subplots(1, 2, figsize=(22, 10), dpi=300)

    # plot evaluation loss
    _ = plt.subplot(1, 2, 1)
    for loss in eval_losses.columns:
        plt.plot(eval_losses.index + 1,
                 eval_losses[loss], linewidth=1)
        plt.ylim(0.95 * eval_losses.min().min(),
                 1.01 * eval_losses.max().max())
        plt.ylabel('Evaluation loss')
        plt.xlabel('Epoch')
        plt.axhline(y=loss_best_trial.min(),
                    linewidth=0.4,
                    alpha=0.4,
                    color='black',
                    linestyle='dashed')
        plt.axvline(x=loss_best_trial.index[loss_best_trial == loss_best_trial.min()] + 1,
                    linewidth=0.4,
                    alpha=0.4,
                    color='black',
                    linestyle='dashed')
    plt.title(f'{model_name} evaluation loss')

    # plot training loss
    _ = plt.subplot(1, 2, 2)
    for loss in train_losses.columns:
        plt.plot(train_losses.index + 1, train_losses[loss], linewidth=1)
        plt.ylim(0.99 * train_losses.min().min(), 1.01 * train_losses.max().max())
        plt.ylabel('Training loss')
        plt.xlabel('Epoch')
    plt.title(f'{model_name} training loss')

    plt.savefig(f'evaluation/plots/loss/loss_{model_name}.png')
    plt.close()


def plot_best_loss_per_hyper_param(
        model_name: str
):

    # get paths to all trials
    trials = _get_trials(model_name)

    # per trial: get the hyperparams and the smallest achieved loss
    hyperparams = [
        {**_load_hyperparams(f'ray_results/{model_name}/{t}'),
         **{'min_loss': pd.read_csv(f'ray_results/{model_name}/{t}/progress.csv').loss.min()},
         **{'mean_loss': pd.read_csv(f'ray_results/{model_name}/{t}/progress.csv').loss.mean()}} for t in trials
    ]

    # transform list of dicts to DataFrame
    hyperparams_df = pd.DataFrame({key: [h[key] for h in hyperparams] for key in hyperparams[0].keys()})
    for p in hyperparams[0].keys():
        if p not in ['min_loss', 'mean_loss', 'model']:
            plt.figure(figsize=(8, 8), dpi=100)
            plt.scatter(hyperparams_df[p], hyperparams_df['min_loss'], color='blue', label='Smallest')
            plt.scatter(hyperparams_df[p], hyperparams_df['mean_loss'], color='red', label='Mean')
            plt.xlabel(p)
            plt.ylabel('Achieved loss')
            plt.legend()
            plt.savefig(f'evaluation/plots/hyperparams/{model_name}_{p}.png')
            plt.close()
