import pandas as pd
from seaborn import violinplot
import matplotlib.pyplot as plt

from python.utils import load_vitals_from_parquet, custom_logger
from config import TARGETS, TARGET_LABELS, EXTREME_VALUE_LIMITS

DATASETS = ['train', 'validate', 'test', 'eicu']
DATASET_NAMES = {'train': 'Training set',
                 'validate': 'Validation set',
                 'test': 'Internal test set',
                 'eicu': 'External test set (eICU)'}

LOWER_LIMITS = {
    'blood_pressure_systolic': 1,
    'blood_pressure_diastolic': 1,
    'blood_pressure_mean': 1,
    'central_venous_pressure': -18,
    'oxygen_saturation': 36,
    'heart_rate': 1
}

TARGET_TICKS = {
    'blood_pressure_systolic':  [50, 100, 150, 200, 250, 300, 350],
    'blood_pressure_diastolic': [30, 60, 90, 120, 150],
    'blood_pressure_mean':      [30, 60, 90, 120, 150],
    'central_venous_pressure':  [-10, 0, 10, 20, 30, 40, 50],
    'oxygen_saturation':        [40, 50, 60, 70, 80, 90, 100],
    'heart_rate':               [50, 100, 150, 200, 250, 300]
}

LOGGER = custom_logger('plot_distributions.py')


def main():

    # load data
    data = pd.concat(
        [load_vitals_from_parquet(d, TARGETS).assign(dataset=DATASET_NAMES[d]) for d in DATASETS]
    )

    LOGGER.info(f'Loaded data of shape {data.shape}')

    _ = plt.subplots(3, 2, figsize=(7.5, 8.75), dpi=1000)

    for i, t in enumerate(TARGETS, start=1):

        LOGGER.info(f'Adding plot for target {t}')

        _ = plt.subplot(3, 2, i)

        legend = i in [3, 5]

        v = violinplot(
            data=data,
            y=t,
            hue='dataset',
            width=0.95,
            legend="auto" if legend else False,
            cut=0,
            inner='quart',
            bw_adjust=1.15
        )

        if legend:
            plt.legend(ncol=4,
                       prop=dict(size=8),
                       loc='upper center',
                       bbox_to_anchor=(1.1, 1.175))

        _ = plt.ylabel(TARGET_LABELS[t], weight='bold', size=7.5)
        _ = plt.yticks(ticks=TARGET_TICKS[t])
        v.tick_params(axis='y', which='major', pad=0, labelsize=7.5, width=0.5, length=2)
        _ = plt.xlabel('')
        _ = plt.xticks([])
        _ = plt.ylim(LOWER_LIMITS[t], 1.05 * EXTREME_VALUE_LIMITS[t]['max'])

    _ = plt.savefig(
        'evaluation/plots/performance/target_distributions.tiff',
        bbox_inches='tight',
        format="tiff",
        pil_kwargs={"compression": "tiff_lzw"}
    )

    LOGGER.info('Plot saved.')


if __name__ == '__main__':
    main()
