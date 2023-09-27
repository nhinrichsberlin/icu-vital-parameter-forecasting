import os
from typing import Optional
from functools import cached_property
from pyarrow.parquet import ParquetDataset
import pandas as pd

from torch.utils.data import Dataset
from python.data.data_utils import build_x_y_single_case
from python.utils import custom_logger
from config import TARGETS, FORECAST_HORIZON


LOGGER = custom_logger('torch_data.py')


class VitalsDataset(Dataset):
    def __init__(self,
                 train_validate_or_test: str,
                 mu_sigma: Optional[dict] = None,
                 targets: list = TARGETS,
                 horizon: int = FORECAST_HORIZON,
                 subsample_size: Optional[int] = None):

        self.train_validate_or_test = train_validate_or_test
        self.mu_sigma = mu_sigma if mu_sigma is not None else {f: {'mean': 0, 'std': 1} for f in targets}
        self.targets = targets
        self.horizon = horizon
        self.data_directory = os.path.abspath(f'data/processed/vitals_{train_validate_or_test}')
        self.subsample_size = subsample_size
        self.case_files = self.get_case_files
        self.max_len = self.max_seq_len
        self.current_epoch = 0
        LOGGER.info(f'\n\n Dataset {self.train_validate_or_test}: {len(self.case_files)} cases. \n')

    @cached_property
    def get_case_files(self):
        available_files = \
            sorted(
                list(
                    set(
                        os.listdir(f'data/processed/vitals_{self.train_validate_or_test}')
                    ) - {'.gitkeep', '.DS_Store'}
                )
            )
        if self.subsample_size is not None:
            return available_files[:self.subsample_size]
        else:
            return available_files

    @cached_property
    def max_seq_len(self):
        cases = ParquetDataset(self.data_directory, use_legacy_dataset=False)
        return sorted(cases.fragments, key=lambda f: f.count_rows(), reverse=True)[0].count_rows()

    def _read_case_data(self, case_file: str):
        df_case = pd.read_parquet(
            os.path.join(self.data_directory, case_file)
        )
        return df_case

    def __len__(self):
        return len(self.case_files)

    def __getitem__(self, idx):
        vitals_case = self._read_case_data(self.case_files[idx])
        # re-shape the sub-set into tensors
        X, y, imputation_indicators = build_x_y_single_case(
            df_case=vitals_case,
            targets=self.targets,
            horizon=self.horizon,
            mu_sigma=self.mu_sigma,
            desired_seq_len=self.max_len
        )
        return X, y, imputation_indicators
