from typing import Dict

import xarray as xr
import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn import preprocessing

from cemc_data_tool.ml.moml.data_loader.running_period import (
    get_train_periods,
)
from cemc_data_tool.ml.moml.data_loader.dataset_3 import (
    extract_test_dataset,
    extract_train_dataset
)
from cemc_data_tool.ml.moml.data_loader.dataset import (
    reshape_array_to_sample,
    reshape_array_to_samples
)
from cemc_data_tool.ml.moml.validate.index import rmse


class MosModel(object):
    def __init__(
            self,
            train_period_type: str = "running"
    ):
        self.train_period_type = train_period_type

    def set_dataset(self, input_ds: xr.Dataset, output_ds: xr.Dataset):
        self.input_ds = input_ds
        self.output_ds = output_ds

    def train(
            self,
            sample_start_time: pd.Timestamp,
            sample_forecast_time: pd.Timedelta
    ) -> LinearRegression:
        if self.train_period_type == "running":
            train_periods = get_train_periods(sample_start_time, sample_forecast_time)
        else:
            raise ValueError(f"train period type {self.train_period_type} is not supported.")

        self.test_input_ds, self.test_output_ds = extract_test_dataset(
            self.input_ds, self.output_ds, sample_start_time, sample_forecast_time
        )

        self.train_input_ds, self.train_output_ds = extract_train_dataset(
            self.input_ds, self.output_ds, train_periods, sample_forecast_time
        )

        self.test_output = reshape_array_to_sample(
            self.test_output_ds.to_array().transpose("variable", ...).values
        )
        self.test_input = reshape_array_to_sample(
            self.test_input_ds.to_array().transpose("variable", ...).values
        )
        self.train_output = reshape_array_to_samples(
            self.train_output_ds.to_array().transpose("time", "variable", ...).values
        )
        self.train_input = reshape_array_to_samples(
            self.train_input_ds.to_array().transpose("time", "variable", ...).values
        )

        self.input_scaler = preprocessing.StandardScaler().fit(self.train_input)
        train_input_norm = self.input_scaler.transform(self.train_input)
        self.output_scaler = preprocessing.StandardScaler().fit(self.train_output)
        train_output_norm = self.output_scaler.transform(self.train_output)

        self.linear_model = LinearRegression()
        self.linear_model.fit(train_input_norm, train_output_norm)
        return self.linear_model

    def predict(self) -> np.ndarray:
        test_input_norm = self.input_scaler.transform(self.test_input)

        predicted_value = self.output_scaler.inverse_transform(
            self.linear_model.predict(test_input_norm)
        )

        return predicted_value

    def calculate_index(self, predicted_value: np.ndarray, raw_value: np.ndarray) -> Dict:
        predict_rmse = rmse(predicted_value, self.test_output)
        raw_rmse = rmse(raw_value, self.test_output)
        return {
            "rmse_predict": predict_rmse,
            "rmse_raw": raw_rmse
        }
