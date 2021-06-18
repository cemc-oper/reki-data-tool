import xarray as xr
import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn import preprocessing

from moml.data_loader.running_period import (
    get_train_periods,
    extract_test_output,
    extract_test_input,
    extract_train_input_output,
    reshape_array_to_sample,
    reshape_array_to_samples
)
from moml.index import rmse


class MosModel(object):
    def __init__(self, train_period_type="running"):
        self.train_period_type = train_period_type

    def set_dataset(self, ds):
        self.ds = ds

    def train(self, sample_start_time, sample_forecast_time):
        if self.train_period_type == "running":
            train_periods = get_train_periods(sample_start_time)
        else:
            raise ValueError(f"train period type {self.train_period_type} is not supported.")

        self.test_output_ds = extract_test_output(self.ds, sample_start_time, sample_forecast_time)
        self.test_input_ds = extract_test_input(self.ds, sample_start_time, sample_forecast_time)
        self.train_input_ds, self.train_output_ds = extract_train_input_output(self.ds, train_periods, sample_forecast_time)

        self.test_output = reshape_array_to_sample(self.test_output_ds.values)
        self.test_input = reshape_array_to_sample(self.test_input_ds.values)
        self.train_output = reshape_array_to_samples(self.train_output_ds.values)
        self.train_input = reshape_array_to_samples(self.train_input_ds.values)

        self.scaler = preprocessing.StandardScaler().fit(self.train_input)
        train_input_norm = self.scaler.transform(self.train_input)
        train_output_norm = self.scaler.transform(self.train_output)

        self.linear_model = LinearRegression()
        self.linear_model.fit(train_input_norm, train_output_norm)
        return self.linear_model

    def predict(self):
        test_input_norm = self.scaler.transform(self.test_input)

        predicted_value = self.scaler.inverse_transform(
            self.linear_model.predict(test_input_norm)
        ).reshape(self.test_output_ds.shape)

        return predicted_value

    def calculate_index(self, predicted_value):
        predict_rmse = rmse(predicted_value, self.test_output_ds.values)
        raw_rmse = rmse(self.test_input_ds.values, self.test_output_ds.values)
        return {
            "rmse_predict": predict_rmse,
            "rmse_raw": raw_rmse
        }
