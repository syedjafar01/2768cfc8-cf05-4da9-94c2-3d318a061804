from abc import abstractmethod
import pickle
import os
from sklearn import metrics
import numpy as np
import pandas as pd
import logging
import json
from .mlblock import MLBlock
import razor.flow as rf
import typing as typ

logger = logging.getLogger()

@rf.block
class Regressor(MLBlock):
    __publish__ = False
    __label__ = "Regressor"
    __description__ = "Regressor"
    __tags__ = ['ML models']
    __category__ = 'ML models'


    @abstractmethod
    def create_model(self, **kwargs):
        """
        Should create and return the model object
        """

    @abstractmethod
    def attribute_func(self, *args, **kwargs):
        """
        Should return the attribute values
        """

    @abstractmethod
    def operation_func(self, *args, **kwargs):
        """
        Should return the operation reuslts
        """

    def load_saved_model(self):
        t = self.get_save_path(path=self.path)
        t = os.path.join(t, 'model.sav')
        self.model = pickle.load(open(t, 'rb'))

    def save_model(self):
        t = self.create_save_path(path=self.path)
        t = os.path.join(t, 'model.sav')
        if os.path.exists(t):
            os.remove(t)
        pickle.dump(self.model, open(t, 'wb'))

    def run(self):
        try:
            if isinstance(self.load, str) and isinstance(self.save, str):
                load = json.loads(self.load)
                save = json.loads(self.save)
            else:
                load = self.load
                save = self.save
            if load==True and save==True:
                raise ValueError("Model cannot be loaded and saved simultaneously")
            if load:
                self.load_saved_model()
            else:
                self.model = self.create_model()
            if self.model is None:
                raise Exception(" model function should return a model. Got None")

            if self.x_data is not None and len(self.x_data) != 0:
                if isinstance(self.x_data, (np.ndarray, list)):
                    if self.x_data.shape[0] > 100000 or self.x_data.shape[1] > 100:
                        raise ValueError(f"Block limit exceeded for Input data rows (<=100000) or columns(<=100)")
                    if np.isnan(self.x_data).sum() > 0:
                        raise ValueError("x_data contains unhandled NaN values")
                else:
                    raise ValueError("x_data should be of type Array-like")
            if self.y_data is not None and len(self.y_data) != 0:
                if isinstance(self.y_data, (np.ndarray, list)):
                    if len(self.y_data) != len(self.x_data):
                        raise ValueError(
                            "The length of x_data is " + str(self.x_data.shape[0]) + " but length of y_data is " + str(
                                self.y_data.shape[0]) + ".")
                    if np.isnan(self.y_data).sum() > 0:
                        raise ValueError("y_data contains unhandled NaN values")
                else:
                    raise ValueError("y_data should be of type Array-like")
            pred = self.operation_func()
            self.predictions.put([pred] if isinstance(pred, (int, float, str)) else pred)

            if self.attribute is not None and len(self.attribute) != 0:
                attr_value = self.attribute_func()
                self.logger.info(f"{self.attribute}: {attr_value}")
                self.model_attributes.put([attr_value] if isinstance(attr_value, (int, float, str)) else attr_value)

            if all((v is not None) and (len(v) != 0) for v in [self.x_data, self.y_data]) or all(
                    (v is not None) and (len(v) != 0) for v in [self.test_x_data, self.test_y_data]) and self.operation != "evaluate":
                if self.test_x_data is None or len(self.test_x_data) == 0:
                    self.x_test, self.y_test = self.x_data, self.y_data
                else:
                    self.x_test, self.y_test = self.test_x_data, self.test_y_data
                self.evaluate()
            if save:
                self.save_model()
        except Exception as e:
            self.logger.error(f"Error in {self.__class__.__name__} Block", exc_info=True)
            self.logger.error(e, exc_info=True)
            
    def evaluate(self):
        if isinstance(self.x_test, (np.ndarray, list)) and isinstance(self.y_test, (np.ndarray, list)):
            if self.x_test.shape[0] > 100000 or self.x_test.shape[1] > 100:
                raise ValueError("Block limit exceeded for Test data rows (<=100000) or columns(<=100)")
            if len(self.y_test) != len(self.x_test):
                raise ValueError(
                    "The length of x_data is " + str(self.x_test.shape[0]) + " but length of y_data is " + str(
                        self.y_test.shape[0]) + ".")
            if np.isnan(self.x_test).sum() > 0 or np.isnan(self.y_test).sum() > 0:
                raise ValueError("Test data contains unhandled NaN values")

            metric_func = {"mean_squared_error": metrics.mean_squared_error,
                           "root_mean_squared_error": "root_mean_squared_error",
                           "mean_absolute_error": metrics.mean_absolute_error,
                           "r2_score": metrics.r2_score}
            metric_function = [self.metric_function] if not isinstance(self.metric_function, list) else self.metric_function
            final_metrics = []
            for _metric_function in metric_function:
                eval_metric = metric_func.get(_metric_function, None)
                if eval_metric is None:
                    raise Exception("Please check the evaluate function specified")
                if _metric_function == "root_mean_squared_error":
                    _metric_value = metrics.mean_squared_error(self.y_test, self.model.predict(self.x_test), squared=False)
                else:
                    _metric_value = eval_metric(self.y_test, self.model.predict(self.x_test))
                self.logger.info(f'{_metric_function}: {_metric_value}')
                final_metrics.append(_metric_value)
        else:
            raise ValueError("Test data should be of type Array-like")

        return (final_metrics if len(final_metrics) > 1 else final_metrics[0])
