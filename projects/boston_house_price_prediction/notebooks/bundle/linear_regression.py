import numpy as np
from .regressor import Regressor
from sklearn.linear_model import LinearRegression as lr_regression
import typing as typ
import razor.flow as rf

@rf.block
class LinearRegression(Regressor):
    __publish__ = True
    __label__ = "LinearRegression"
    __description__ = "LinearRegression"
    __tags__ = ['ML models']
    __category__ = 'ML models'
    """
   Linear Regressor from sklearn
    """
    fit_intercept: bool = rf.Input(True, label="fit_intercept")
    normalize: bool = rf.Input(False, label="normalize")
    copy_X: bool = rf.Input(True, label="copy_X")
    n_jobs: dict = rf.Input({"n_jobs":"[None]"}, label="n_jobs") ############ Need to change (int)
    operation: str = rf.Input("model_instance", label="operation", help="fit, predict, score, evaluate, model_instance, get_params, set_params")
    attribute: str = rf.Input("", label="attribute", help="coef_, rank_, singular_, intercept_")
    x_data: typ.Any = rf.Input({}, label="x_data")
    y_data: typ.Any = rf.Input({}, label="y_data")
    test_x_data: typ.Any = rf.Input({}, label="test_x_data")
    test_y_data: typ.Any = rf.Input({}, label="test_y_data")  
    metric_function: list = rf.Input(["r2_score"], label="metric_function")
    path: str = rf.Input("", label="path")
    load: bool = rf.Input(False, label="load")
    save: bool = rf.Input(False, label="save")
    params: dict = rf.Input({}, label="params")
    predictions: rf.Output[typ.Any]
    model_attributes: rf.Output[typ.Any]

    def create_model(self):
        n_jobs = eval(self.n_jobs["n_jobs"])[0]
        return lr_regression(fit_intercept=self.fit_intercept, normalize=self.normalize, copy_X=self.copy_X, n_jobs=n_jobs)

    def operation_func(self):
        func = {"fit": self.model.fit, "predict": self.model.predict,
                "score": self.model.score, "evaluate": self.evaluate, "model_instance": self.model,
                "get_params": self.model.get_params, "set_params": self.model.set_params}
        method = func.get(self.operation, None)
        if method is None:
            raise Exception("Please check the Operation function specified")
        if self.operation in ["fit", "score"]:
            print(self.x_data.shape, self.y_data.shape)
            pred = method(self.x_data, self.y_data)
        elif self.operation in ["predict"]:
            if self.x_data is None or len(self.x_data) == 0:
                x_data = self.test_x_data
            else:
                x_data = self.x_data
            pred = method(x_data)
        elif self.operation in ["evaluate"]:
            if self.test_x_data is None or len(self.test_x_data) == 0:
                self.x_test, self.y_test = self.x_data, self.y_data
            else:
                self.x_test, self.y_test = self.test_x_data, self.test_y_data
            pred = self.evaluate()
        elif self.operation == 'get_params':
            pred = method()
        elif self.operation == 'set_params':
            pred = method(**self.params)
        else:
            pred = method
        return pred

    def attribute_func(self):
        k = self.model
        attr = {"coef_": k.coef_, "rank_": k.rank_, "singular_": k.singular_, "intercept_": k.intercept_}
        attr_value = attr.get(self.attribute, None)
        if attr_value is None:
            raise Exception("Please check the Attribute function specified")
        return attr_value
