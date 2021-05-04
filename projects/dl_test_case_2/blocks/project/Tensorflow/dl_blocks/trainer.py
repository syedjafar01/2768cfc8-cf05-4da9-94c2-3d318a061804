import os
from razor import flow as rf

import json

from ..utils.temp_data import TempStore
from .base_block import TF_BaseBlock

from typing import Union, List, Optional, Any

from razor.api import mlc

mlc_environ = 'Platform'


@rf.block(type_check=False)
class Tensorflow_Train(TF_BaseBlock):
    __publish__ = True
    __label__ = "Tensorflow_Train"
    __category__ = "TF Blocks"

    model: Any
    epochs: int
    batch_size: int
    callbacks: Union[List[str], str]
    optimizers: Union[List[str], str]
    train_data: rf.SeriesInput[dict]
    metrics: Union[List[str], dict]

    test_data: rf.SeriesInput[dict] = None
    valid_data: rf.SeriesInput[dict] = None

    # Only in Jupyter
    learning_rate: Union[float, List[float]] = [0.1]
    save_path: Optional[str] = None
    valid_interval: str = "epoch"
    valid_frequency: int = 1

    saved_path: rf.Output[str] = rf.Output(label='saved_path')

    # UI
    valid_split: dict = None

    def standardize_sdk_params(self):
        if not isinstance(self.optimizers, list):
            self.m_optimizers = [self.optimizers]
        self.m_save_path = self.save_path

        self.m_valid_interval = self.valid_interval.lower()
        self.m_valid_frequency = self.valid_frequency
        self.m_model = self.model
        self.m_learning_rate = self.learning_rate
        self.m_metrics = self.metrics

    def standardize_ui_params(self):
        self.configure_line_graph_callback()
        # if len(self.callbacks) != 0:
        self.configure_callbacks()
        self.logger.info(f"Json path {self.model['jsonPath']}")
        new_path = '/runner-space' + self.model['jsonPath']
        self.logger.info(new_path)
        model_name = self.model['name']

        self.configure_mlc(model_name=model_name)
        register_run_req = {'runStatus': "IN_PROGRESS"}
        register_run_req.update(self.mlc_request_data)
        self.m_save_path = mlc.register_run(data=register_run_req, environment=mlc_environ)
        self.m_model = self.get_model_from_path(path=new_path)
        self.logger.info(self.m_model)

        self.configure_optimizer_lr()
        self.configure_valid_interval()
        self.m_metrics = list(self.metrics.values())
        hyper_params = {"epochs": self.epochs, "batch_size": self.batch_size}
        mlc.set_hyper_params(data={'modelFilter': self.mlc_request_data}, environment=mlc_environ,
                             hyper_params=hyper_params)

    def standardize_params(self):
        if self.train_data is None:
            raise ValueError("Missing Train data. Please provide Train data")
        self.m_train_data = self.configure_train_temp_stores(self.train_data)
        self.m_valid_data = self.configure_train_temp_stores(self.valid_data)
        self.m_test_data_gen = self.configure_test_temp_stores(self.test_data)

        if self.ui_mod:
            self.logger.info("UI Mode")
            self.standardize_ui_params()
        else:
            self.logger.info("SDK Mode")
            self.standardize_sdk_params()

    def run(self):
        from rztdl.backends import backend
        backend.tensorflow()
        self.temp_stores: List[TempStore] = []
        self.m_callbacks = []
        self.logger.info("Standardizing params")
        try:
            self.standardize_params()
            self.logger.info("Standardizing params completed")
            self.fit()
        finally:
            for i in self.temp_stores:
                i.close()

    def fit(self):
        self.logger.info(self.m_save_path)
        from razor.api.context import UserContext
        self.logger.info(UserContext.project_id)
        self.m_model.fit_generator(train_data=self.m_train_data,
                                   valid_data=self.m_valid_data,
                                   test_data=self.m_test_data_gen,
                                   epochs=self.epochs, batch_size=self.batch_size,
                                   optimizers=self.m_optimizers,
                                   lr=self.m_learning_rate,
                                   metrics=self.m_metrics,
                                   valid_interval_type=self.m_valid_interval,
                                   valid_frequency=self.m_valid_frequency,
                                   model_save_path=self.m_save_path, callbacks=self.m_callbacks)
        self.logger.info("Checkpoint path")
        self.logger.info(self.m_save_path)
        self.saved_path.put(self.m_save_path)

    def get_model_from_path(self, path):
        if not os.path.exists(path=path):
            self.logger.info("path doesn't exist")
            raise FileNotFoundError(f"{path} is not Found. Cannot read model json")
        input_json = json.load(open(path, "r"))
        template = json.loads(input_json["template"])
        input_json["template"] = template
        template_req_data = {
            'modelFilter': self.mlc_request_data
        }
        mlc.set_model_template(data=template_req_data, environment=mlc_environ, template=input_json)
        from model_designer_parser import ModelParser
        code, model = ModelParser().generate_model_code(model_flow_json=input_json)
        return model
