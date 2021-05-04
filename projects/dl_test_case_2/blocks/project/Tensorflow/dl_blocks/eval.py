from razor import flow as rf

from ..utils.temp_data import TempStore

from .base_block import TF_BaseBlock

from typing import Union, List, Any


@rf.block(type_check=False)
class Tensorflow_Eval(TF_BaseBlock):
    __publish__ = True
    __label__ = "Tensorflow_Eval"
    __category__ = "TF Blocks"

    model: Union[Any, dict] = None
    batch_size: int
    data: rf.SeriesInput[dict]
    metrics: Union[List[str], dict]

    output: rf.Output[dict]

    # SDK
    load_path: str = None

    def run(self):
        self.logger.info(self.metrics)
        self.logger.info(self.model)
        from rztdl.backends import backend
        backend.tensorflow()
        self.temp_stores: List[TempStore] = []
        self.m_data = self.configure_test_temp_stores(self.data)
        try:
            self.standardize_params()

            output = self.m_model.evaluate_generator(test_data=self.m_data,
                                                     metrics=self.m_metrics,
                                                     batch_size=self.batch_size,
                                                     model_load_path=self.m_load_path)
            self.logger.info(f"Metric Output : {output}")
            self.output.put(output)
        finally:
            for i in self.temp_stores:
                i.close()

    def standardize_sdk_params(self):
        if not self.load_path:
            raise ValueError("Missing mandatory param load_path")
        self.m_load_path = self.load_path
        self.m_model = self.model
        self.m_metrics = self.metrics

    def standardize_ui_params(self):
        self._modify_params_after_load_from_mlc()
        self.m_metrics = list(self.metrics.values())

    def standardize_params(self):
        if self.ui_mod:
            self.standardize_ui_params()
        else:
            self.standardize_sdk_params()
        if self.data is None:
            raise ValueError("Missing data. Please provide data")
