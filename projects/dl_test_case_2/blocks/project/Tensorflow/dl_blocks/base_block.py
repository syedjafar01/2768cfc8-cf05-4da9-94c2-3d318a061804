from ..utils.temp_data import TempStore

mlc_environ = 'Platform'


class TF_BaseBlock:

    def __init__(self):
        self.model = None
        self.m_model = None
        self._block_params = {}
        self.temp_stores = []
        self.logger = None
        self.m_callbacks = []
        self._ui_mod = False

        self.m_valid_frequency = None
        self.m_valid_interval = None

        self.m_optimizers = {}
        self.optimizers = {}
        self.m_learning_rate = []

        self.m_valid_data = None
        self.m_test_data = None
        self.m_test_data_gen = None
        self.valid_split = None

        self.mlc_request_data = {}

        self.m_load_path = None
        self.metrics = None
        self.m_metrics = None

        self.m_data = None

        self.m_layers = None
        self.m_details = None
        self.m_train_data = None
        self.train_data = None
        self.m_save_path = None

    @property
    def ui_mod(self):
        if not hasattr(self, "_ui_mod"):
            self._ui_mod = isinstance(self.model, dict)
        return self._ui_mod

    def configure_train_temp_stores(self, data):
        ts = TempStore(data=data)
        self.temp_stores.append(ts)
        return ts.data_write if data is not None else data

    def configure_test_temp_stores(self, data):
        ts = TempStore(data=data)
        self.temp_stores.append(ts)
        return ts.infer_data_write if data is not None else data

    def configure_mlc(self, model_name):
        key = {"pipelineID": self._block_params.get("pipelineID"),
               "pipelineRunID": self._block_params.get("pipelineRunId"),
               "blockID": self._block_params.get("blockId"),
               "version": self._block_params.get("version"),
               "pipelineName": self._block_params.get("pipelineName"),
               "blockName": self._block_params.get("blockName"),
               "pipelineRunName": self._block_params.get("pipelineRunName"),
               'blockRunId': self._block_params.get('blockRunId'),
               'projectTag': self._block_params.get("projectTag"),
               'modelName': model_name,
               'flowType': "Train"}
        self.mlc_request_data = {"key": key, "modelType": "DL"}

    def configure_optimizer_lr(self):
        self.logger.debug("Configuring LR ")
        from rztdl.backends.tensorflow.helpers.optimizer_schedules import ConstantLR, ExponentialDecay
        lr_config_map = {'ConstantLR': ConstantLR, "ExponentialDecay": ExponentialDecay}
        self.m_optimizers = []
        self.m_learning_rate = []
        for optimizer_name, lr_config in self.optimizers.items():
            self.m_optimizers.append(optimizer_name)
            lr_config = lr_config['learning_rate']
            lr_config_class = lr_config_map[lr_config['internal_name']]
            self.m_learning_rate.append(lr_config_class(**lr_config['properties']))

    def _modify_params_after_load_from_mlc(self):
        from razor.api import mlc
        request_data = {"key": self.model['key'], "modelType": "DL"}
        self.logger.debug("Getting entry from MLC")
        run_details = mlc.get_run_details(data=request_data, environment=mlc_environ)
        if len(run_details) == 0:
            self.logger.debug("No entries found in MLC")
            raise ValueError("No entries found in MLC")
        if len(run_details) > 1:
            self.logger.debug("Multiple entries in MLC with same key")
            raise ValueError("Multiple entries in MLC with same key")
        mlc_entry = run_details[-1]
        from model_designer_parser import ModelParser
        _, model = ModelParser().generate_model_code(model_flow_json=mlc_entry['modelDetails'])
        self.m_model = model
        self.m_model.name = mlc_entry['key']['modelName']
        self.m_load_path = mlc_entry['outputSavePath']
        return mlc_entry

    def configure_line_graph_callback(self):
        from ..utils.line_graph import LineGraphCallback
        from rztdl.backends.utils.string_constants import DataSplit
        self.m_callbacks.append(LineGraphCallback(split=DataSplit.TRAIN, block=self))
        if self.m_valid_data:
            self.m_callbacks.append(LineGraphCallback(split=DataSplit.VALID, block=self))
        if self.m_test_data_gen:
            self.m_callbacks.append(LineGraphCallback(split=DataSplit.TEST, block=self))

    def configure_callbacks(self):
        from rztdl.backends.tensorflow.callbacks.early_stopping import EarlyStopping
        from rztdl.backends.tensorflow.callbacks.reduce_lr_on_plateau import ReduceLROnPlateau
        from rztdl.backends.tensorflow.callbacks.checkpoint_callback import CheckPointCallback
        from rztdl.backends.utils.string_constants import DataSplit, IntervalType, Mode
        enum_type_config = {i.name: i for i in DataSplit}
        enum_type_config.update({i.name: i for i in IntervalType})
        enum_type_config.update({i.name: i for i in Mode})
        callbacks_dict = {"EarlyStopping": EarlyStopping, "ReduceLROnPlateau": ReduceLROnPlateau,
                          "CheckPointCallback": CheckPointCallback}
        for callback_name, callback_config in self.callbacks.items():
            if callback_config["internal_name"] and len(callback_config["properties"]) != 0:
                callback = callbacks_dict.get(callback_config["internal_name"], None)
                if callback is None:
                    raise ValueError(f"{callback_config['internal_name']} callback is not supported.")
                param_config = {
                    property_name: (
                        property_value["internal_name"] if isinstance(property_value, dict) else property_value)
                    for property_name, property_value in callback_config["properties"].items()}
                if "split" in param_config.keys():
                    param_config["split"] = enum_type_config[param_config["split"]]
                if "interval_type" in param_config.keys():
                    param_config["interval_type"] = enum_type_config[param_config["interval_type"]]
                if "mode" in param_config.keys():
                    param_config["mode"] = enum_type_config[param_config["mode"]]

                # TODO: Remove below 2 conditions when UI supports this
                if "restore_best_weights" in param_config.keys() and param_config["restore_best_weights"] is None:
                    param_config["restore_best_weights"] = False
                if "verbose" in param_config.keys() and param_config["verbose"] is None:
                    param_config["verbose"] = False

                self.m_callbacks.append(callback(**param_config))
            # else:
            #     raise ValueError(
            #         f"No callback selected for callback {callback_name}. Please select required callback from the dropdown.")

    def configure_valid_interval(self):
        self.m_valid_interval = self.valid_split['interval_type']['internal_name'].lower()
        self.m_valid_frequency = self.valid_split['frequency']
