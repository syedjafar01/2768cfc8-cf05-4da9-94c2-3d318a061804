from razor import flow as rf

from ..utils.temp_data import TempStore
from .base_block import TF_BaseBlock

from typing import Union, List, Any


@rf.block(type_check=False)
class Tensorflow_Infer(TF_BaseBlock):
    __publish__ = True
    __label__ = "Tensorflow_Infer"
    __category__ = "TF Blocks"

    model: Union[Any, dict] = None
    batch_size: int
    data: rf.SeriesInput[dict]
    layers: Union[List[str], dict]
    data_id: bool = False

    output: rf.SeriesOutput[dict] = rf.Output(label='output')

    # SDK
    load_path: str = None

    def run(self):
        self.logger.info(self.layers)
        self.logger.info(self.model)
        from rztdl.backends import backend
        backend.tensorflow()
        self.temp_stores: List[TempStore] = []
        self.m_data = self.configure_test_temp_stores(self.data)
        try:
            self.standardize_params()

            for out in self.m_model.predict_generator(test_data=self.m_data,
                                                      layers=self.m_layers,
                                                      batch_size=self.batch_size,
                                                      use_id=self.data_id,
                                                      model_load_path=self.m_load_path):
                self.output.put(out)
        finally:
            for i in self.temp_stores:
                i.close()

    def standardize_sdk_params(self):
        if not self.load_path:
            raise ValueError("Missing mandatory param load_path")
        self.m_load_path = self.load_path
        self.m_model = self.model
        self.m_layers = self.layers

    def ui_load_model(self):
        mlc_entry = self._modify_params_after_load_from_mlc()
        self.m_details = mlc_entry['modelDetails']

    def standardize_ui_params(self):
        self.ui_load_model()
        self.m_layers = self.get_all_components(model_json=self.m_details["template"], layers=self.layers)
        self.logger.info(f"Layers selected: {self.m_layers}")
        if len(self.m_layers) == 0:
            raise ValueError("Please select the layers to infer model, Got None.")

    def standardize_params(self):
        if self.ui_mod:
            self.standardize_ui_params()
        else:
            self.standardize_sdk_params()
        if self.data is None:
            raise ValueError("Missing data. Please provide data")

    def get_all_components(self, model_json, layers):
        all_components = []
        for key, val in layers.items():

            key = key.split(",")
            if len(key) == 1:  # Normal component
                all_components.append(val)

            if len(key) == 2:  # Groups component
                group_id = key[0]
                group_name, group_instance_name, is_shared = self.get_group_name(group_id, model_json)
                if is_shared and group_instance_name:
                    all_components.append((group_instance_name, f"{group_name}_{val}"))
                else:
                    all_components.append(f"{group_name}_{val}")

            if len(key) == 3:  # Stack component
                stack_id = key[0]
                iteration = key[1]
                block_id = key[2]
                block_name = model_json["stacks"][stack_id]["stack_updates"][block_id][iteration]["name"]
                all_components.append(block_name)

            if len(key) == 4:  # stack component which is inside groups
                group_id = key[0]
                stack_id = key[1]
                iteration = key[2]
                block_id = key[3]
                group_name, group_instance_name, is_shared = self.get_group_name(group_id, model_json)
                block_name = model_json["stacks"][stack_id]["stack_updates"][block_id][iteration]["name"]
                if is_shared and group_instance_name:
                    all_components.append((group_instance_name, f"{group_name}_{block_name}"))
                else:
                    all_components.append(f"{group_name}_{block_name}")

        return all_components

    def get_group_name(self, current_group_id, model_json):
        group_ids = model_json["groups"].keys()
        for group_id in group_ids:
            if model_json["groups"][group_id]["sharedComponentDetail"]["shareDetail"] and current_group_id in \
                    model_json["groups"][group_id]["sharedComponentDetail"]["shareDetail"].keys():
                print()
                return model_json["groups"][group_id]["name"], \
                       model_json["groups"][group_id]["sharedComponentDetail"]["shareDetail"][current_group_id][
                           "name"], True
        return model_json["groups"][current_group_id]["name"], None, False
