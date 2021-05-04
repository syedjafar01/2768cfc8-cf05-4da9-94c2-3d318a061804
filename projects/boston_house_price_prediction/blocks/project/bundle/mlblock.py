__all__ = ['MLBlock']

import os
from abc import abstractmethod
from IPython.display import JSON
import razor.flow as rf
import typing as typ
from razor.api import mlc

@rf.block
class MLBlock:
    __publish__ = False
    __label__ = "MLBlock"
    __description__ = "MLBlock"
    __tags__ = ['ML models']
    __category__ = 'ML models'
    
    def init(self):
        self.use_mlc=True
        
    def mlc(self, arg):
        self.use_mlc = arg
        return self

    @abstractmethod
    def create_model(self, **kwargs):
        """
        Should create and return the model object
        """

    @abstractmethod
    def load_saved_model(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def save_model(self, **kwargs):
        raise NotImplementedError

    def _get_save_path(self, path, model_type="ML", model_name=None, block_type=None):
        if not self.use_mlc:
            return path
        if block_type is None:
            block_type = self.__class__.__name__
        if model_name is None:
            model_name = self.__class__.__name__
        mlc_instance = MLC()
        data = {
            "key": {
                "tag": path,
                'model_name': model_name,
                'block_type': block_type
            },
            "modelType": model_type,
        }
        save_paths = mlc_instance.output_save_path(data=data, environment="SDK")
        if len(save_paths) < 1:
            raise Exception(f"save_path {path} for {self.__class__.__name__} not found in MLC ")
        return get_jupyter_or_engine_path(save_paths[-1]['outputSavePath'])

    def get_all_saved_models(self, block_type=None, path=None, model_type='ML', model_name=None, environment='SDK',
                             pretty=True):
        """
        Gets all saved models from MLC based on inputs
        :param block_type: BlockType DL/ LinearRegression etc, Ignore
        :param path:
        :param model_type:
        :param model_name:
        :param environment:
        :param pretty:
        :return:
        """
        if model_type not in ['ML', 'DL']:
            raise Exception(f" modeltype should be either ML/DL got {model_type}")
        if environment not in ['SDK', 'Platform']:
            raise Exception(f" environment should be either SDK/Platform got {environment}")
        if block_type is None:
            block_type = self.__class__.__name__
        mlc_instance = MLC()
        key = {"block_type": block_type}
        if model_name:
            key['model_name'] = model_name
        if path:
            key['tag']: path
        data = {
            "key": key,
            "modelType": model_type,
        }
        saved_models = mlc_instance.run_details(data=data, environment=environment)
        return JSON(saved_models) if pretty else saved_models

    def get_save_path(self, path, model_name=None):
        """
        Gets existing save_path from MLC for the model. Used during inference/ persist_train
        :param path:
        :param model_name:
        :return:
        """
        return self._get_save_path(path=path, model_name=model_name)

    def create_save_path(self, path, model_name=None):
        """
        Creates a save_path in MLC for the current block and model
        :param path:
        :param model_name:
        :return:
        """
        path = self._create_save_path(path=path, model_name=model_name)
        os.makedirs(path, exist_ok=True)
        return path

    def _post_metrics_to_mlc(self, metrics, path, block_type=None, model_name=None, model_type="ML"):
        """
        :param metrics: A dictionary containing train and test metrics
        :param path: Path used to save model
        :param block_type: block information
        :param model_name: model name
        :param model_type: 'DL' or 'ML'
        :return:
        """
        if block_type is None:
            block_type = self.__class__.__name__
        if model_name is None:
            model_name = self.__class__.__name__
        data = {
            "metrics": metrics,
            "modelFilter": {
                "key": {
                    "tag": path,
                    'model_name': model_name,
                    'block_type': block_type
                },
                "modelType": model_type
            },
        }
        self.logger.info(f"Posting metrics to MLC {data}")
        return MLC().set_metrics(data=data, environment="SDK")

    def _create_save_path(self, path, block_type=None, model_name=None, model_type="ML"):
        if not self.use_mlc:
            return path
        if block_type is None:
            block_type = self.__class__.__name__
        if model_name is None:
            model_name = self.__class__.__name__
        data = {
            "key": {
                "tag": path,
                'model_name': model_name,
                'block_type': block_type
            },
            "modelType": model_type,
        }
        mlc_instance = MLC()
        output_save_path_res = mlc_instance.register_run(data=data, environment="SDK")
        return output_save_path_res