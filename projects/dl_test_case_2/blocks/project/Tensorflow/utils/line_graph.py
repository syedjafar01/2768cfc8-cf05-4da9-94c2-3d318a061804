import logging
import math
from typing import Union

import numpy as np
from rztdl.modules.callbacks.callback import Callback
from rztdl.backends.utils.string_constants import IntervalType, DataSplit

logger = logging.getLogger(__name__)


class LineGraphCallback(Callback):

    def __init__(self, split: Union[DataSplit, str], block: object,
                 interval_type: IntervalType = IntervalType.BATCH_END,
                 frequency: int = 1, custom_metric_key=""):
        """

        :param split: split
        :param interval_type: Interval Type : epoch/batch etc
        :param frequency: No of Intervals to trigger logging
        :param block
        """

        super().__init__(split=split, frequency=frequency,
                         interval_type=interval_type)
        self.custom_metric_key = custom_metric_key  # For K_fold
        self.metrics = {}
        self.optimizer_lr = {}
        self.block = block
        self.counter = 0
        self.prefix = self.set_prefix()

    def configure(self, *args, **kwargs):  # pragma: no cover
        """
        To be implemented
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def run(self, counter, metrics, loss, **kwargs):  # pragma: no cover
        from razor.api.core.metrics import LineGraph
        self.counter += 1
        metrics.update(loss)
        for metric_key, value in metrics.items():
            if isinstance(value, np.ndarray):
                continue
            if isinstance(value, list):
                continue
            if self.counter == 1:
                self.metrics[metric_key] = LineGraph(
                    name=f"{self.prefix}{metric_key} {self.custom_metric_key} : {self.split}",
                    y_label=[metric_key],
                    y_type=float,
                    keys=self.block._block_params, x_label='steps')
            if math.isnan(value):
                logger.warning(f"metric {metric_key} value is nan. Skipping metric posting")
                continue
            self.metrics[metric_key].post(x=self.counter, y=[float(value)])

        for optimizer_name, lr in kwargs.get('optimizer_lr', {}).items():
            if self.counter == 1:
                self.optimizer_lr[optimizer_name] = LineGraph(
                    name=f"{self.prefix}Learning Rate {self.custom_metric_key} : " + optimizer_name,
                    y_label=["Learning rate"],
                    y_type=float,
                    keys=self.block._block_params, x_label='steps')
            self.optimizer_lr[optimizer_name].post(x=self.counter, y=[float(lr)])

    def set_prefix(self):
        try:
            import horovod.tensorflow as hvd
            return "Worker " + str(hvd.rank()) + ": "
        except ImportError as e:
            return ""
