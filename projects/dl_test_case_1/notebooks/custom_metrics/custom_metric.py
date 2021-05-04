# -*- coding: utf-8 -*-
"""
| *@created on:* 21/07/20,
| *@author:* Umesh Kumar,
| *@version:* v3.0.0
|
| *Description:*
| 
| *Sphinx Documentation Status:* Complete
|
"""
from rztdl.backends import backend
from rztdl.utils.annotations.types import Input

backend.set("tensorflow")

from rztdl.modules.metrics.custom_metric import CustomMetric

import tensorflow as tf


class CustomAccuracy(CustomMetric):
    predictions: Input
    labels: Input

    def build(self, predictions, labels):
        self.metric = tf.keras.metrics.Accuracy(name=self.name)

    def forward(self, predictions, labels):
        return self.metric(y_pred=predictions, y_true=labels)

    def validate(self, predictions, labels):
        if list(predictions) != list(labels):
            raise ValueError("Tensor Dimensions doesn't match {}:{}".format(
                list(labels), list(predictions)))
