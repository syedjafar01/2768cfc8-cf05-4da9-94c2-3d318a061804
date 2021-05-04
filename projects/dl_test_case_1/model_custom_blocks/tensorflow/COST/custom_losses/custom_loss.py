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
from rztdl.utils.annotations.types import Input, Output

backend.set("tensorflow")

from rztdl.modules.losses.custom_loss import CustomLoss

import tensorflow as tf


class CustomMSELoss(CustomLoss):
    predictions: Input
    labels: Input
    output: Output

    def build(self, predictions, labels):
        self.loss = tf.keras.losses.MeanSquaredError()

    def forward(self, predictions, labels, ):
        return self.loss(y_pred=predictions, y_true=labels)

    def validate(self, predictions, labels):
        if list(predictions) != list(labels):
            raise ValueError("Tensor Dimensions doesn't match {}:{}".format(
                list(labels), list(predictions)))
