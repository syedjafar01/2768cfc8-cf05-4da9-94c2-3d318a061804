from rztdl.backends import backend
from rztdl.utils.annotations.types import Input, Output

backend.set("tensorflow")

from rztdl.modules.layers.custom_layer import CustomLayer

import tensorflow as tf


class CustomDense(CustomLayer):
    units: int

    input: Input
    output: Output

    def post_init(self):
        pass

    def build(self, input):
        self.layer = tf.keras.layers.Dense(name=self.name, units=self.units)

    def forward(self, input):
        return {"output": self.layer(input)}

    def validate(self, input):
        if not len(input) == 2:
            raise ValueError(f'Dense takes 2 dimensional input. Given {len(input)}')
