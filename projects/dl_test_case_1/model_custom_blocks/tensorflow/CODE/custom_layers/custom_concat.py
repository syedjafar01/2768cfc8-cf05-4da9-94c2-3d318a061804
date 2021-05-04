from rztdl.backends import backend
from rztdl.utils.annotations.types import Input, Output, ListInput

backend.set("tensorflow")

from rztdl.modules.layers.custom_layer import CustomLayer

import tensorflow as tf


class CustomConcat(CustomLayer):
    axis: int = -1

    input: ListInput
    output: Output

    def post_init(self):
        pass

    def build(self, input):
        self.operator = tf.keras.layers.Concatenate(name=self.name, axis=self.axis)

    def forward(self, input):
        _out = self.operator(input)
        return {"output": _out}

    def validate(self, input):

        if len(input) < 2:
            raise SizeError(entity_name=self.name,
                            message='Requires minimum of 2 inputs. Given inputs: {}'.format(len(input)))
        length_of_shape = len(input[0])
        for rzt_out in input[1:]:
            if len(rzt_out) != len(input[0]):
                raise DimensionError(component_name=self.name,
                                     message=f"Tensor Dimension doesn't match {rzt_out}, "
                                             f"{input[0]}")
            for i in range(length_of_shape):
                if i != abs(self.axis) and rzt_out[i] != input[0][i]:
                    raise DimensionError(component_name=self.name,
                                         message=f"A concat operator requires inputs with matching "
                                                 f"shapes except for the concat axis (given {self.axis}). "
                                                 f"Got inputs shapes: {rzt_out}, "
                                                 f"{input[0]}")

        if self.axis == 0:
            raise DimensionError(component_name=self.name,
                                 message=f" Concat Dimension can't be 0, since it is batch dimension")

        if abs(self.axis) >= length_of_shape:
            raise DimensionError(component_name=self.name,
                                 message=f"Concat Dimension parameter takes value between "
                                         f"-{length_of_shape} and {length_of_shape} excluding 0. Given {self.axis}")