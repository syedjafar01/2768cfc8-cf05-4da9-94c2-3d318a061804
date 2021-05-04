import razor.flow as rf


@rf.block()
class DataGen:
    __publish__ = True
    __label__ = "RandomDataGen"
    __category__ = "TF Blocks"


    num_records: int = 10
    column_shapes: dict = {"InputLayer__1":[4], "InputLayer__2":[1]}
    output: rf.SeriesOutput[dict]

    def run(self):
        import numpy as np
        from razor import api
        ps = api.datasources('Project Space')
        self.logger.info(ps.abspath())
        self.logger.info(ps.ls())
        for i in range(self.num_records):
            self.output.put({col_name: np.random.random(shape) for col_name, shape in self.column_shapes.items()})
