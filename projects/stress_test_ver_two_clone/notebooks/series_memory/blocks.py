import razor.flow as rf
import logging
import time, os
import typing as typ
import numpy as np
import pandas as pd
from razor import api

logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.FunctionExecutor)
class STDIn_series:
    __publish__ = True
    __label__ = "STDIn_series"
    
    filename: str
    records: int
    out_ds: rf.SeriesOutput[typ.Any]

    def run(self):
        with open(project_space_path(self.filename), 'r') as f:
            text_str = f.read()

        logger.info(len(text_str))
        for _ in range(self.records):
            self.out_ds.put(text_str)


@rf.block(executor=rf.FunctionExecutor)
class STDOut_series:
    __publish__ = True
    __label__ = "STDOut_series"
    
    in_ds: rf.SeriesInput[typ.Any]
    out_filename: str

    def run(self):
        text_str = [str(i) for i in self.in_ds][0]
        logger.info(len(text_str))
        with open(project_space_path(self.out_filename), 'w') as f:
            f.write(text_str)
            
            