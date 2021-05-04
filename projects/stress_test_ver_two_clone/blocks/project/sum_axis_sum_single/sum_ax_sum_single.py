import razor.flow as rf
import logging
import time
import typing as typ
import numpy as np
import pandas as pd

logger = logging.getLogger()


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class SumAxisSum:
    __publish__ = True
    __label__ = "SumAxisSum"
    
    in_ds: typ.Any
    out_ds: rf.Output[typ.Any]

    def run(self):        
        df = self.in_ds.copy()
        logger.info(df['x_temp'].sum())
        self.out_ds.put(df)