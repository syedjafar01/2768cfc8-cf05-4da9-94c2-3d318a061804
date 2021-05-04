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


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class AxisSum:
    __publish__ = True
    __label__ = "AxisSum"
    
    in_ds: typ.Any
    out_ds: rf.Output[typ.Any]

    def run(self):
        df = self.in_ds.copy()
        df['x_temp'] = df.sum(axis=1)
        self.out_ds.put(df)

        
@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class SumAxisSum:
    __publish__ = True
    __label__ = "SumAxisSum"
    
    s_in_ds: typ.Any
    l_in_ds: typ.Any
    s_out_ds: rf.Output[typ.Any]
    l_out_ds: rf.Output[typ.Any]
        
    def run(self):
        s_df = self.s_in_ds.copy()
        logger.info('small df')
        logger.info(s_df['x_temp'].sum())
        
        self.s_out_ds.put(s_df)
        del(s_df)
        
        l_df = self.l_in_ds.copy()
        logger.info('large df')
        logger.info(l_df['x_temp'].sum())
        
        self.l_out_ds.put(l_df)
        
        
@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class DropAxisSum:
    __publish__ = True
    __label__ = "DropAxisSum"
    
    in_ds: typ.Any
    out_ds: rf.Output[typ.Any]

    def run(self):
        df = self.in_ds.copy()
        df.drop(['x_temp'], axis=1, inplace=True)
        self.out_ds.put(df)
        
        