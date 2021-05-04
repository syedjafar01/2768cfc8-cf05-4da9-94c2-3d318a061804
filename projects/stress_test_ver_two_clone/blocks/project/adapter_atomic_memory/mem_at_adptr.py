import razor.flow as rf
from razor import api
import logging
import os, time
import typing as typ
import numpy as np
import pandas as pd

logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDIn:
    __publish__ = True
    __label__ = "STDIn"
    
    filename: str
    out_ds: rf.Output[typ.Any]

    def run(self):
        df = pd.read_parquet(project_space_path(self.filename))

        df['x_temp'] = np.nan
        logger.info(df.head())
        self.out_ds.put(df)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDOut:
    __publish__ = True
    __label__ = "STDOut"
    
    in_ds: typ.Any
    out_filename: str

    def run(self):
        logger.info(self.in_ds.shape)
        self.in_ds.to_parquet(project_space_path(self.out_filename), index=False)
        