import razor.flow as rf
from razor import api

import time, os
import typing as typ
import pandas as pd
import os
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)
    
    
@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDIn:    
    __publish__ = True
    __label__ = "STDIn_socket"
    
    filename: str
    out_ds: rf.Output[typ.Any]

    def run(self):
        print("Reading data...")
        df = pd.read_parquet(project_space_path(self.filename))

        print("Reading Done...", df.shape)
        self.out_ds.put(df)
        print("Transferred...")


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDOut:    
    __publish__ = True
    __label__ = "STDOut_socket"
    
    in_ds: typ.Any
    out_filename: str

    def run(self):
        print("Writing Data...", self.in_ds.shape)
        self.in_ds.to_parquet(project_space_path(self.out_filename), index=False)
        print("Writing Done...")
            
        