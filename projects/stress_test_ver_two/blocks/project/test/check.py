import razor.flow as rf
from razor import api

import time
import typing as typ
import pandas as pd
import os


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)
    
    
@rf.block
class STDIn:    
    __publish__ = True
    __label__ = "STDIn"
    
    filename: str
    out_ds: rf.Output[typ.Any]
#     out_ds: rf.Output[typ.Any] = rf.Output(transport=rf.KafkaTransport)

    def run(self):
        df = pd.read_parquet(project_space_path(self.filename))

        print(df.shape)
        self.out_ds.put(df)


@rf.block
class STDOut:    
    __publish__ = True
    __label__ = "STDOut"
    
    in_ds: typ.Any
    out_filename: str

    def run(self):
        print(self.in_ds.shape)
#         sftp_c = datasources('test_source').client()
        self.in_ds.to_parquet(project_space_path(self.out_filename), index=False)
            
        