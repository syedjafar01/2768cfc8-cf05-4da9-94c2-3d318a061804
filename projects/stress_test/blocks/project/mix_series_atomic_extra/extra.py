import razor.flow as rf
import logging
import time
import typing as typ
import numpy as np
import pandas as pd
from razor import api

logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block
class STDIn:
    __publish__ = True
    __label__ = "STDIn"
    
    s_file: str
    l_file: str
    s_val: rf.SeriesOutput[int] = rf.Output(transport=rf.KafkaTransport)
    l_val: rf.SeriesOutput[int] = rf.Output(transport=rf.KafkaTransport)
    
    def run(self):
        sftp_client = datasources('test_source').client()
        with sftp_client.open(self.s_file, 'r') as f:
            l = f.read().decode('utf-8')
            
        with sftp_client.open(self.l_file, 'r') as f:
            m = f.read().decode('utf-8')
            
        for i in l.split(', '):
            self.s_val.put(int(i))
            
        for i in m.split(', '):
            self.l_val.put(int(i))
            
            
@rf.block
class STDOut:
    __publish__ = True
    __label__ = "STDOut"
    
    in_array: np.ndarray
    out_file: str
    
    def run(self):
        logger.info(sum(self.in_array))
        text_str = ', '.join([str(i) for i in self.in_array])
        with open(project_space_path(self.out_file), 'w') as f:
            f.write(text_str)
            
