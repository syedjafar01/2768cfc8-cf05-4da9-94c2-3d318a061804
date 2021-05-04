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


@rf.block
class STDIn:
    __publish__ = True
    __label__ = "STDIn"
    
    s_file: str
    l_file: str
    s_val: rf.SeriesOutput[int]
    l_val: rf.SeriesOutput[int]
    
    def run(self):
        with open(project_space_path(self.s_file), 'r') as f:
            l = f.read()
            
        with open(project_space_path(self.l_file), 'r') as f:
            m = f.read()
            
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
            
