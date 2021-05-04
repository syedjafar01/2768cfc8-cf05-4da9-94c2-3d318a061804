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
class CreateData:
    __publish__ = True
    __label__ = "CreateData"
    
    filename: str
    size: int
    out_filename: rf.Output[str]
    
    def run(self):
        arr = list(np.random.randint(1, 11, size=self.size))
        text_str = ', '.join([str(i) for i in arr])
        
        logger.info(sum(arr))
        
        with open(project_space_path(self.filename), 'w') as f:
            f.write(text_str)
        self.out_filename.put(self.filename)
        
        
@rf.block
class ArrayMul:
    __publish__ = True
    __label__ = "ArrayMul"
    
    array: rf.SeriesInput[int]
    array_out: rf.SeriesOutput[int] = rf.Output(transport=rf.KafkaTransport)
    
    def run(self):
        l = 2 * np.array([i for i in self.array])
        for i in l:
            self.array_out.put(int(i))
        
        
@rf.block
class ArrayAdd:
    __publish__ = True
    __label__ = "ArrayAdd"
    
    s_array: rf.SeriesInput[int]
    l_array: rf.SeriesInput[int]
    s_array_out: rf.SeriesOutput[int]
    l_array_out: rf.SeriesOutput[int]
    
    def run(self):
        for i in self.s_array:
            self.s_array_out.put(int(i+1))
        for j in self.l_array:
            self.l_array_out.put(int(j+2))
        
        
@rf.block
class ArrayDeOp:
    __publish__ = True
    __label__ = "ArrayDeOp"
    
    array: rf.SeriesInput[int]
    k: int
    out_array: rf.Output[np.ndarray]
    
    def run(self):
        l = np.array([i for i in self.array])
        self.out_array.put((l-self.k)//2)
        

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
            
            