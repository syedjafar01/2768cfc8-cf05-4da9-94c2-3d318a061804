import numpy as np
import logging
import typing as t
from copy import deepcopy
import razor.flow as rf
from razor import api

logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block
class STDIn:
    __publish__ = True
    __label__ = "STDIn"
    
    filename: str
    a: rf.Output[t.Any]
    b: rf.Output[t.Any]
    c: rf.Output[t.Any]
    d: rf.Output[t.Any]
    e: rf.Output[t.Any]
        
    def run(self):
        with open(project_space_path(self.filename), 'r') as f:
            lines = f.readlines()
        
        self.a.put(int(lines[0]))
        self.b.put(int(lines[1]))
        self.c.put(int(lines[2]))
        self.d.put(int(lines[3]))
        self.e.put(int(lines[4]))

    
@rf.block
class Multiply3:
    __publish__ = True
    __label__ = "Multiply3"
    
    input_sum: t.Any
    input_multiplier: t.Any
    out_file: str
        
    def run(self):
        self.logger.info('Running Multiply block')
        self.logger.info(f'input_sum :{self.input_sum}')
        self.logger.info(f'input_multiplier :{self.input_multiplier}')
        result = self.input_sum * self.input_multiplier
        self.logger.info(f'result :{result}')
        print("$$", result)
        sftp_client = datasources("system_test_files").client()
        with sftp_client.open(self.out_file, 'w') as file:
            file.write(str(result))
    
    
@rf.block
class Branch2_Add2:
    __publish__ = True
    __label__ = "Branch2_Add2"
    
    arr1: t.Any
    arr2: t.Any
    out_file: str
        
    def run(self):
        self.logger.info('Running Multiply block')
        res = [self.arr1[i]*self.arr2[i] for i in range(len(self.arr1))]
        result = 0
        for val in res:
            result+=val
        self.logger.info(f'result :{result}')
        print("**", result)
        sftp_client = datasources("system_test_files").client()
        with sftp_client.open(self.out_file, 'a') as file:
            file.write('\n')
            file.write(str(result))
            
        
@rf.block
class Multiply2:
    __publish__ = True
    __label__ = "Multiply2"
    
    input_sum: t.Any
    input_multiplier: t.Any
    out_file_name: str
    
    def run(self):
        self.logger.info('Running Multiply block')
        self.logger.info(f'input_sum :{self.input_sum}')
        self.logger.info(f'input_multiplier :{self.input_multiplier}')
        self.result=self.input_sum * self.input_multiplier
        self.logger.info(f'result :{self.result}')
        print("$$", self.result)
        with open(project_space_path(self.out_file_name), 'w') as file:
            file.write(str(self.result))
        
        
@rf.block
class Branch3_Add1:
    __publish__ = True
    __label__ = "Branch3_Add1"
    
    arr1: t.Any
    arr2: t.Any
    out_file: str
        
    def run(self):
        self.logger.info('Running Add block')
        result = 0
        for i in self.arr1:
            result+=i
        for j in self.arr2:
            result+=j
            
        self.logger.info(f'result :{result}')
        with open(project_space_path(self.out_file), 'a') as file:
            file.write('\n')
            file.write(str(result))
        