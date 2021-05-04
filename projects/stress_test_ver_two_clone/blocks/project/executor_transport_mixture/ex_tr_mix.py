import os
import numpy as np
import logging
import razor.flow as rf
import typing as t

from razor import api

def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDIn_2:
    __publish__ = True
    __label__ = "STDIn_2"
    
    filename: t.Any
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
        
        
@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class Add1:
    __publish__ = True
    __label__ = "Add1"
    
    input_1: t.Any
    input_2: t.Any
    sum_output: rf.Output[t.Any]
        
    def run(self):
        self.logger.info('Running Add block')
        self.logger.info(f'input_1 :{self.input_1}')
        self.logger.info(f'input_2 :{self.input_2}')
        self.sum_output.put((self.input_1 + self.input_2))
        

@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class Add2:
    __publish__ = True
    __label__ = "Add2"
    
    input_1: t.Any
    input_2: t.Any
    sum_output: rf.Output[t.Any]
    
    def run(self):
        self.logger.info('Running Add block')
        self.logger.info(f'input_1 :{self.input_1}')
        self.logger.info(f'input_2 :{self.input_2}')
        self.sum_output.put((self.input_1 + self.input_2))
        

@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class Multiply1:
    __publish__ = True
    __label__ = "Multiply1"
    
    input_sum: t.Any
    input_multiplier: t.Any
    output_mul: rf.Output[t.Any]
    
    def run(self):
        self.logger.info('Running Multiply block')
        self.logger.info(f'input_sum :{self.input_sum}')
        self.logger.info(f'input_multiplier :{self.input_multiplier}')
        self.output_mul.put((self.input_sum * self.input_multiplier))
        
        
@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class Multiply2:
    __publish__ = True
    __label__ = "Multiply2"
    
    input_sum: t.Any
    input_multiplier: t.Any
    out_file_name: t.Any
    
    def run(self):
        self.logger.info('Running Multiply block')
        self.logger.info(f'input_sum :{self.input_sum}')
        self.logger.info(f'input_multiplier :{self.input_multiplier}')
        result=self.input_sum * self.input_multiplier
#         self.logger.info(f'result :{result}')
        print("$$", result)
        with open(project_space_path(self.out_file_name), 'w') as file:
            file.write(str(result))
        
        