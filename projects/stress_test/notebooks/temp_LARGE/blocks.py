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
    a: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    b: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    c: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    d: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    e: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    f: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    g: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)        
    h: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    i: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    arr1: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    arr2: rf.Output[t.Any] = rf.Output(transport=rf.FileTransport)
    
    
    def run(self):
        with open(project_space_path(self.filename), 'r') as fil:
            lines = fil.readlines()
            a1 = lines[-1]
            a2 = lines[-2]
            lines = lines[:-2]
        
        a1 = a1.strip('[]').strip('\n').split(',')
        temp_list=[]
        
        self.logger.info("Length of array:")
        self.logger.info(len(a1))
        cnt=0
        for each in a1:
            cnt+=1
            temp = int(each)
#             self.logger.info("temp after")
#             self.logger.info(temp)
#             self.logger.info(type(temp))
            temp_list.append(temp)
            
        a1=deepcopy(temp_list)
#         a1 = [int(each) for each in a1]
        a2 = a2.strip('[]').strip('\n').split(',')
        a2 = [int(each) for each in a2]
        
        self.a.put(int(lines[0]))
        self.b.put(int(lines[1]))
        self.c.put(int(lines[2]))
        self.d.put(int(lines[3]))
        self.e.put(int(lines[4]))
        self.f.put(int(lines[5]))
        self.g.put(int(lines[6]))
        self.h.put(int(lines[7]))
        self.i.put(int(lines[8]))
        self.arr1.put(a1)
        self.arr2.put(a2)
        