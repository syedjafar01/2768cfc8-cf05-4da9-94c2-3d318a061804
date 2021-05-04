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
