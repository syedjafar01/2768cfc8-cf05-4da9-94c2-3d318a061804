import os
import razor.flow as rf
from razor.api import datasources
import numpy as np
import logging
logger = logging.getLogger()

from razor import api

def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class StringType:
    __publish__ = True
    __label__ = "String"
    
    a: str
        
    def run(self):
        logger.info(self.a)

