import razor.flow as rf
from razor.api import datasources
import numpy as np
import logging
logger = logging.getLogger()

from razor import api

def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block
class IntType:
    __publish__ = True
    __label__ = "Int"
    
    a: int
        
    def run(self):
        logger.info(self.a)

