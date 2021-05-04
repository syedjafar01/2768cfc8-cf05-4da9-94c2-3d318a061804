import razor.flow as rf
from razor import api
import numpy as np
import logging
logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block
class CustomValidator:
    __publish__ = True
    __label__ = "CustomValid"
    
    num: int

    @rf.validator('num', help='Input should a non-negative integer.')
    def validate_num(val):
        return val>=0
        
    def run(self):
        logger.info(self.num)