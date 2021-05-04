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
class ReplaceChar:
    __publish__ = True
    __label__ = "ReplaceChar"
    
    in_ds: rf.SeriesInput[typ.Any]
    out_ds: rf.SeriesOutput[typ.Any]

    def run(self):
        for i in self.in_ds:
            text_str = i.replace('r', '!!!R!!!')
            self.out_ds.put(text_str)
            
            
@rf.block       
class PrintReplaceFreq:
    __publish__ = True
    __label__ = "PrintReplaceFreq"
    
    s_in_ds: rf.SeriesInput[typ.Any]
    l_in_ds: rf.SeriesInput[typ.Any]
    s_out_ds: rf.SeriesOutput[typ.Any]
    l_out_ds: rf.SeriesOutput[typ.Any]

    def run(self):
        logger.info('small')
        for i in self.s_in_ds:
            logger.info(i.count('!!!R!!!'))
            self.s_out_ds.put(i)
            
        logger.info('big')
        for i in self.l_in_ds:
            logger.info(i.count('!!!R!!!'))
            self.l_out_ds.put(i)
            
            
@rf.block
class DeReplaceChar:
    __publish__ = True
    __label__ = "DeReplaceChar"
    
    in_ds: rf.SeriesInput[typ.Any]
    out_ds: rf.SeriesOutput[typ.Any]

    def run(self):
        for i in self.in_ds:
            text_str = i.replace('!!!R!!!', 'r')
            self.out_ds.put(text_str)
