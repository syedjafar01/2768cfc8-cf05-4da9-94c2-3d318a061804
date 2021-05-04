import razor.flow as rf
from razor import api
import logging
import time
import typing as typ
import numpy as np
import pandas as pd

logger = logging.getLogger()


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class PrintReplaceFreq:
    __publish__ = True
    __label__ = "PrintReplaceFreq"
    
    in_ds: rf.SeriesInput[typ.Any]
    out_ds: rf.SeriesOutput[typ.Any]

    def run(self):
        for i in self.in_ds:
            logger.info(i.count('!!!R!!!'))
            self.out_ds.put(i)
            
            