import razor.flow as rf
from razor import api
import logging
import time, os
import typing as typ
import numpy as np
import pandas as pd

logger = logging.getLogger()


def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDIn:
    __publish__ = True
    __label__ = "STDIn_series"
    
    filename: str
    records: int
    out_ds: rf.SeriesOutput[typ.Any] = rf.Output(transport=rf.KafkaTransport)

    def run(self):
        with open(project_space_path(self.filename), 'r') as f:
            text_str = f.read()

        text_str = text_str.replace('r', '!!!R!!!')
        logger.info(len(text_str))
        for _ in range(self.records):
            self.out_ds.put(text_str)


@rf.block(executor=rf.ContainerExecutor(cores=1, memory=5000))
class STDOut:
    __publish__ = True
    __label__ = "STDOut_series"
    
    in_ds: rf.SeriesInput[typ.Any]
    out_filename: str

    def run(self):
        text_str = ''
        for i in self.in_ds:
            if len(text_str) == 0:
                text_str = i
            else:
                continue
        logger.info(len(text_str))
        with open(project_space_path(self.out_filename), 'w') as f:
            f.write(text_str)
            