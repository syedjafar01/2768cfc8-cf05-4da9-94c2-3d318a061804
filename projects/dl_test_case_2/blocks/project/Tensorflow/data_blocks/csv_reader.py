import os
from itertools import chain
from logging import getLogger

import pandas as pd
import razor.flow as rf


def project_space_path(path):
    from razor import api
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


logger = getLogger("projectspacereader")


def validate_input_column_mapping(input_dict):
    if not input_dict:
        raise ValueError("Input Column mapping cannot be empty.")
    if not isinstance(input_dict, dict):
        raise ValueError(f"Input Column mapping must be a dictionary. got {input_dict} of type {type(input_dict)}")
    if any(not isinstance(item, str) for item in input_dict.keys()):
        raise ValueError(
            f"Invalid input_column_mapping. All keys must be strings (Input layer names), Got Keys {list(input_dict.keys())}")
    for key, columns in input_dict.items():
        if not columns or not isinstance(columns, list):
            raise ValueError(
                f"Invalid input_column_mapping. All Values must be list of strings. Got {columns} for key {key}")
        if any(not isinstance(col, str) for col in columns):
            raise ValueError(f"Invalid columns names for key {key}. All columns names should be string. Got {columns} ")


@rf.block()
class CsvProjectSpaceReader:
    __publish__ = True
    __label__ = "CsvProjectSpaceReader"
    __category__ = "TF Blocks"

    path: str = "titanic.csv"
    input_column_mapping: dict = {"InputLayer__1": ["A1", "A2", "A3"], "InputLayer__2": ["Y"]}
    chunk_size: int = 100

    data: rf.SeriesOutput[dict]

    def run(self):

        path = self.path.replace('"', '').replace("'", "")
        path = project_space_path(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} doesn't exist in project space")

        chunk_size = self.chunk_size
        if not isinstance(self.chunk_size, int):
            chunk_size = int(self.chunk_size)
        validate_input_column_mapping(self.input_column_mapping)
        columns = list(chain.from_iterable(self.input_column_mapping.values()))
        logger.info(type(self.input_column_mapping))
        logger.info(path)
        for df in pd.read_csv(path, chunksize=chunk_size, usecols=columns):
            output = {}
            for input_layer, mapping in self.input_column_mapping.items():
                output[input_layer] = df[mapping].values
            for row in zip(*output.values()):
                self.data.put({x: y for x, y in zip(self.input_column_mapping.keys(), row)})
