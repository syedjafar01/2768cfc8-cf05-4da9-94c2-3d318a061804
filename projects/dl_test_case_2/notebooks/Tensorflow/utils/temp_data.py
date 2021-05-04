import json
import tempfile

import numpy as np

import logging

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):  # pragma: no cover
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def project_space_path(path):
    return "/tmp/dl_temp"


class TempStore:  # pragma: no cover

    def __init__(self, data):
        self.temp_file = tempfile.NamedTemporaryFile(prefix=project_space_path("dl_temp"))
        self.epoch_counter = 0
        self.data = data
        logger.debug(f"Temp store created {self.temp_file.name}")

    def data_write(self):
        if self.epoch_counter == 0:
            self.epoch_counter += 1
            with open(self.temp_file.name, 'w') as f:
                logger.debug(f"Opened Temp file {self.temp_file.name}")
                for i in self.data:
                    f.write(json.dumps(obj=i, cls=NumpyEncoder) + "\n")
                    yield i
        else:
            with open(self.temp_file.name) as f:
                line = f.readline()
                while line:
                    yield json.loads(line)
                    line = f.readline()

    def infer_data_write(self):
        for i in self.data:
            yield i

    def close(self):
        logger.debug(f"Closing tmp file {self.temp_file.name}")
        self.temp_file.close()