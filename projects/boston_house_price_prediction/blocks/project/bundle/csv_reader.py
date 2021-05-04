import pandas as pd
import csv
from datetime import datetime
from dateutil.parser import parse
import razor.flow as rf
import typing as t
import razor
from razor.api import datasources



@rf.block
class CsvProjectSpaceReader_PythonStream:
    input_file_path: str = rf.Input(label = "input_file_path",help = "path to the file to be read")
    batch_size: int = rf.Input(default = 10000, label = "batch size",help = "number of lines to be read in single iteration")
    parse_dates: list = rf.Input(default=[], label="parse dates",
                                 help="date columns to be parsed to standard python datetime format as an array")
    date_parser: str = rf.Input(default="", label="date parser standard",
                                help="custom date format of the date columns in standard python datetime format")

    limit: int = rf.Input(default=-1, label="limit",
                                help="total number of rows to read default is -1 and it will read entire data on default value")
    output_stream: rf.SeriesOutput[t.List[list]] = rf.SeriesOutput(label="output_stream", transport=rf.KafkaTransport)
    output_schema: rf.Output[dict] = rf.Output(label="output_schema", transport=rf.KafkaTransport)

    __publish__ = True
    __label__ = ""
    __description__ = "To read csv/text files from project space"
    __tags__ = ["bigdata","project space","reader","csv/text"]
    __category__ = 'SourceConnectors_PythonStreaming'

    def create_schema(self,ipfilepath, parse_dates=[], date_parser=''):

        if len(parse_dates) > 0:
            if len(date_parser) > 0:
                tmp_df = pd.read_csv(ipfilepath, nrows=100, parse_dates=parse_dates,
                                     date_parser=lambda x: pd.datetime.strptime(x, date_parser))
            else:
                tmp_df = pd.read_csv(ipfilepath, nrows=100, parse_dates=parse_dates)
        else:
            tmp_df = pd.read_csv(ipfilepath, nrows=100)
        data_schema = {}
        data_schema["columns"] = list(tmp_df.columns)
        data_schema["dtypes"] = {}
        for key, val in tmp_df.dtypes.to_dict().items():
            data_schema["dtypes"][key] = str(val)

        return data_schema

    def run(self):
        
        ps_path = datasources('Project Space').abspath()
        self.logger.info(f"Project Space path: {ps_path}")

        data_schema = self.create_schema(ps_path + self.input_file_path, self.parse_dates, self.date_parser)
        self.output_schema.put(data_schema)
        self.logger.info("the schema obtained from the data is:")
        self.logger.info(str(data_schema))

        with open(ps_path + self.input_file_path) as csvfile:
            linereader = csv.reader(csvfile)
            row_count = 0
            tmp_rows = []
            break_limit_flag = False
            for i, row in enumerate(linereader):
                if i == 0:
                    continue


                if len(tmp_rows) == self.batch_size:
                    if self.limit != -1:
                        if row_count + self.batch_size >= self.limit:
                            break_limit_flag = True
                            tmp_batch_size = self.limit - row_count

                    if break_limit_flag:
                        tmp_rows = tmp_rows[:tmp_batch_size]

                    self.output_stream.put(tmp_rows)
                    row_count += len(tmp_rows)
                    tmp_rows = []
                    self.logger.info("records pushed : " + str(row_count))
                    if break_limit_flag:
                        break


                if len(self.parse_dates) > 0:
                    if len(self.date_parser) > 0:
                        for col in self.parse_dates:
                            row[data_schema["columns"].index(col)] = str(
                                datetime.strptime(row[data_schema["columns"].index(col)], self.date_parser))
                    else:
                        for col in self.parse_dates:
                            row[data_schema["columns"].index(col)] = str(parse(row[data_schema["columns"].index(col)]))

                tmp_rows.append(row)

            if len(tmp_rows) > 0:
                self.output_stream.put(tmp_rows)
                row_count += len(tmp_rows)
                self.logger.info("records pushed :" + str(row_count))
