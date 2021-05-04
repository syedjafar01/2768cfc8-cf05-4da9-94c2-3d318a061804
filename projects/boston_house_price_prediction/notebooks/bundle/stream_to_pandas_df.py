import pandas as pd
import razor.flow as rf
import typing as t


@rf.block
class DataQueueToPandasDf_PythonStream:
    input_stream: rf.SeriesInput[t.List[list]]= rf.Input(label="input_stream",help = "data queue from the previous stream block")
    input_schema: dict = rf.Input(label = "input_schema",help = "data schema from the previous stream block")
    df: rf.Output[pd.DataFrame] = rf.Output(label="df")
    __publish__ = True
    __label__ = "Stream to Pandas"
    __description__ = "Converts data queue to pandas dataframe"
    __tags__ = ["kafkatopandas","streamtopandas","dataqueuetopandas","kafka","queue","pandas","stream"]
    __category__ = 'DataProcessing_PythonStreaming'
    def run(self):
        rcve_count = 0
        final_rows = []
        for data in self.input_stream:
            rcve_count += len(data)
            self.logger.info("total records recived in streaming : " +str(rcve_count))
            for row in data:
                final_rows.append(row)
        self.logger.info("Shape of data : "+str(len(final_rows)))
        df_ = pd.DataFrame(final_rows,columns=self.input_schema['columns'])
        df_ = df_.astype(self.input_schema['dtypes'])
        self.logger.info("Type of data : "+str(df_.dtypes))
        self.df.put(df_)