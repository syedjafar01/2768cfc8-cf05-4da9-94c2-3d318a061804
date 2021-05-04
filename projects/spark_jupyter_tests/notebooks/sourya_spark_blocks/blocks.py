import razor
import razor.flow as rf
from razor import api
from razor.flow.spark import SparkBlock, SparkExecutor
import typing as t
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)



@rf.block
class NonSparkPath:
    __publish__ = True
    __label__ = "NonSpark_Path"
    ip_path: str
    op_path: rf.Output[str]
    def run(self):
        self.op_path.put(project_space_path(self.ip_path))
        
        
@rf.block
class NonSparkCols:
    __publish__ = True
    __label__ = "NonSpark_Cols"
    cols_list: t.Any
    # Atomic input - csv filename relative to project space
    selected_cols: rf.Output[t.Any]
    # Atomic output of type spark DataFrame.
    def run(self):
        self.selected_cols.put(self.cols_list)
        
        
@rf.block
class ReadCsvProjectSpaceETA(SparkBlock):
    __publish__ = True
    __label__ = "ReadCsv_ProjectSpace_ETA"
    filename:str    
    def run(self):
        df = self.spark.read.csv(self.filename, header = True)
        print("Data Schema")
        df.printSchema()
        

@rf.block
class SelectDataDescribe(SparkBlock):
    __publish__ = True
    __label__ = "SelectData_Describe"
    filename: str
    def run(self):
        df = self.spark.read.csv(self.filename, header = True)
        print("Data Stat")
        df_stat = df.describe()
        df_stat.show()
        
        
@rf.block
class ReadCsvProjectSpace(SparkBlock):
    __publish__ = True
    __label__ = "ReadCsv_ProjectSpace"
    filename:str
    data: rf.Output[DataFrame]
    def run(self):
        df = self.spark.read.csv(self.filename, header = True)
        print("Data Schema")
        df.printSchema()
        self.data.put(df)
        

@rf.block
class SelectData(SparkBlock):
    __publish__ = True
    __label__ = "Select_Data"
    wanted_cols: t.Any
    inputData: DataFrame
    outputData: rf.Output[DataFrame]
    def run(self):
        df = self.inputData
        df_selected = df.select(self.wanted_cols)
        df_selected.show()
        self.outputData.put(df_selected)
        
        
        
@rf.block
class ConcatData(SparkBlock):
    __publish__ = True
    __label__ = "Concat_Data"
    inputData_1: DataFrame
    inputData_2: DataFrame
    outputData: rf.Output[DataFrame]
    def run(self):
        df_concat = self.inputData_1.union(self.inputData_1)
        print("Concat Data")
        df_concat.show()
        self.outputData.put(df_concat)
        

@rf.block
class RenameCol(SparkBlock):
    __publish__ = True
    __label__ = "Rename_Col"
    inputData: DataFrame
    col_dic: t.Any
    outputData: rf.Output[DataFrame]
    def run(self):
        df_rename = self.inputData.withColumnRenamed(list(self.col_dic.keys())[0], self.col_dic[list(self.col_dic.keys())[0]])
        df_rename = df_rename.withColumnRenamed(list(self.col_dic.keys())[1], self.col_dic[list(self.col_dic.keys())[1]])
        self.outputData.put(df_rename)
      

@rf.block
class DropDuplicates(SparkBlock):
    __publish__ = True
    __label__ = "Drop_Duplicates"
    inputData: DataFrame
    outputData: rf.Output[DataFrame]
    def run(self):
        df_clean = self.inputData.dropDuplicates()
        print('final dataframe')
        df_clean.show()
        self.outputData.put(df_clean)     
        

        
        
        
        
        