import pandas as pd
import razor.flow as rf
import numpy as np


@rf.block
class SklearnMLFeeder:
    __publish__ = True
    __label__ = "SklearnMLFeeder"
    __description__ = "Sklearn ML feeder from pandas to numpy x and y"
    __tags__ = ["SklearnMLFeeder","getdata","mlfeeder","sklearndata"]
    __category__ = "ML UTILS"
    x_columns: list = rf.Input(default= [],label="x_columns")
    y_column: str = rf.Input(default="",label="y_column")
    df: pd.core.frame.DataFrame = rf.Input(label="input_df",help="pandas dataframe input")
    out_x: rf.Output[np.ndarray]
    out_y: rf.Output[np.ndarray]
    feature_names: rf.Output[list]
    def run(self):
        if self.y_column is not None and len(self.y_column)!=0:
            if len(self.x_columns)==0:
                x_columns_ = self.df.columns.to_list()
                x_columns_.remove(self.y_column)
            else:
                x_columns_ = self.x_columns
            x = self.df[x_columns_].values
            y = np.squeeze(self.df[self.y_column].values)
            self.out_x.put(x)
            self.out_y.put(y)
        else:
            if len(self.x_columns)==0:
                x_columns_ = self.df.columns.to_list()
                x_columns_.remove(self.y_column)
            else:
                x_columns_ = self.x_columns
            x = self.df[x_columns_].values
            self.out_x.put(x)
        self.feature_names.put(x_columns_)