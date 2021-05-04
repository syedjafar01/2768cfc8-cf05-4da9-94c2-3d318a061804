import razor.flow as rf
import typing as t
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from category_encoders import *
import xgboost as xgb
import shap
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import BayesianRidge, LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import os
from razor import api


def project_space_path(path: str):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)

@rf.block
class LoadCTRData:
    __publish__ = True
    __label__ = "LoadCTRData"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_data_filepath: str
    ad_data: rf.Output[t.Any] 
    ad_target_data: rf.Output[t.Any] 
    def run(self):
        ad_data_df = pd.read_csv(project_space_path(self.ad_data_filepath))
        ad_data_df = ad_data_df.drop(columns=['Ad ID'])

        target_pred_col = 'CTR (link click-through rate)'

        target_data = ad_data_df[[target_pred_col]]
        ad_data = ad_data_df.drop(columns=[target_pred_col])
        
        self.ad_data.put(ad_data)
        self.ad_target_data.put(target_data)

        
@rf.block
class SplitData:
    __publish__ = True
    __label__ = "SplitData"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_data: t.Any
    ad_target_data: t.Any
    ad_train_data: rf.Output[t.Any] 
    ad_test_data: rf.Output[t.Any] 
    ad_train_data_target: rf.Output[t.Any] 
    ad_test_data_target: rf.Output[t.Any] 
    ad_col_dtypes_data: rf.Output[t.Any] 
    def run(self):
        column_datatypes = {i:str(self.ad_data.dtypes.to_dict()[i]) for i in list(self.ad_data.columns)}

        dtypes_col_nm_dic = { i:[] for i in column_datatypes.values()}
        for i in column_datatypes.keys():
            dtypes_col_nm_dic[column_datatypes[i]].append(i)
            
        X_train, X_test, y_train, y_test = train_test_split(self.ad_data, self.ad_target_data, test_size=0.33, random_state=42)
        
        self.ad_train_data.put(X_train)
        self.ad_test_data.put(X_test)
        self.ad_train_data_target.put(y_train)
        self.ad_test_data_target.put(y_test)
        self.ad_col_dtypes_data.put(dtypes_col_nm_dic)
        

@rf.block
class Data_Encoding:
    __publish__ = True
    __label__ = "Data_Encoding"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_train_data: t.Any
    ad_test_data: t.Any
    ad_train_data_target: t.Any
    ad_col_dtypes_data: t.Any
    ad_train_enc_data: rf.Output[t.Any] 
    ad_test_enc_data: rf.Output[t.Any] 
    def run(self):
        enc = TargetEncoder(cols=self.ad_col_dtypes_data['object'])
        X_train = enc.fit_transform(self.ad_train_data, self.ad_train_data_target)
        X_test = enc.transform(self.ad_test_data)
        
        self.ad_train_enc_data.put(X_train)
        self.ad_test_enc_data.put(X_test)

@rf.block
class Scaling:
    __publish__ = True
    __label__ = "Scaling"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_train_enc_data: t.Any
    ad_test_enc_data: t.Any
    ad_train_scale_data: rf.Output[t.Any] 
    ad_test_scale_data: rf.Output[t.Any] 
    def run(self):
        scaler = StandardScaler()
        X_train = scaler.fit_transform(self.ad_train_enc_data)
        X_test = scaler.transform(self.ad_test_enc_data)
        
        self.ad_train_scale_data.put(X_train)
        self.ad_test_scale_data.put(X_test)

@rf.block
class TrainModel:
    __publish__ = True
    __label__ = "TrainModel"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_train_data: t.Any
    ad_test_data: t.Any 
    ad_train_data_target: t.Any
    ad_test_data_pred: rf.Output[t.Any]
    def run(self):
        X_train = self.ad_train_data
        X_test = self.ad_test_data
        y_train = np.squeeze(self.ad_train_data_target.values)
        
        xg_reg = xgb.XGBRegressor(objective ='reg:squarederror', colsample_bytree = 0.3, learning_rate = 0.1,
                max_depth = 5, alpha = 10, n_estimators = 10)
        xg_reg.fit(X_train,y_train)

        y_test_pred = xg_reg.predict(X_test)
        self.ad_test_data_pred.put(y_test_pred)
        
@rf.block
class Evaluation:
    __publish__ = True
    __label__ = "Evaluation"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ad_test_data_target: t.Any
    ad_test_data_pred: t.Any
    def run(self):
        y_test_pred = self.ad_test_data_pred
        y_test = np.squeeze(self.ad_test_data_target.values)
        
        print('MSE : ',mean_squared_error(y_test, y_test_pred))
        print('RMSE : ',mean_squared_error(y_test, y_test_pred,squared=False))
        print('r2_score : ',r2_score(y_test, y_test_pred))

@rf.block
class SavePrediction:
    __publish__ = True
    __label__ = "SavePrediction"
    __description__ = "load raw data"
    __tags__ = ["CTRPred_blocks"]
    __category__ = "CTRPred"
    
    ctr_pred_data_filepath: str
    ad_test_data: t.Any
    ad_test_data_target: t.Any
    ad_test_data_pred: t.Any
    def run(self):
        y_test_pred = self.ad_test_data_pred
        y_test = np.squeeze(self.ad_test_data_target.values)
        self.ad_test_data['Pred_CTR'] = list(y_test_pred)
        self.ad_test_data['GT_CTR'] = list(y_test)
        
        self.ad_test_data.to_csv(project_space_path(self.ctr_pred_data_filepath),index=False )
        