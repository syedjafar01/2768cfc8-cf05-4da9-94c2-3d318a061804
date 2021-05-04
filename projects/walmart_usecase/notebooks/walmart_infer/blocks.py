import razor
# General imports
import numpy as np
import pandas as pd
import os, sys, gc, time, warnings, pickle, psutil, random

# custom imports
from multiprocessing import Pool        # Multiprocess Runs

warnings.filterwarnings('ignore')

import razor.flow as rf
from razor import api
import razor

import typing as t



def project_space_path(path):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block
class Infer:
    __publish__ = True
    __label__ = "Infer"
    
    path: str
    
    
    def init(self):
        self.VER = 1                          # Our model version
        self.SEED = 42  
        
        #PATHS for Features
        self.ORIGINAL = project_space_path(self.path)+"/"
#         "M5Forecasting"
        self.BASE     = project_space_path('M5Forecasting/m5-simple-fe/grid_part_1.pkl')
        self.PRICE    = project_space_path('M5Forecasting/m5-simple-fe/grid_part_2.pkl')
        self.CALENDAR = project_space_path('M5Forecasting/m5-simple-fe/grid_part_3.pkl')
        self.LAGS     = project_space_path('M5Forecasting/lags_df_28.pkl')
        self.MEAN_ENC = project_space_path('M5Forecasting/mean_encoding_df.pkl')

        #LIMITS and const
        self.TARGET      = 'sales'            # Our target
        self.START_TRAIN = 0                  # We can skip some rows (Nans/faster training)
        self.END_TRAIN   = 1913               # End day of our train set
        self.P_HORIZON   = 28                 # Prediction horizon
        self.USE_AUX     = False               # Use or not pretrained models
        self.N_CORES = psutil.cpu_count()     # Available CPU cores

        # AUX(pretrained) Models paths
        self.AUX_MODELS = './M5Forecasting/m5-aux-models/'


        #STORES ids
        self.STORES_IDS = pd.read_csv(self.ORIGINAL+'sales_train_validation.csv')['store_id']
        self.STORES_IDS = list(self.STORES_IDS.unique())


        #SPLITS for lags creation
        self.SHIFT_DAY  = 28
        self.N_LAGS     = 15
        self.LAGS_SPLIT = [col for col in range(self.SHIFT_DAY, self.SHIFT_DAY + self.N_LAGS)]
        
        self.remove_features = ['id','state_id','store_id',
                           'date','wm_yr_wk','d', self.TARGET]
        self.mean_features   = ['enc_cat_id_mean','enc_cat_id_std',
                           'enc_dept_id_mean','enc_dept_id_std',
                           'enc_item_id_mean','enc_item_id_std'] 

            
    def seed_everything(self, seed=0):
        random.seed(seed)
        np.random.seed(seed)


    ## Multiprocess Runs
    def df_parallelize_run(self, func, t_split):
        num_cores = np.min([self.N_CORES,len(t_split)])
        pool = Pool(num_cores)
        df = pd.concat(pool.map(func, t_split), axis=1)
        pool.close()
        pool.join()
        return df

    def get_data_by_store(self, store):

        # Read and contact basic feature
        df1 = pd.read_pickle(self.BASE)
#         df1 = df1[df1['store_id']==store]
        df2 = pd.read_pickle(self.PRICE).iloc[:,2:]
#         df2 = df2[df2['store_id']==store]
        df3 = pd.read_pickle(self.CALENDAR).iloc[:,2:]
        df = pd.concat([df1, df2, df3],
                        axis=1)

        # Leave only relevant store
        df = df[df['store_id']==store]

        # With memory limits we have to read 
        # lags and mean encoding features
        # separately and drop items that we don't need.
        # As our Features Grids are aligned 
        # we can use index to keep only necessary rows
        # Alignment is good for us as concat uses less memory than merge.
        df2 = pd.read_pickle(self.MEAN_ENC)[self.mean_features]
        df2 = df2[df2.index.isin(df.index)]

        df = pd.concat([df, df2], axis=1)
        del df2 # to not reach memory limit 
        gc.collect()

        df3 = pd.read_pickle(self.LAGS).iloc[:,3:]
        df3 = df3[df3.index.isin(df.index)]


        df = pd.concat([df, df3], axis=1)
        del df3 # to not reach memory limit 
        gc.collect()

        # Create features list
        features = [col for col in list(df) if col not in self.remove_features]
        features = [col for col in features if col not in ['Unnamed: 0']]
        
        del df # to not reach memory limit 
        gc.collect()
        
        return features

    # Recombine Test set after training
    def get_base_test(self):
        base_test = pd.DataFrame()

        for store_id in self.STORES_IDS:
            temp_df = pd.read_pickle(project_space_path('M5Forecasting/test_'+store_id+'.pkl'))
            temp_df['store_id'] = store_id
            base_test = pd.concat([base_test, temp_df]).reset_index(drop=True)

        return base_test


    ########################### Helper to make dynamic rolling lags
    #################################################################################
    def make_lag(self, LAG_DAY):
        lag_df = base_test[['id','d',self.TARGET]]
        col_name = f'sales_lag_{str(LAG_DAY)}'
        lag_df[col_name] = lag_df.groupby(['id'])[self.TARGET].transform(lambda x: x.shift(LAG_DAY)).astype(np.float16)
        return lag_df[[col_name]]


    def make_lag_roll(self, LAG_DAY, base_test):
        shift_day = LAG_DAY[0]
        roll_wind = LAG_DAY[1]
        lag_df = base_test[['id','d',self.TARGET]]
        col_name = f'rolling_mean_tmp_{str(shift_day)}_{str(roll_wind)}'
        lag_df[col_name] = lag_df.groupby(['id'])[self.TARGET].transform(lambda x: x.shift(shift_day).rolling(roll_wind).mean())
        return lag_df[[col_name]]
    
    def run(self):
        ########################### Predict
        #################################################################################

        # Create Dummy DataFrame to store predictions
        all_preds = pd.DataFrame()

        # Join back the Test dataset with 
        # a small part of the training data 
        # to make recursive features
        base_test = self.get_base_test()

        # Timer to measure predictions time 
        main_time = time.time()

        ROLS_SPLIT = []

        for i in [1,7,14]:
            for j in [7,14,30,60]:
                ROLS_SPLIT.append([i,j])

        # Loop over each prediction day
        # As rolling lags are the most timeconsuming
        # we will calculate it for whole day
        for PREDICT_DAY in range(1,29):    
            print('Predict | Day:', PREDICT_DAY)
            start_time = time.time()

            # Make temporary grid to calculate rolling lags
            grid_df = base_test.copy()
            lag_param_df = pd.DataFrame()
#             grid_df = pd.concat([grid_df, self.df_parallelize_run(self.make_lag_roll, ROLS_SPLIT)], axis=1)
            for lag_param in ROLS_SPLIT:
                lag_param_df = pd.concat([lag_param_df, self.make_lag_roll(lag_param,base_test)], axis=1)
            grid_df = pd.concat([grid_df, lag_param_df], axis=1)


            for store_id in self.STORES_IDS:
                self.logger.info(store_id)

                # Read all our models and make predictions
                # for each day/store pairs
                model_path = project_space_path('M5Forecasting/lgb_model_'+store_id+'_v'+str(self.VER)+'.bin') 
                if self.USE_AUX:
                    model_path = self.AUX_MODELS + model_path

                estimator = pickle.load(open(model_path, 'rb'))

                day_mask = base_test['d']==(self.END_TRAIN+ PREDICT_DAY)
                store_mask = base_test['store_id']==store_id

                mask = (day_mask)&(store_mask)
                MODEL_FEATURES = self.get_data_by_store(store_id)
                base_test[self.TARGET][mask] = estimator.predict(grid_df[mask][MODEL_FEATURES])

            # Make good column naming and add 
            # to all_preds DataFrame
            temp_df = base_test[day_mask][['id',self.TARGET]]
            temp_df.columns = ['id','F'+str(PREDICT_DAY)]
            if 'id' in list(all_preds):
                all_preds = all_preds.merge(temp_df, on=['id'], how='left')
            else:
                all_preds = temp_df.copy()

            print('#'*10, ' %0.2f min round |' % ((time.time() - start_time) / 60),
                          ' %0.2f min total |' % ((time.time() - main_time) / 60),
                          ' %0.2f day sales |' % (temp_df['F'+str(PREDICT_DAY)].sum()))
            del temp_df

        all_preds = all_preds.reset_index(drop=True)

        submission = pd.read_csv(self.ORIGINAL+'sample_submission_accuracy.csv')[['id']]
        submission = submission.merge(all_preds, on=['id'], how='left').fillna(0)
        submission.to_csv(self.ORIGINAL+'submission_v'+str(self.VER)+'.csv', index=False)
    