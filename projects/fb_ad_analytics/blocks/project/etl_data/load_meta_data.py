import razor.flow as rf
import typing as t
import pandas as pd
import os
from razor import api


def project_space_path(path: str):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ProcessExecutor)
class LoadMetaData:
    __publish__ = True
    __label__ = "Load_Meta_Data"
    
    usertarget_data_filepath: str
    city_feat_filepath: str
    state_feat_filepath: str
    usertarget_data: rf.Output[t.Any]
    city_feat: rf.Output[t.Any]
    state_feat: rf.Output[t.Any]
    def run(self):
        usertarget_data_df = pd.read_csv(project_space_path(self.usertarget_data_filepath))
        city_feat_df =  pd.read_csv(project_space_path(self.city_feat_filepath))
        state_feat_df =  pd.read_csv(project_space_path(self.state_feat_filepath))

        self.usertarget_data.put(usertarget_data_df)
        self.city_feat.put(city_feat_df)
        self.state_feat.put(state_feat_df)