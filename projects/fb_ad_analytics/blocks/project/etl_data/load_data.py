import razor.flow as rf
import typing as t
import pandas as pd
import os
from razor import api


def project_space_path(path: str):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)

@rf.block
class LoadData:
    __publish__ = True
    __label__ = "Load_Data"
    
    age_attr_filepath: str
    dma_attr_filepath: str
    placement_attr_filepath: str
    ad_response_filepath: str
    ad_attr_age: rf.Output[t.Any]
    ad_attr_dma: rf.Output[t.Any]
    ad_attr_placement: rf.Output[t.Any]
    ad_response: rf.Output[t.Any]
    def run(self):
        ad_attr_age_df = pd.read_excel(project_space_path(self.age_attr_filepath))
        ad_attr_dma_df = pd.read_excel(project_space_path(self.dma_attr_filepath))
        ad_attr_placement_df = pd.read_excel(project_space_path(self.placement_attr_filepath))

        ad_response_df = pd.read_excel(project_space_path(self.ad_response_filepath))

        self.ad_attr_age.put(ad_attr_age_df)
        self.ad_attr_dma.put(ad_attr_dma_df)
        self.ad_attr_placement.put(ad_attr_placement_df)
        self.ad_response.put(ad_response_df)