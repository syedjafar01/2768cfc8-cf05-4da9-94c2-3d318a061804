import razor.flow as rf
import typing as t
import os
from razor import api


def project_space_path(path: str):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ProcessExecutor)
class FilterData:
    __publish__ = True
    __label__ = "Filter_Data"
    
    ad_attr_age: t.Any
    ad_attr_dma: t.Any
    ad_attr_placement: t.Any
    ad_response: t.Any
    state_feat: t.Any
    ad_attr_age_filtered: rf.Output[t.Any]
    ad_attr_dma_filtered: rf.Output[t.Any]
    ad_attr_placement_filtered: rf.Output[t.Any]
    ad_response_filtered: rf.Output[t.Any]
    state_feat_filtered: rf.Output[t.Any]

    def run(self):
        ad_attr_age_cols = ['Ad ID', 'Age', 'Gender', 'CTR (link click-through rate)']
        ad_attr_placement_cols = ['Ad ID', 'Placement', 'CTR (link click-through rate)']
        ad_attr_dma_cols = ['Ad ID', 'DMA', 'CTR (link click-through rate)']
        ad_response_cols = ['Ad ID','Body (ad settings)','Campaign budget','Link (ad settings)','headline']

        ad_attr_age_df = self.ad_attr_age[ad_attr_age_cols]
        ad_attr_placement_df = self.ad_attr_placement[ad_attr_placement_cols]
        ad_attr_dma_df = self.ad_attr_dma[ad_attr_dma_cols]
        ad_response_df = self.ad_response[ad_response_cols]
        state_feat_df = self.state_feat[['States', 'IT_Market_Share', 'Total_Market_Share', 'IT_Market_Ratio']]

        self.ad_attr_age_filtered.put(ad_attr_age_df)
        self.ad_attr_dma_filtered.put(ad_attr_dma_df)
        self.ad_attr_placement_filtered.put(ad_attr_placement_df)
        self.ad_response_filtered.put(ad_response_df)
        self.state_feat_filtered.put(state_feat_df)