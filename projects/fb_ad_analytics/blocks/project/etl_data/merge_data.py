import razor.flow as rf
import typing as t

@rf.block
class MergeData:
    __publish__ = True
    __label__ = "Merge_Data"
    
    ad_attr_age_filtered: t.Any
    ad_attr_dma_filtered: t.Any
    ad_attr_placement_filtered: t.Any
    ad_response_filtered: t.Any
    state_feat_filtered: t.Any
    usertarget_data: t.Any
    city_feat: t.Any
    ad_attr_age_joined: rf.Output[t.Any]
    ad_attr_dma_joined: rf.Output[t.Any]
    ad_attr_placement_joined: rf.Output[t.Any]
    
    def run(self):
        ad_attr_age_data = self.ad_response_filtered.merge(self.ad_attr_age_filtered,how='inner',on='Ad ID')
        ad_attr_age_data = self.usertarget_data.merge(ad_attr_age_data,how='right',on='Ad ID')

        ad_attr_placement_data = self.ad_response_filtered.merge(self.ad_attr_placement_filtered,how='inner',on='Ad ID')
        ad_attr_placement_data = self.usertarget_data.merge(ad_attr_placement_data,how='right',on='Ad ID')

        ad_attr_dma_data = self.ad_response_filtered.merge(self.ad_attr_dma_filtered,how='inner',on='Ad ID')
        ad_attr_dma_data = self.usertarget_data.merge(ad_attr_dma_data,how='right',on='Ad ID')

        ad_attr_dma_data = ad_attr_dma_data.merge(self.city_feat,how='left',on='DMA')
        ad_attr_dma_data = ad_attr_dma_data.merge(self.state_feat_filtered,how='left',on='States')

        self.ad_attr_age_joined.put(ad_attr_age_data)
        self.ad_attr_dma_joined.put(ad_attr_dma_data)
        self.ad_attr_placement_joined.put(ad_attr_placement_data)
