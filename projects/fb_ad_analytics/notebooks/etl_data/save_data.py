import razor.flow as rf
import typing as t
import os
from razor import api


def project_space_path(path: str):
    ps = api.datasources('Project Space')
    return os.path.join(ps.abspath(), path)


@rf.block(executor=rf.ProcessExecutor)
class SaveData:
    __publish__ = True
    __label__ = "Save_Data"

    ad_attr_age_data: t.Any
    ad_attr_dma_data: t.Any
    ad_attr_placement_data: t.Any
    age_attr_data_filepath: str
    dma_attr_data_filepath: str
    placement_attr_data_filepath: str

    def run(self):
        self.ad_attr_age_data.to_csv(project_space_path(self.age_attr_data_filepath),index=False )
        self.ad_attr_placement_data.to_csv(project_space_path(self.dma_attr_data_filepath),index=False )
        self.ad_attr_dma_data.to_csv(project_space_path(self.placement_attr_data_filepath),index=False )