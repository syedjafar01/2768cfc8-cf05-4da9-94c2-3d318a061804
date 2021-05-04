import razor.flow as rf
import typing as t


@rf.block
class FillNaN:
    __publish__ = True
    __label__ = "Fill_Nan"

    ad_attr_age_joined: t.Any
    ad_attr_dma_joined: t.Any
    ad_attr_placement_joined: t.Any
    ad_attr_age_data: rf.Output[t.Any]
    ad_attr_dma_data: rf.Output[t.Any]
    ad_attr_placement_data: rf.Output[t.Any]

    def dtypewise_fillna(self,joined_data):
        column_datatypes = {i:str(joined_data.dtypes.to_dict()[i]) for i in list(joined_data.columns)}

        dtypes_col_nm_dic = { i:[] for i in column_datatypes.values()}
        for i in column_datatypes.keys():
            dtypes_col_nm_dic[column_datatypes[i]].append(i)

        numerical_columns = dtypes_col_nm_dic['int64'] + dtypes_col_nm_dic['float64']
        cat_columns = dtypes_col_nm_dic['object']

        for col_nm in numerical_columns:
            joined_data[col_nm] = joined_data[col_nm].fillna(0)

        for col_nm in cat_columns:
            joined_data[col_nm] = joined_data[col_nm].fillna('unknown')

        return joined_data

    def run(self):
        ad_attr_age_df = self.dtypewise_fillna(self.ad_attr_age_joined)
        ad_attr_placement_df = self.dtypewise_fillna(self.ad_attr_placement_joined)
        ad_attr_dma_df = self.dtypewise_fillna(self.ad_attr_dma_joined)

        self.ad_attr_age_data.put(ad_attr_age_df)
        self.ad_attr_dma_data.put(ad_attr_dma_df)
        self.ad_attr_placement_data.put(ad_attr_placement_df)