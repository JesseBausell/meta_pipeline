"""
This is a boilerplate pipeline 'descriptive_stats'
generated using Kedro 0.18.3
"""
import pandas as pd
from typing import Dict

# def dataframe_merged_vc(primary_dataset: pd.DataFrame,reference_dataset: pd.DataFrame,header: Dict) -> pd.DataFrame:
#     primary_dataset = primary_dataset[header["df_headers"]]
#     value_counts = pd.DataFrame(primary_dataset[header["df_merge"]].value_counts()).rename(
#         columns={header["df_merge"]: "counts"})
#     value_counts.index.rename(header["df_merge"], inplace=True)
#     reference_dataset = reference_dataset[header["rt_headers"]]
#     merged_dataset = value_counts.merge(reference_dataset, how='outer', left_index=True, right_on=header["rt_merge"])
#     merged_dataset = merged_dataset.loc[~merged_dataset['counts'].isna()]
#     merged_dataset.rename(columns={k: f"Variable_{k}" for k in merged_dataset.keys()}, inplace=True)
#     return merged_dataset

def dataframe_tabulated(primary_dataset: pd.DataFrame, header: Dict) -> pd.DataFrame:
    primary_lists = primary_dataset.groupby(header['group_by'], sort=True)[header["list_by"]].agg(
        lambda x: list(set(x))).reset_index()
    primary_counts = primary_dataset.groupby(header['group_by'])[header['group_by']].count().to_frame().rename(
        columns={header['group_by']: "counts"})
    merged_counts = primary_counts.merge(primary_lists, left_index=True, right_on=header['group_by'])
    return merged_counts

def dataframe_merge_tabulate(primary_dataset: pd.DataFrame, reference_dataset: pd.DataFrame, header: Dict):
    reference_dataset = reference_dataset[header["rt_headers"]]
    primary_dataset_merged = primary_dataset.merge(reference_dataset,how="inner",left_index=True, right_index=True)
    merged_counts = dataframe_tabulated(primary_dataset_merged, header)
    return merged_counts


