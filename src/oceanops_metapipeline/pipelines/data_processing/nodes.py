"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

import pandas as pd
import requests
from typing import Dict

def series_2_dataframe(series: pd.core.series.Series) -> pd.DataFrame:
    try:
        series.loc[series.isna()] = "{}"
        series = series.apply(eval)
    except:
        pass
    series_df = pd.DataFrame(list(series),index=series.index)
    header = series.name
    if header != "ptfVariables":
        new_headers = {key: header + '_' + key for key in series_df.keys()}
        series_df.rename(columns=new_headers, inplace=True)
    return series_df

def json_pandas(oops_platforms_region: requests.models.Response) -> pd.DataFrame:
    oops_platforms_region = oops_platforms_region.json()
    platforms_df = pd.DataFrame(oops_platforms_region['data'])
    platforms_df.set_index('id', inplace=True)
    platforms_series = platforms_df['ptfDepl']
    platforms_coordinates = series_2_dataframe(platforms_series)
    platforms_df = platforms_df.join(platforms_coordinates)
    platforms_df.drop(columns='ptfDepl',inplace=True)
    platforms_df.rename(columns={'ptfDepl_lat':'lat','ptfDepl_lon':'lon','ptfDepl_ship':'ship'},inplace=True)
    platforms_df.index.rename('ptf_id',inplace=True)
    return platforms_df

def json_extender(primary_raw_dataset: pd.DataFrame, header: str) -> pd.DataFrame:
    # subset_raw_dataset = primary_raw_dataset[['lat','lon',header]]
    series = primary_raw_dataset[header]
    dataframe_new = series_2_dataframe(series)
    # intermediate_dataset = primary_raw_dataset.join(dataframe_new)
    # dataframe_new.drop(columns=header,inplace=True)
    return dataframe_new

def df_merge(df1: pd.DataFrame,df2: pd.DataFrame,df1_discard: str) -> pd.DataFrame:
    # df1_discard = df1_discard["exclude_program"]
    df1.drop(columns=df1_discard, inplace=True)
    # df2.drop(columns=df2_discard, inplace=True)
    merged_df = df1.join(df2)
    return merged_df

def varaiable_extender(primary_raw_dataset: pd.DataFrame, header: str) -> pd.DataFrame:
    series = primary_raw_dataset[header]
    dataframe_new = series_2_dataframe(series)
    stacked_dataframe = pd.DataFrame((dataframe_new.stack()), columns=[header])
    variable_df = pd.DataFrame(list(stacked_dataframe[header]), index=stacked_dataframe.index)
    variable_df.fillna(method='ffill', inplace=True)
    return variable_df

def dataframe_refinement(intermediate_dataset: pd.DataFrame,reference_dataset: pd.DataFrame,header: Dict) -> pd.DataFrame:
    intermediate_dataset = intermediate_dataset[header["df_headers"]]
    intermediate_dataset.drop_duplicates(inplace=True)
    reference_dataset = reference_dataset[header["rt_headers"]]
    refined_dataset = intermediate_dataset.merge(reference_dataset, how='inner', left_on=header["df_merge"], right_on=header["rt_merge"])
    refined_dataset.drop(columns=header["rt_merge"])

    value_counts = pd.DataFrame(intermediate_dataset['variableId'].value_counts()).rename(columns={header["df_merge"]: 'counts'})
    value_counts.index.rename(header["rt_merge"], inplace=True)
    meta_variable_total = value_counts.merge(reference_dataset, how='inner', left_index=True,right_on=header["rt_merge"])
    value_counts.reset_index(inplace=True)
    IND = [i for i, v in enumerate(value_counts[header["rt_merge"]].values) if v not in reference_dataset[header["rt_merge"]].values]
    unknown_values = value_counts.iloc[IND]
    meta_variable_total = pd.concat([meta_variable_total, unknown_values])
    meta_variable_total.rename(columns={header["rt_merge"]:header["df_merge"]},inplace=True)
    return refined_dataset, meta_variable_total



