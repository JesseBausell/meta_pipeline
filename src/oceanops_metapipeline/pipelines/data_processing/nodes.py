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
    print(header)
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

def df_merge(df1: pd.DataFrame,df2: pd.DataFrame,df1_discard: Dict) -> pd.DataFrame:
    df1_discard = df1_discard["exclude_program"]
    df1.drop(columns=df1_discard, inplace=True)
    # df2.drop(columns=df2_discard, inplace=True)
    merged_df = df1.join(df2)
    return merged_df

