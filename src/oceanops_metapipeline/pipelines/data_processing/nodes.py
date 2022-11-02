"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

import pandas as pd
import requests
from typing import Dict

def series_2_dataframe(series: pd.core.series.Series) -> pd.DataFrame:
    try:
        series = series.apply(eval)
    except:
        pass
    series_df = pd.DataFrame(list(series),index=series.index)
    print(series_df)
    return series_df

def json_pandas(oops_platforms_region: requests.models.Response) -> pd.DataFrame:
    oops_platforms_region = oops_platforms_region.json()
    platforms_df = pd.DataFrame(oops_platforms_region['data'])
    platforms_df.set_index('id', inplace=True)
    platforms_series = platforms_df['ptfDepl']
    platforms_coordinates = series_2_dataframe(platforms_series)
    platforms_df = platforms_df.join(platforms_coordinates)
    platforms_df.drop(columns='ptfDepl',inplace=True)
    platforms_df.index.rename('ptf_id',inplace=True)
    # platforms_df = pd.merge(platforms_df,platforms_coordinates)
    return platforms_df

def json_extender(primary_raw_dataset: pd.DataFrame, header: str) -> pd.DataFrame:
    # header = 'program'
    subset_raw_dataset = primary_raw_dataset[['lat','lon',header]]
    series = subset_raw_dataset[header]
    dataframe_new = series_2_dataframe(series)
    intermediate_dataset = subset_raw_dataset.join(dataframe_new)
    intermediate_dataset.drop(columns=header,inplace=True)
    return intermediate_dataset


