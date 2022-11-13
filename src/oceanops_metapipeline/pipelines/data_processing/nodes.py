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
    series = primary_raw_dataset[header]
    dataframe_new = series_2_dataframe(series)
    return dataframe_new

def df_merge(df1: pd.DataFrame,df2: pd.DataFrame,df1_discard: str) -> pd.DataFrame:
    df1.drop(columns=df1_discard, inplace=True)
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
    refined_dataset = intermediate_dataset.merge(reference_dataset, how='outer', left_on=header["df_merge"],
                                                 right_on=header["rt_merge"])
    refined_dataset = refined_dataset.loc[~refined_dataset[header["df_merge"]].isna()]
    refined_dataset.drop(columns=header["rt_merge"], inplace=True)
    refined_dataset.rename(columns={header["df_merge"]:header["rt_merge"]},inplace=True)
    refined_dataset.set_index(['ptf_id', 'lat', 'lon'], inplace=True)
    new_headers_refined = {key: header['header_name'] + '_' + key for key in refined_dataset.keys()}
    refined_dataset.rename(columns=new_headers_refined, inplace=True)
    time_delta = pd.to_datetime(refined_dataset['Variable_lastMeasured']) - pd.to_datetime(
        refined_dataset['Variable_firstMeasured'])
    refined_dataset.insert(2, "time_delta", time_delta)
    return refined_dataset

def year_dictionary(years):
    years = years.split(',')
    return {year: 1 for year in range(int(years[0]),int(years[1])+1)}

def boolean_variable_extension(primary_raw_dataset):
    primary_raw_dataset['start_year'] = pd.to_datetime(primary_raw_dataset['Variable_firstMeasured']).dt.year.astype(str)
    primary_raw_dataset['stop_year'] = pd.to_datetime(primary_raw_dataset['Variable_lastMeasured']).dt.year.astype(str)
    primary_raw_dataset['year_list'] = primary_raw_dataset['start_year'] + ',' + primary_raw_dataset['stop_year']
    year_series = primary_raw_dataset['year_list'].apply(year_dictionary)
    year_boolean_df = pd.DataFrame(list(year_series), index=year_series.index)
    year_boolean_df = ~year_boolean_df.isna()
    year_columns = list(year_boolean_df.keys())
    year_columns.sort()
    boolean_variable_df = year_boolean_df[year_columns]
    return boolean_variable_df
