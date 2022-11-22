"""
This is a boilerplate pipeline 'descriptive_stats'
generated using Kedro 0.18.3
"""
import pandas as pd
from typing import Dict
import numpy as np

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
    delta_time = primary_dataset.groupby(header["group_by"])["time_delta"].agg(["min", "max", "median"])
    delta_time.rename(columns={"min":"delta_t_min","max":"delta_t_max","median":"delta_t_median"},inplace=True)
    delta_time = delta_time.round(2)
    merged_counts = delta_time.merge(merged_counts,how="inner",left_index=True, right_on=header['group_by'])
    return merged_counts

def dataframe_counts_annual(boolean_dataset: pd.DataFrame, totals_dataset: pd.DataFrame, header: Dict) -> pd.DataFrame:
    yearly_columns = pd.to_numeric(boolean_dataset.columns,errors='coerce')
    yearly_counts = boolean_dataset.groupby(header['group_by'])[
        list(yearly_columns[~yearly_columns.isna()].astype(int).astype(str))].agg(list(header['math_operations'].keys()))
    yearly_counts = totals_dataset[[header['group_by']] + [header['reference_index']]].merge(yearly_counts, how='inner', right_on=header["group_by"],
                                                           left_on=header["group_by"])
    yearly_counts.set_index([header['group_by']] + [header['reference_index']], inplace=True)
    return yearly_counts

def dataframe_counts_excel(boolean_dataset_counts: pd.DataFrame, boolean_dataset_days: pd.DataFrame, totals_dataset: pd.DataFrame, header_counts: Dict, header_days: Dict) -> Dict[str,pd.DataFrame]:
    excel_data_totals = {"Totals": totals_dataset}
    excel_data_totals[str(*header_counts["math_operations"].values())] = dataframe_counts_annual(boolean_dataset_counts,totals_dataset,header_counts)
    annual_stats_days = dataframe_counts_annual(boolean_dataset_days, totals_dataset, header_days)
    year_list, var_list = zip(*annual_stats_days.keys())
    year_list = np.unique(np.asarray(year_list,dtype=int))
    year_list = np.asarray(year_list,dtype=str)
    var_list = np.unique(np.asarray(var_list,dtype=str))
    for v in var_list:
        colum_list = [(year, v) for year in year_list]
        excel_data_totals[header_days['math_operations'][v]] = annual_stats_days[colum_list]
    return excel_data_totals

def dataframe_counts_excel_filtered(boolean_dataset_counts: pd.DataFrame, totals_dataset: pd.DataFrame, header_counts: Dict) -> Dict[str,pd.DataFrame]:
    boolean_dataset_counts.reset_index(inplace=True)
    boolean_dataset_counts.drop(columns=header_counts['drop'], inplace=True)
    boolean_dataset_counts_singles = boolean_dataset_counts.groupby(header_counts['index_reset']).max()
    boolean_dataset_counts_singles = totals_dataset[[header_counts["group_by"]] + [header_counts["reference_index"]]].merge(
        boolean_dataset_counts_singles, how='inner', left_index=True, right_index=True)
    counts = boolean_dataset_counts_singles.groupby([header_counts["group_by"]] + [header_counts["reference_index"]]).sum()
    excel_data_totals = {'counts': counts}
    excel_data_totals['Totals'] = boolean_dataset_counts_singles.groupby([header_counts["group_by"]] + [header_counts["reference_index"]])[header_counts["reference_index"]].count().to_frame()
    return excel_data_totals


def dataframe_time_annual(boolean_dataset: pd.DataFrame, refined_dataset: pd.DataFrame, header: Dict) -> pd.DataFrame:
    refined_dataset.reset_index(drop=True, inplace=True)
    yearly_columns = pd.to_numeric(boolean_dataset.columns, errors='coerce')
    year_list = list(yearly_columns[~yearly_columns.isna()].astype(int).astype(str))
    boolean_index = boolean_dataset.index
    boolean_dataset.reset_index(drop=True, inplace=True)
    annual_sums = boolean_dataset.sum(axis=1)

    boolean_subset = boolean_dataset.loc[annual_sums == 1].multiply(
        refined_dataset[header["time_delta"]].values[annual_sums == 1], axis=0) / 365
    boolean_dataset.loc[annual_sums == 1] = boolean_subset

    first_year = pd.to_datetime(refined_dataset[header['start_time']].loc[annual_sums > 1]).dt.year.astype(str)
    new_years_eve = first_year + "-12-31T23:59:59"
    days_firstyear = (pd.to_datetime(new_years_eve) - pd.to_datetime(
        refined_dataset[header['start_time']].loc[annual_sums > 1])) / np.timedelta64(1, 'D') / 365

    last_year = pd.to_datetime(refined_dataset[header['end_time']].loc[annual_sums > 1]).dt.year.astype(str)
    new_years_day = last_year + "-01-01T00:00:01"
    days_lastyear = (pd.to_datetime(refined_dataset[header['end_time']].loc[annual_sums > 1]) - pd.to_datetime(
        new_years_day)) / np.timedelta64(1, 'D') / 365

    boolean_dataset = boolean_dataset[year_list]
    first_year = first_year.astype(int)
    last_year = last_year.astype(int)
    for year in boolean_dataset.keys().astype(int):
        boolean_dataset.iloc[first_year[first_year == year].index, year - min(first_year)] = days_firstyear[
            first_year == year].values
        boolean_dataset.iloc[last_year[last_year == year].index, year - min(first_year)] = days_lastyear[
            last_year == year].values
    boolean_dataset = boolean_dataset.multiply(365)
    boolean_dataset.set_index(boolean_index, inplace=True)
    return boolean_dataset

