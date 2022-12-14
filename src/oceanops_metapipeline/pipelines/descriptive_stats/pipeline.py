"""
This is a boilerplate pipeline 'descriptive_stats'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import dataframe_tabulated, dataframe_merge_tabulate, dataframe_counts_excel_filtered, dataframe_time_annual, dataframe_counts_excel, yearly_variable_compiler, yearly_nonvariable_compiler


def create_pipeline(**kwargs) -> Pipeline:

    data_tabulate_days = pipeline([
        node(
            func=dataframe_merge_tabulate,
            inputs=["variable_refined_data","platform_ptfModel_ptfType_ptfFamily_merger","params:variable_group"],
            outputs="tabulated_variable_data",
            name="variable_tabulator"
        )])

    data_merge = pipeline([

        node(
            func=dataframe_tabulated,
            inputs=["merged_program_country","params:nation_group"],
            outputs="tabulated_nation_data",
            name="nation_tabulator"
        ),
        node(
            func=dataframe_tabulated,
            inputs=["merged_program_country", "params:program_group"],
            outputs="tabulated_program_data",
            name="program_program_tabulator"
        ),
        node(
            func=dataframe_tabulated,
            inputs=["platform_ptfModel_ptfType_ptfFamily_merger", "params:platform_group"],
            outputs="tabulated_platform_data",
            name="platform_tabulator"
        ),
        node(
            func=dataframe_time_annual,
            inputs=["boolean_yearly_variable","variable_refined_data","params:boolean_time"],
            outputs="boolean_daily_variable",
            name="tabulate_days_by_year"
        )
    ])


    yearly_tabulate = pipeline([

        node(
            func=dataframe_counts_excel,
            inputs=["boolean_yearly_variable","boolean_daily_variable","tabulated_variable_data",
                    "params:boolean_counts_variables","params:boolean_days_variables"],
            outputs="tabulated_variable_data_annual_counts",
            name="annual_variable_counter"
        ),
        node(
            func=dataframe_counts_excel_filtered,
            inputs=["boolean_yearly_variable", "merged_program_country",
                    "params:boolean_counts_country"],
            outputs="tabulated_countries_data_annual_counts",
            name="annual_country_counter"
        ),
        node(
            func=dataframe_counts_excel_filtered,
            inputs=["boolean_yearly_variable", "merged_program_country",
                    "params:boolean_counts_program"],
            outputs="tabulated_programs_data_annual_counts",
            name="annual_program_counter"
        ),
        node(
            func=dataframe_counts_excel_filtered,
            inputs=["boolean_yearly_variable", "platform_ptfModel_ptfType_ptfFamily_merger",
                    "params:boolean_counts_platform"],
            outputs="tabulated_platform_data_annual_counts",
            name="annual_platform_counter"
        ),
        node(func=yearly_variable_compiler,
             inputs=["boolean_yearly_variable","variable_refined_data","platform_ptfModel_ptfType_ptfFamily_merger",
                     "merged_program_country","params:header_variable_compiler"],
             outputs='variable_list_by_year',
             name='variable_by_years'),
        node(func=yearly_nonvariable_compiler,
             inputs=["variable_refined_data","merged_program_country","platform_ptfModel_ptfType_ptfFamily_merger",
                     "primary_raw_dataset","params:header_variable_compiler"],
             outputs='unknown_variable_list_by_year',
             name='annual_nonvariable_counter')
    ])

    return data_tabulate_days + data_merge + yearly_tabulate