"""
This is a boilerplate pipeline 'descriptive_stats'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import dataframe_tabulated, dataframe_merge_tabulate


def create_pipeline(**kwargs) -> Pipeline:

    return pipeline([
        # node(
        #     func=dataframe_tabulated,
        #     inputs=["variable_refined_data","params:variable_group"],
        #     outputs="tabulated_variable_data",
        #     name="variable_tabulator"
        # ),
        node(
            func=dataframe_merge_tabulate,
            inputs=["variable_refined_data","platform_ptfModel_ptfType_ptfFamily_merger","params:variable_group"],
            outputs="tabulated_variable_data",
            name="variable_tabulator"
        ),
        node(
            func=dataframe_tabulated,
            inputs=["merged_program_country","params:program_country_group"],
            outputs="tabulated_program_data",
            name="program_country_tabulator"
        ),
        node(
            func=dataframe_tabulated,
            inputs=["platform_ptfModel_ptfType_ptfFamily_merger", "params:platform_group"],
            outputs="tabulated_platform_data",
            name="platform_tabulator"
        )
    ])
