"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import json_pandas, json_extender


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=json_pandas,
                 inputs="oops_platforms_region",
                 outputs="primary_raw_dataset",
                 name="oops_platforms_df"
                ),

            node(
                func=json_extender,
                inputs=["primary_raw_dataset","params:program"],
                outputs="program_dataset",
                name="program_compiler"
            ),
            node(
                func=json_extender,
                inputs=["program_dataset", "params:country"],
                outputs="country_dataset",
                name="program_country_compiler"
            ),
        ])
