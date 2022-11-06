"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import json_pandas, json_extender, df_merge, varaiable_extender, dataframe_refinement, boolean_variable_extension


def create_pipeline(**kwargs) -> Pipeline:
    program_pipeline = pipeline(
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
            node(
                func=df_merge,
                inputs=["program_dataset", "country_dataset","params:country"],
                outputs="merged_program_country",
                name="program_country_merger"
            ),
        ])

    platform_pipeline = pipeline(
        [
            node(
                func=json_extender,
                inputs=["primary_raw_dataset","params:platform_model"],
                outputs="platform_model_dataset",
                name="platform_model_compiler"
            ),
            node(
                func=json_extender,
                inputs=["platform_model_dataset", "params:ptfModel_ptfType"],
                outputs="platform_ptfModel_ptfType",
                name="ptfModel_ptfType_compiler"
            ),
            node(
                func=json_extender,
                inputs=["platform_ptfModel_ptfType", "params:ptfModel_ptfType_ptfFamily"],
                outputs="platform_ptfModel_ptfType_ptfFamily",
                name="ptfModel_ptfType_ptfFamily_compiler"
            ),
            node(
                func=df_merge,
                inputs=["platform_model_dataset", "platform_ptfModel_ptfType","params:ptfModel_ptfType"],
                outputs="platform_ptfModel_ptfType_merger",
                name="ptfModel_ptfType_dataset_merger"
            ),
            node(
                func=df_merge,
                inputs=["platform_ptfModel_ptfType_merger", "platform_ptfModel_ptfType_ptfFamily", "params:ptfModel_ptfType_ptfFamily"],
                outputs="platform_ptfModel_ptfType_ptfFamily_merger",
                name="platform_ptfModel_ptfType_ptfFamily_merger"
            ),
        ])

    variable_pipeline = pipeline([
        node(
            func=varaiable_extender,
            inputs=["primary_raw_dataset", "params:ptfVariables"],
            outputs="variable_dataset",
            name="variable_dataset_compiler"
        ),
        node(
            func=dataframe_refinement,
            inputs=["variable_dataset","variable_reference_table","params:df_refinement"],
            outputs=["variable_refined_data","tabulated_variable_data"],
            name="variable_dataset_refiner"
        ),
        node(
            func=boolean_variable_extension,
            inputs="variable_refined_data",
            outputs="boolean_yearly_variable",
            name="boolean_years_for_variables"
        )
    ])
    return program_pipeline + platform_pipeline + variable_pipeline
