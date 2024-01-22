from dagster import (
    EnvVar,
    Field,
    FilesystemIOManager,
    MonthlyPartitionsDefinition,
    resource,
)

from mormon_queer_analysis.resources.open_client import OpenAIClientResource


with open("mormon_queer_analysis/resources/filter_keywords.txt", "r") as file:
    FILTER_KEYWORDS = [line.strip() for line in file.readlines()]


@resource({"start_date": Field(str), "end_date": Field(str)})
def monthly_partitions_resource(context):
    return MonthlyPartitionsDefinition(
        start_date=context.resource_config["start_date"],
        end_date=context.resource_config["end_date"],
    )


RESOURCES_LOCAL = {
    "io_manager": FilesystemIOManager(
        base_dir="data",  # Path is built relative to where `dagster dev` is run
    ),
    "monthly_partitions": monthly_partitions_resource.configured(
        {"start_date": "2023-01-01", "end_date": "2023-03-01"}
    ),
    "open_ai_client": OpenAIClientResource(openai_api_key=EnvVar("OPENAI_API_KEY")),
}


RESOURCES_PRODUCTION = {
    "io_manager": FilesystemIOManager(
        base_dir="data",  # Path is built relative to where `dagster dev` is run
    ),
    "monthly_partitions": monthly_partitions_resource.configured(
        {"start_date": "2005-06-01", "end_date": "2023-03-01"}
    ),
    "open_ai_client": OpenAIClientResource(openai_api_key=EnvVar("OPENAI_API_KEY")),
}
