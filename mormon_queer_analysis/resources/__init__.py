from importlib import resources
import os

from dagster import EnvVar, FilesystemIOManager, resource
from dagster._utils import file_relative_path
from dagster_dbt import DbtCliResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from mormon_queer_analysis.partitions import reddit_partitions_def
from mormon_queer_analysis.resources.duckdb_io_manager import Database
from mormon_queer_analysis.resources.open_client import (
    OpenAIClientResource,
    OpenAISubsampleClientResource,
)


DBT_PROJECT_DIR = file_relative_path(__file__, "../../mormon_queer_analysis_dbt")
DUCKDB_DIR = os.path.join(DBT_PROJECT_DIR, "dagster.duckdb")
dbt_local_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    target="local",
)
dbt_production_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    target="production",
)


FILTER_KEYWORDS = resources.read_text(
    "mormon_queer_analysis.resources", "filter_keywords.txt"
).splitlines()


@resource({"start_date": str, "end_date": str})
def reddit_partitions_resource(context):
    return reddit_partitions_def(
        start_date=context.resource_config["start_date"],
        end_date=context.resource_config["end_date"],
    )


RESOURCES_LOCAL = {
    "io_manager": FilesystemIOManager(
        base_dir=DBT_PROJECT_DIR,  # Path is built relative to where `dagster dev` is run
    ),
    "reddit_partitions": reddit_partitions_resource.configured(
        {
            "start_date": EnvVar("START_DATE"),
            "end_date": EnvVar("END_DATE"),
        }  # Use a smaller subset of time
    ),
    "open_ai_client": OpenAISubsampleClientResource(
        openai_api_key=EnvVar("OPENAI_API_KEY")
    ),  # Reduce costs by subsampling how much data you send to the API
    "dbt": dbt_local_resource,
}


RESOURCES_PRODUCTION = {
    "io_manager": DuckDBPandasIOManager(
        database=DUCKDB_DIR,
        schema="REDDIT",
    ),
    "database": Database(path=DUCKDB_DIR),
    "reddit_partitions": reddit_partitions_resource.configured(
        {
            "start_date": EnvVar("START_DATE"),
            "end_date": EnvVar("END_DATE"),
        }
    ),
    "open_ai_client": OpenAIClientResource(openai_api_key=EnvVar("OPENAI_API_KEY")),
    "dbt": dbt_production_resource,
}
