import json
import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from mormon_queer_analysis.resources import FILTER_KEYWORDS


# TODO This way of finding directories is gross
DBT_PROJECT_DIR = (
    Path(__file__).joinpath("..", "..", "..", "mormon_queer_analysis_dbt").resolve()
)
# MANIFEST_FILE = DBT_PROJECT_DIR.joinpath("target", "manifest.json")

dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = DBT_PROJECT_DIR.joinpath("target", "manifest.json")


@dbt_assets(manifest=dbt_manifest_path)
def dagster_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    lgbt_keywords_condition = " OR ".join(
        [f"LOWER(text) LIKE '%{keyword.lower()}%'" for keyword in FILTER_KEYWORDS]
    )
    dbt_vars = {"lgbt_keywords_condition": lgbt_keywords_condition}
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], context=context
    ).stream()
