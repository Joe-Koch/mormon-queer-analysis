import os

from dagster import (
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    define_asset_job,
    load_assets_from_modules,
)

from .assets import reddit, open_ai

reddit_assets = load_assets_from_modules([reddit], group_name="Reddit")
open_ai_assets = load_assets_from_modules([open_ai], group_name="OpenAI")
all_assets = [*reddit_assets, *open_ai_assets]

reddit_data_job = define_asset_job(
    "reddit_data_pull_job", selection=AssetSelection.groups("Reddit")
)
open_ai_job = define_asset_job(
    "open_ai_api_job", selection=AssetSelection.groups("OpenAI")
)

# Set DAGSTER_DEPLOYMENT env var to "local" or "production" to specify which resource config to use.
resources = {
    "local": {
        "io_manager": FilesystemIOManager(
            base_dir="data",  # Path is built relative to where `dagster dev` is run
        ),
    },
    "production": {
        "io_manager": FilesystemIOManager(
            base_dir="data",
        ),
    },
}

deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    jobs=[reddit_data_job, open_ai_job],
    resources=resources[deployment_name],
)
