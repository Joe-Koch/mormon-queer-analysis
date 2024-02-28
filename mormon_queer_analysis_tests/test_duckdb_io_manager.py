import os
import uuid
from contextlib import contextmanager
from typing import Iterator

import pytest
from dagster import AssetKey, asset, build_input_context, build_output_context
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster_duckdb_pandas import DuckDBPandasIOManager
from pandas import DataFrame as PandasDataFrame

from mormon_queer_analysis.resources.duckdb_io_manager import Database


def mock_output_context(asset_key: AssetKey) -> OutputContext:
    @asset(name=asset_key.path[-1], key_prefix=asset_key.path[:-1])
    def my_asset():
        pass

    return build_output_context(op_def=my_asset.op, name="result", asset_key=asset_key)


def mock_input_context(upstream_output_context: OutputContext) -> InputContext:
    return build_input_context(
        upstream_output=upstream_output_context,
        name=upstream_output_context.name,
        asset_key=upstream_output_context.asset_key,
    )


@contextmanager
def temporary_duckdb_table(
    contents: PandasDataFrame, database: Database
) -> Iterator[AssetKey]:
    schema = "reddit"
    table_name = "a" + str(uuid.uuid4()).replace("-", "_")

    database.query(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM {contents};")
    try:
        yield AssetKey([schema, table_name])
    finally:
        database.query(f"DROP TABLE {schema}.{table_name};")


@pytest.mark.skipif(
    os.environ.get("TEST_DUCKDB") != "true",
    reason="avoid dependency on DuckDB for tests",
)
def test_handle_output_then_load_input_pandas():
    duckdb_manager = DuckDBPandasIOManager(
        database="database/test.duckdb", schema="TEST"
    )
    contents1 = PandasDataFrame(
        [{"col1": "a", "col2": 1}]
    )  # just to get the types right
    contents2 = PandasDataFrame([{"col1": "b", "col2": 2}])  # contents we will insert
    with temporary_duckdb_table(contents1) as temp_table_key:
        output_context = mock_output_context(asset_key=temp_table_key)
        duckdb_manager.handle_output(output_context, contents2)

        input_context = mock_input_context(output_context)
        input_value = duckdb_manager.load_input(input_context)
        assert input_value.equals(contents2), f"{input_value}\n\n{contents2}"
