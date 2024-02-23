from dagster import ConfigurableResource
from dagster_duckdb_pandas import DuckDBPandasIOManager
import duckdb

database_io_manager = DuckDBPandasIOManager(
    database="database/dagster.duckdb", schema="REDDIT"
)


class Database(ConfigurableResource):
    path: str

    def query(self, body: str):
        with duckdb.connect(self.path) as conn:
            result = conn.query(body)
            if result:
                return result.to_df()
            else:
                return
