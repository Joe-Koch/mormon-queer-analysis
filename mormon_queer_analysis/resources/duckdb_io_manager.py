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
            return conn.query(body).to_df()
