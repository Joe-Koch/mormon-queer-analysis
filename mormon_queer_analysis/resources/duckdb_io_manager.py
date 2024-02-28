from dagster import ConfigurableResource
import duckdb


class Database(ConfigurableResource):
    path: str

    def query(self, body: str):
        with duckdb.connect(self.path) as conn:
            result = conn.query(body)
            if result:
                return result.to_df()
            else:
                return
