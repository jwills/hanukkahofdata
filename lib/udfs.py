from duckdb import DuckDBPyConnection

from dbt.adapters.duckdb.plugins import BasePlugin


def phone_numberize(name: str) -> str:
    return "1729"


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("phone_numberize", phone_numberize)
