from duckdb import DuckDBPyConnection
import numpy.core.multiarray # noqa, see https://duckdb.org/docs/archive/0.9.2/api/python/known_issues#numpy-import-multithreading

from dbt.adapters.duckdb.plugins import BasePlugin


def phone_numberize(name: str) -> str:
    """Convert the name to a phone number based on the letters on a phone keypad."""
    name = name.lower()
    phone_number = ""
    for letter in name:
        if letter in "abc":
            phone_number += "2"
        elif letter in "def":
            phone_number += "3"
        elif letter in "ghi":
            phone_number += "4"
        elif letter in "jkl":
            phone_number += "5"
        elif letter in "mno":
            phone_number += "6"
        elif letter in "pqrs":
            phone_number += "7"
        elif letter in "tuv":
            phone_number += "8"
        elif letter in "wxyz":
            phone_number += "9"
        else:
            phone_number += letter
    return phone_number


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("phone_numberize", phone_numberize)
