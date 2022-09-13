from sqlite3 import Connection
from typing import List

from .column import Column


class DBMetaData:
    @classmethod
    def select_table_names(cls, connection: Connection) -> List[str]:
        cur = connection.execute(
            "select name from sqlite_master where type='table';")
        table_names = [s[0] for s in cur.fetchall()]
        return table_names

    @classmethod
    def select_columns_metadata(
            cls,
            connection: Connection,
            table_name: str) -> List[Column]:
        cur = connection.execute(f"PRAGMA table_info({table_name});")
        columns = list()
        for c in cur.fetchall():
            columns.append(Column(c[0], c[1], c[2], c[3], c[4], c[5]))
        return columns
