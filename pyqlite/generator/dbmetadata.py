from sqlite3 import Connection
from typing import List

from .column import Column


class DBMetaData:
    """SQLite DB metadata accessor.
    """

    @classmethod
    def select_table_names(cls, connection: Connection) -> List[str]:
        """Get table names from database.

        Parameters
        ----------
        connection : Connection
            SQLite database connection

        Returns
        -------
        List[str]
            Table names
        """
        cur = connection.execute(
            "select name from sqlite_master where type='table';")
        table_names = [s[0] for s in cur.fetchall()]
        return table_names

    @classmethod
    def select_columns_metadata(
            cls,
            connection: Connection,
            table_name: str) -> List[Column]:
        """Get column data from the specified table.

        Parameters
        ----------
        connection : Connection
            SQLite database connection
        table_name : str
            The target table name

        Returns
        -------
        List[Column]
            Column data
        """
        cur = connection.execute(f"PRAGMA table_info({table_name});")
        columns = list()
        for c in cur.fetchall():
            columns.append(Column(c[0], c[1], c[2], c[3], c[4], c[5]))
        return columns
