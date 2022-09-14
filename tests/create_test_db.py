import os
import sqlite3
from sqlite3 import Connection


class DBForTestCreator:
    def __init__(self, dir: str = os.path.dirname(__file__),
                 filename: str = "test.db") -> None:
        self.filepath = os.path.join(dir, filename)

    def create(self) -> None:
        con = sqlite3.connect(self.filepath)
        self.__create_users_table(con)
        self.__create_user_edited_histories_table(con)
        self.__create_all_optional_columns_table(con)
        self.__create_products_table(con)
        con.commit()

    def __create_users_table(self, con: Connection) -> None:
        sql = """CREATE TABLE IF NOT EXISTS users
        (
            id integer not null primary key,
            name text not null,
            phone text not null,
            address text
        )
        """
        con.execute(sql)

    def __create_user_edited_histories_table(self, con: Connection) -> None:
        sql = """CREATE TABLE IF NOT EXISTS user_edited_histories
        (
            datetime text not null,
            note text
        )
        """
        con.execute(sql)

    def __create_all_optional_columns_table(self, con: Connection) -> None:
        sql = """CREATE TABLE IF NOT EXISTS all_optional_columns
        (
            col1 text,
            col2 integer,
            col3 real
        )
        """
        con.execute(sql)

    def __create_products_table(self, con: Connection) -> None:
        sql = """CREATE TABLE IF NOT EXISTS backup_users
        (
            id integer not null primary key,
            name text,
            phone text,
            address text
        )
        """
        con.execute(sql)


def main():
    db = DBForTestCreator()
    db.create()


if __name__ == '__main__':
    main()
