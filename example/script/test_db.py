import os
import sqlite3
from sqlite3 import Connection


class TestDB:
    def __init__(self, dir: str = os.path.dirname(__file__),
                 filename: str = "test.db") -> None:
        self.filepath = os.path.join(dir, filename)

    def create(self) -> None:
        con = sqlite3.connect(self.filepath)
        self.__create_users_table(con)
        self.__create_products_table(con)
        self.__create_orders_table(con)
        con.commit()

    def __create_users_table(self, con: Connection) -> None:
        sql = '''CREATE TABLE IF NOT EXISTS users
        (
            id integer not null primary key,
            name text not null,
            phone text not null,
            address text not null
        )
        '''
        con.execute(sql)

    def __create_products_table(self, con: Connection) -> None:
        sql = '''CREATE TABLE IF NOT EXISTS products
        (
            id integer not null primary key,
            name text not null,
            amount integer not null
        )
        '''
        con.execute(sql)

    def __create_orders_table(self, con: Connection) -> None:
        sql = '''CREATE TABLE IF NOT EXISTS orders
        (
            id integer not null primary key,
            quantity integer not null,
            amount_total real not null,
            note text,
            user_id integer not null,
            product_id integer not null,
            foreign key (user_id) references users(id),
            foreign key (product_id) references products(id)
        )
        '''
        con.execute(sql)


def main():
    db = TestDB()
    db.create()


if __name__ == '__main__':
    main()
