#######################################
# References
# --------------
# - Rails Active Record Query Interface
#   https://guides.rubyonrails.org/active_record_querying.html
#
# - 【Laravel】Eloquentを使ったモデルを理解する
#   https://bonoponz.hatenablog.com/entry/2020/10/06/%E3%80%90Laravel%E3%80%91Eloquent%E3%82%92%E4%BD%BF%E3%81%A3%E3%81%9F%E3%83%A2%E3%83%87%E3%83%AB%E3%82%92%E7%90%86%E8%A7%A3%E3%81%99%E3%82%8B
#
# - Python SQLiteでデータベース接続を閉じない場合
#   https://www.web-dev-qa-db-ja.com/ja/python/python-sqlite%E3%81%A7%E3%83%87%E3%83%BC%E3%82%BF%E3%83%99%E3%83%BC%E3%82%B9%E6%8E%A5%E7%B6%9A%E3%82%92%E9%96%89%E3%81%98%E3%81%AA%E3%81%84%E5%A0%B4%E5%90%88/942231172/
#######################################


import contextlib
import sqlite3
from sqlite3 import Connection
from typing import Final


class DB:
    def __init__(self, db_filepath: str) -> None:
        self.db_filepath: Final[str] = db_filepath
        self.con: Final[Connection] = sqlite3.connect(db_filepath)

    def commit(self):
        self.con.commit()

    def rollback(self):
        self.con.rollback()

    def close(self):
        self.con.close()

    ###################
    # Select
    ###################
    def find(self):
        pass

    # Rails
    def find_by(self):
        pass

    def where(self):
        pass
    ###################

    ###################
    # Insert
    ###################
    # Rails
    def create(self):
        pass

    def save(self):
        pass
    ###################

    def update(self):
        pass

    def delete(self):
        pass

    def execute(self):
        pass

    @contextlib.contextmanager
    def transaction_scope(self):
        connection_for_transaction = self.__class__(self.db_filepath)
        with contextlib.closing(connection_for_transaction.con) as tran:
            try:
                yield tran
            finally:
                tran.rollback()
                tran.close()
