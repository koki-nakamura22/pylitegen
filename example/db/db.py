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
from typing import Any, Final, List, Tuple, Type

from example.model.model import BaseModel


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

    def __check_condition(
            self,
            model: Type[BaseModel],
            condition: dict) -> bool:
        model_mambers = list(filter(
            lambda a: a not in [
                'table_name',
                'pk_names'],
            model.__annotations__))
        for k in condition.keys():
            if k not in model_mambers:
                return False
        return True

    def __check_specify_pk(
            self,
            model_type: Type[BaseModel],
            condition: dict) -> bool:
        return set(model_type.pk_names) == set(condition.keys())

    ###################
    # Select
    ###################

    def __make_select(self,
                      model_class: Type[BaseModel],
                      condition: dict) -> Tuple[str,
                                                List]:
        sql = f"SELECT * FROM {model_class.table_name} WHERE 1=1"
        param_list = list()
        for k in condition:
            sql += f" AND {k} = ?"
            param_list.append(condition[k])
        return sql, param_list

    def __find(self, model_class: Type[BaseModel], condition: dict):
        sql, param_list = self.__make_select(model_class, condition)
        return self.execute(sql, param_list).fetchone()

    def find(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_specify_pk(model_class, condition):
            raise ValueError('Primary keys do not match')
        return self.__find(model_class, condition)

    def find_by(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions do not match')
        return self.__find(model_class, condition)

    def where(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions do not match')
        sql, param_list = self.__make_select(model_class, condition)
        return self.execute(sql, param_list).fetchall()

    ###################

    ###################
    # Insert
    ###################
    def insert(self, model: BaseModel, insert_or_ignore: bool = True):
        params_str = '?, ' * len(model.member_names_as_list())
        params_str.rstrip().rstrip(',')
        or_ignore_str = " OR IGNORE" if insert_or_ignore else ''
        sql = f"INSERT{or_ignore_str} INTO {model.table_name} VALUES ({params_str})"
        return self.execute(sql, model.values_as_list())

    ###################

    def update(self):
        pass

    def delete(self):
        pass

    def delete_by_model(self, model: BaseModel):
        sql = f"DELETE FROM {model.table_name} WHERE 1=1"
        param_list = list()
        member_list = model.pk_names if 0 < len(
            model.pk_names) else model.member_names_as_list()
        for member_name in member_list:
            sql += f" AND {member_name} = ?"
            param_list.append(getattr(model, member_name))
        return self.execute(sql, param_list)

    def execute(self, sql: str, params: List):
        return self.con.execute(sql, params)

    @ contextlib.contextmanager
    def transaction_scope(self):
        connection_for_transaction = self.__class__(self.db_filepath)
        with contextlib.closing(connection_for_transaction.con) as tran:
            try:
                yield tran
            finally:
                tran.rollback()
                tran.close()
