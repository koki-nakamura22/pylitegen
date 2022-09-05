import contextlib
import sqlite3
from sqlite3 import Connection
from typing import Any, Final, List, Tuple, Type

from example.model import BaseModel


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
        params_str = '?, ' * len(model.member_names)
        params_str.rstrip().rstrip(',')
        or_ignore_str = " OR IGNORE" if insert_or_ignore else ''
        sql = f"INSERT{or_ignore_str} INTO {model.table_name} VALUES ({params_str})"
        return self.execute(sql, model.values)

    ###################

    def update(self):
        # Cannot use when there are no primary keys in a table
        pass

    def delete(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions do not match')
        sql = f"DELETE FROM {model_class.table_name} WHERE 1=1"
        param_list = list()
        for k in condition:
            sql += f" AND {k} = ?"
            param_list.append(condition[k])
        self.execute(sql, param_list)

    def delete_by_model(self, model: BaseModel):
        sql = f"DELETE FROM {model.table_name} WHERE 1=1"
        param_list = list()
        member_list = model.pk_names if 0 < len(
            model.pk_names) else model.member_names
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
