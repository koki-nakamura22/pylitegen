import contextlib
import sqlite3
from sqlite3 import Connection
from typing import Any, Final, List, Optional, Tuple, Type

from example.db.querybuilder import QueryBuilder
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
        model_members = model.get_member_names()
        for k in condition.keys():
            if k not in model_members:
                return False
        return True

    def __check_specify_pk(
            self,
            model_type: Type[BaseModel],
            condition: dict) -> bool:
        return set(model_type.pks) == set(condition.keys())

    ###################
    # Select
    ###################

    def __find(self, model_class: Type[BaseModel], condition: dict):
        sql, param_list = QueryBuilder.build_select(model_class, condition)
        r = self.execute(sql, param_list).fetchone()
        return None if r is None else model_class.get_class_type()(*r)

    def find(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_specify_pk(model_class, condition):
            raise ValueError('Primary keys do not match')
        return self.__find(model_class, condition)

    def find_by(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions and columns do not match')
        return self.__find(model_class, condition)

    def where(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions and columns do not match')
        sql, param_list = QueryBuilder.build_select(model_class, condition)
        # TODO: fetchall or fetchmany
        where_result = self.execute(sql, param_list).fetchall()
        model_list = []
        for o in where_result:
            model_list.append(model_class.get_class_type()(*o))
        return model_list

    ###################

    ###################
    # Insert
    ###################
    def insert(self, model: BaseModel, insert_or_ignore: bool = True):
        sql, param_list = QueryBuilder.build_insert(model, insert_or_ignore)
        return self.execute(sql, param_list)

    ###################

    ###################
    # Update
    ###################
    def update(self,
               model_class: Type[BaseModel],
               data_to_be_updated: dict,
               condition: dict):
        sql, param_list = QueryBuilder.build_update(
            model_class, data_to_be_updated, condition)
        return self.execute(sql, param_list).rowcount

    def update_by_model(self, model: BaseModel):
        if len(model.__class__.pks) == 0:
            raise ValueError(
                'Cannot use this function with no primary key model')

        sql, param_list = QueryBuilder.build_update_by_model(model)
        r = self.execute(sql, param_list)
        if 0 < r.rowcount:
            model._BaseModel__set_cache()  # type: ignore
        return r.rowcount
    ###################

    ###################
    # Delete
    ###################
    def delete(self, model_class: Type[BaseModel], condition: dict):
        if not self.__check_condition(model_class, condition):
            raise ValueError('Conditions do not match')
        sql, param_list = QueryBuilder.build_delete(model_class, condition)
        return self.execute(sql, param_list).rowcount

    def delete_by_model(self, model: BaseModel):
        sql, param_list = QueryBuilder.build_delete_by_model(model)
        return self.execute(sql, param_list).rowcount
    ###################

    def execute(self, sql: str, params: Optional[List] = None):
        if params is None:
            return self.con.execute(sql)
        else:
            return self.con.execute(sql, params)

    @contextlib.contextmanager
    def transaction_scope(self):
        connection_for_transaction = self.__class__(self.db_filepath)
        with contextlib.closing(connection_for_transaction) as tran:
            try:
                yield tran
            finally:
                tran.rollback()
                tran.close()
