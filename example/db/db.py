import contextlib
from logging import getLogger
import sqlite3
from sqlite3 import Connection
from typing import Final, List, Optional, Type, Union

import example.log
from example.db.querybuilder import QueryBuilder
from example.model import BaseModel


class DB:
    log_level: Optional[int] = None

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

    ###################
    # Select
    ###################
    def find(self, model_class: Type[BaseModel], *primary_key_values):
        pks = model_class.get_pks()
        if len(pks) == 0:
            raise ValueError(
                'Cannot use find method because this class does not have any primary keys')
        if len(primary_key_values) != len(model_class.get_pks()):
            raise ValueError(
                'The number of primary keys and primary key values do not match')
        sql = QueryBuilder.build_select_with_qmark_parameters(model_class, pks)
        r = self.execute(sql, list(primary_key_values)).fetchone()
        return None if r is None else model_class.get_class_type()(*r)

    def find_by(self,
                model_class: Type[BaseModel],
                where: Optional[str] = None,
                params: Optional[Union[dict,
                                       List]] = None):
        if (where is None and params is not None) or (
                where is not None and params is None):
            raise ValueError(
                'Both where and values must be passed, or not passed both')
        if where is not None and params is not None:
            sql = QueryBuilder.build_select(model_class, where)
            r = self.execute(sql, params).fetchone()
        else:
            sql = QueryBuilder.build_select(model_class)
            r = self.execute(sql).fetchone()
        return None if r is None else model_class.get_class_type()(*r)

    def where(self,
              model_class: Type[BaseModel],
              where: Optional[str] = None,
              params: Optional[Union[dict,
                                     List]] = None):
        if (where is None and params is not None) or (
                where is not None and params is None):
            raise ValueError(
                'Both where and values must be passed, or not passed both')

        # TODO: fetchall or fetchmany
        if where is not None and params is not None:
            sql = QueryBuilder.build_select(model_class, where)
            r = self.execute(sql, params).fetchall()
        else:
            sql = QueryBuilder.build_select(model_class)
            r = self.execute(sql).fetchall()
        model_list = []
        for o in r:
            model_list.append(model_class.get_class_type()(*o))
        return model_list

    ###################
    # Insert
    ###################
    def insert(self, model: BaseModel, insert_or_ignore: bool = True) -> int:
        sql, param_list = QueryBuilder.build_insert(model, insert_or_ignore)
        return self.execute(sql, param_list).rowcount

    def bulk_insert(
            self,
            models: List,
            insert_or_ignore: bool = True) -> int:
        if not all(hasattr(m, 'class_type') for m in models):
            # BaseModel class has class_type attribute.
            raise ValueError(
                'All parameter models must be inherited BaseModel')

        if not all(m.class_type == models[0].class_type for m in models):
            raise ValueError('Multiple types of models cannot be specified')

        sql, param_list = QueryBuilder.build_bulk_insert(
            models, insert_or_ignore)
        return self.execute(sql, param_list).rowcount

    ###################
    # Update
    ###################
    def update(self,
               model_class: Type[BaseModel],
               data_to_be_updated: dict,
               where: Optional[str] = None,
               condition: Optional[Union[dict, List]] = None) -> int:
        if (where is None and condition is not None) or (
                where is not None and condition is None):
            raise ValueError(
                'Both where and values must be passed, or not passed both')

        sql = QueryBuilder.build_update(
            model_class, data_to_be_updated, where, condition)
        if condition is None:
            return self.execute(sql, data_to_be_updated).rowcount
        else:
            if isinstance(condition, dict):
                params = {}
                params.update(data_to_be_updated)
                params.update(condition)
            elif isinstance(condition, list):
                params = list()
                params.extend(list(data_to_be_updated.values()))
                params.extend(condition)
            return self.execute(sql, params).rowcount

    def update_by_model(self, model: BaseModel) -> int:
        pks = model.pks
        if len(pks) == 0:
            raise ValueError(
                'Cannot use this function with no primary key model')

        sql = QueryBuilder.build_update_by_model(model)
        params = getattr(
            model, '_BaseModel__get_data_to_be_updated')()
        for pk in pks:
            params.update({pk: getattr(model, pk)})
        r = self.execute(sql, params)
        if 0 < r.rowcount:
            model._BaseModel__set_cache()  # type: ignore
        return r.rowcount

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

    def execute(self, sql: str, params: Optional[Union[dict, List]] = None):
        r = self.con.execute(
            sql) if params is None else self.con.execute(sql, params)

        if self.log_level is not None:
            logger = getLogger(self.__class__.__name__)
            logger.setLevel(self.log_level)
            msg = 'sql executed: ' + sql
            if params is not None and 0 < len(params):
                msg += ": " + ", ".join(params)
            logger.info(msg)

        return r

    @contextlib.contextmanager
    def transaction_scope(self):
        connection_for_transaction = self.__class__(self.db_filepath)
        with contextlib.closing(connection_for_transaction) as tran:
            try:
                yield tran
            finally:
                tran.rollback()
                tran.close()
