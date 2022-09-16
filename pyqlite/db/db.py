import contextlib
from logging import getLogger
import sqlite3
from sqlite3 import Connection
from typing import Final, List, Optional, Type, Union

import pyqlite.log
from pyqlite.db.isolation_level import IsolationLevel
from pyqlite.db.querybuilder import QueryBuilder
from pyqlite.model import BaseModel


class DB:
    """SQLite client wrapper
    """

    log_level: Optional[int] = None

    def __init__(
            self,
            db_filepath: str,
            isolation_level: IsolationLevel = IsolationLevel.DEFERRED) -> None:
        """Constructor

        Parameters
        ----------
        db_filepath : str
            Database file path
        isolation_level : IsolationLevel, optional
            Isolation level, by default IsolationLevel.DEFERRED
        """
        self.db_filepath: Final[str] = db_filepath
        self.con: Final[Connection] = sqlite3.connect(
            db_filepath, isolation_level=isolation_level.value)

    def commit(self):
        """Commit
        """
        self.con.commit()

    def rollback(self):
        """Rollback
        """
        self.con.rollback()

    def close(self):
        """Close database
        """
        self.con.close()

    ###################
    # Validation
    ###################
    def __validate_where_and_condition(
            self, where: Optional[str], condition: Optional[Union[dict, List]] = None):
        if (where is None and condition is not None) or (
                where is not None and condition is None):
            raise ValueError(
                'Both where and values must be passed, or not passed both')

    ###################
    # Select
    ###################
    def find(self, model_class: Type[BaseModel], *primary_key_values):
        """Find a data by primary keys.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        primary_key_values
            Primary key values

        Returns
        -------
        Optional[Model Type]
            Found model data
            When not found, return None

        Raises
        ------
        ValueError
            Raises ValueError if the model does not have any primary keys
        ValueError
            Raises ValueError if the number of primary key values parameters and the number of primary keys model has
        """
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
                where_params: Optional[Union[dict,
                                       List]] = None):
        """Find a data by specified parameters.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        where : Optional[str], optional
            Where clause str, by default None
        where_params : Optional[Union[dict, List]], optional
            Parameters for where clause, by default None

        Returns
        -------
        _type_
            Found model data
            When not found, return None

        Raises
        ------
        ValueError
            Raises ValueError if only where or where_params is specified
        """
        self.__validate_where_and_condition(where, where_params)
        if where is not None and where_params is not None:
            sql = QueryBuilder.build_select(model_class, where)
            r = self.execute(sql, where_params).fetchone()
        else:
            sql = QueryBuilder.build_select(model_class)
            r = self.execute(sql).fetchone()
        return None if r is None else model_class.get_class_type()(*r)

    def where(self,
              model_class: Type[BaseModel],
              where: Optional[str] = None,
              where_params: Optional[Union[dict,
                                     List]] = None):
        """Find data by specified parameters.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        where : Optional[str], optional
            Where clause str, by default None
        where_params : Optional[Union[dict, List]], optional
            Parameters for where clause, by default None

        Returns
        -------
        _type_
            Found data
            When not found, return an empty list

        Raises
        ------
        ValueError
            Raises ValueError if only where or where_params is specified
        """
        self.__validate_where_and_condition(where, where_params)

        # TODO: fetchall or fetchmany
        if where is not None and where_params is not None:
            sql = QueryBuilder.build_select(model_class, where)
            r = self.execute(sql, where_params).fetchall()
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
        """Insert a data by model.

        Parameters
        ----------
        model : BaseModel
            Target model
        insert_or_ignore : bool, optional
            INSERT OR IGNORE flag
            True: INSERT OR IGNORE
            False: not INSERT OR IGNORE
            , by default True

        Returns
        -------
        int
            Inserted rows count
        """
        sql, param_list = QueryBuilder.build_insert(model, insert_or_ignore)
        return self.execute(sql, param_list).rowcount

    def bulk_insert(
            self,
            models: List,
            insert_or_ignore: bool = True) -> int:
        """Bulk insert data by model list.

        Parameters
        ----------
        models : List
            Target model list
        insert_or_ignore : bool, optional
            INSERT OR IGNORE flag
            True: INSERT OR IGNORE
            False: not INSERT OR IGNORE
            , by default True

        Returns
        -------
        int
            Inserted rows count

        Raises
        ------
        ValueError
            Raises ValueError if the model list contains an object that does not inherit BaseModel class
        ValueError
            Raises ValueError if all model type does not match in the model list
        """
        if not all(hasattr(m, 'class_type') for m in models):
            # BaseModel class has class_type attribute.
            raise ValueError(
                'All parameter models must be inherited BaseModel')

        if not all(m.class_type == models[0].class_type for m in models):
            raise ValueError('Multiple types of models cannot be specified')

        sql, param_list = QueryBuilder.build_bulk_insert(
            models, insert_or_ignore)
        return self.executemany(sql, param_list).rowcount

    ###################
    # Update
    ###################
    def update(self,
               model_class: Type[BaseModel],
               data_to_be_updated: dict,
               where: Optional[str] = None,
               where_params: Optional[Union[dict, List]] = None) -> int:
        """Update a data.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        data_to_be_updated : dict
            Data to be updated
        where : Optional[str], optional
            Where clause str, by default None
        where_params : Optional[Union[dict, List]], optional
            Parameters for where clause, by default None

        Returns
        -------
        int
            Updated rows count

        Raises
        ------
        ValueError
            Raises ValueError if only where or where_params is specified
        """
        self.__validate_where_and_condition(where, where_params)

        sql = QueryBuilder.build_update(
            model_class, data_to_be_updated, where, where_params)
        if where_params is None:
            return self.execute(sql, data_to_be_updated).rowcount
        else:
            if isinstance(where_params, dict):
                params_for_execute = {}
                params_for_execute.update(data_to_be_updated)
                params_for_execute.update(where_params)
            elif isinstance(where_params, list):
                params_for_execute = list()
                params_for_execute.extend(list(data_to_be_updated.values()))
                params_for_execute.extend(where_params)
            return self.execute(sql, params_for_execute).rowcount

    def update_by_model(self, model: BaseModel) -> int:
        """Update a data by model.

        Parameters
        ----------
        model : BaseModel
            Target model

        Returns
        -------
        int
            Updated rows count

        Raises
        ------
        ValueError
            Raises ValueError if the model does not have any primary keys
        """
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
    def delete(
            self,
            model_class: Type[BaseModel],
            where: Optional[str] = None,
            where_params: Optional[Union[dict, List]] = None):
        """Delete a data.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        where : Optional[str], optional
            Where clause str, by default None
        where_params : Optional[Union[dict, List]], optional
            Parameters for where clause, by default None

        Returns
        -------
        int
            Deleted rows count

        Raises
        ------
        ValueError
            Raises ValueError if only where or where_params is specified
        """
        self.__validate_where_and_condition(where, where_params)

        sql = QueryBuilder.build_delete(model_class, where)
        return self.execute(sql, where_params).rowcount

    def delete_by_model(self, model: BaseModel):
        """Delete a data by model.

        Parameters
        ----------
        model : BaseModel
            Target model

        Returns
        -------
        int
            Deleted rows count
        """
        sql = QueryBuilder.build_delete_by_model(model)

        pks = model.pks
        if 0 < len(pks):
            params = {}
            for pk in pks:
                params[pk] = getattr(model, pk)
        else:
            params = model.to_dict()

        return self.execute(sql, params).rowcount

    ###################
    # Execute
    ###################
    def execute(self, sql: str, params: Optional[Union[dict, List]] = None):
        """Execute SQL.

        Parameters
        ----------
        sql : str
            SQL
        params : Optional[Union[dict, List]], optional
            parameters, by default None

        Returns
        -------
        Cursor
            SQL result
        """
        r = self.con.execute(
            sql) if params is None else self.con.execute(sql, params)

        if self.log_level is not None:
            logger = getLogger(self.__class__.__name__)
            logger.setLevel(self.log_level)
            msg = 'sql executed: ' + sql
            if params is not None:
                msg += f", params: {str(params)}"
            logger.info(msg)

        return r

    def executemany(self, sql: str, param_list: List):
        """Execute SQL.

        Parameters
        ----------
        sql : str
            SQL
        param_list : List
            Parameter List

        Returns
        -------
        Cursor
            SQL Result
        """
        r = self.con.executemany(sql, param_list)

        if self.log_level is not None:
            logger = getLogger(self.__class__.__name__)
            logger.setLevel(self.log_level)
            msg = f"sql executed: {sql}, params: "
            for param in param_list:
                msg += f"{str(param)}, "
            msg = msg.rstrip().rstrip(',')
            logger.info(msg)

        return r

    ###################
    # Transaction
    ###################
    @classmethod
    @contextlib.contextmanager
    def transaction_scope(
            cls,
            db_filepath: str,
            isolation_level: IsolationLevel = IsolationLevel.DEFERRED):
        """Create a transaction scope.

        Parameters
        ----------
        db_filepath : str
            Database file path
        isolation_level : IsolationLevel, optional
            Isolation level, by default IsolationLevel.DEFERRED

        Yields
        ------
        DB
            DB instance
        """
        con = cls(db_filepath, isolation_level)
        with contextlib.closing(con) as tran:
            try:
                yield tran
            finally:
                tran.rollback()
                tran.close()
