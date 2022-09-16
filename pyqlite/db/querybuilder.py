import re
from typing import List, Optional, Tuple, Type, Union

from pyqlite.model import BaseModel


class QueryBuilder:
    """QueryBuilder
    """

    ###################
    # Utils
    ###################
    @classmethod
    def __format_where(cls, sql: str) -> str:
        return re.sub(' AND $', '', sql)

    ###################
    # Select
    ###################
    @classmethod
    def build_select_with_qmark_parameters(
            cls,
            model_class: Type[BaseModel],
            primary_key_values: List[str]) -> str:
        """Build select statement with qmark parameters.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        primary_key_values : List[str]
            Primary key values

        Returns
        -------
        str
            Built select statement str

        Raises
        ------
        ValueError
            Raises ValueError if the number of primary key values is 0
        """
        if len(primary_key_values) == 0:
            raise ValueError('The values of keys must be 1 or more')

        sql = f"SELECT * FROM {model_class.get_table_name()} WHERE "
        for k in primary_key_values:
            sql += f"{k} = ? AND "
        sql = cls.__format_where(sql)
        return sql

    @classmethod
    def build_select(cls,
                     model_class: Type[BaseModel],
                     where: Optional[str] = None) -> str:
        """Build select statement.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        where : Optional[str], optional
            Where clause str, by default None

        Returns
        -------
        str
            Built select statement str
        """
        sql = f"SELECT * FROM {model_class.get_table_name()}"
        if where is not None:
            sql += f" WHERE {where}"
        return sql

    ###################
    # Insert
    ###################
    @classmethod
    def build_insert(
            cls,
            model: BaseModel,
            insert_or_ignore: bool = True) -> Tuple[str, List]:
        """Build insert into statetment.

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
        Tuple[str, List]
            Built insert statement str, insert parameters
        """
        params_str = '?, ' * len(model.member_names)
        params_str = params_str.rstrip().rstrip(',')
        or_ignore_str = " OR IGNORE" if insert_or_ignore else ''
        sql = f"INSERT{or_ignore_str} INTO {model.table_name} VALUES ({params_str})"
        return sql, model.values

    @classmethod
    def build_bulk_insert(
            cls,
            models: List,
            insert_or_ignore: bool = True) -> Tuple[str, List]:
        """Build insert into statetment.

        Parameters
        ----------
        models : List
            Target model
        insert_or_ignore : bool, optional
            INSERT OR IGNORE flag
            True: INSERT OR IGNORE
            False: not INSERT OR IGNORE
            , by default True

        Returns
        -------
        Tuple[str, List]
            Built insert statement str, insert parameters
        """
        if len(models) == 0:
            return "", []

        params_str = '?, ' * len(models[0].member_names)
        params_str = params_str.rstrip().rstrip(',')
        or_ignore_str = " OR IGNORE" if insert_or_ignore else ''

        sql = f"INSERT{or_ignore_str} INTO {models[0].table_name} VALUES ("
        for member_name in models[0].member_names:
            sql += f":{member_name}, "
        sql = re.sub(', $', ')', sql)

        param_list = []
        for model in models:
            param_list.append(model.to_dict())

        return sql, param_list

    ###################
    # Update
    ###################
    @ classmethod
    def build_update(
            cls,
            model_class: Type[BaseModel],
            data_to_be_updated: dict,
            where: Optional[str] = None,
            where_params: Optional[Union[dict, List]] = None) -> str:
        """Build update statement.

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
        str
            Built update statement str
        """
        sql = f"UPDATE {model_class.get_table_name()} SET "

        for k in data_to_be_updated:
            if where is None or isinstance(where_params, dict):
                sql += f"{k} = :{k}, "
            else:
                sql += f"{k} = ?, "
        sql = sql.rstrip().rstrip(',')

        if where is not None:
            sql += f" WHERE {where}"

        return sql

    @ classmethod
    def build_update_by_model(cls, model: BaseModel) -> str:
        """Build update statement.

        Parameters
        ----------
        model : BaseModel
            Target model

        Returns
        -------
        str
            Built update statement str
        """
        sql = f"UPDATE {model.table_name} SET "

        data_to_be_updated = getattr(
            model, '_BaseModel__get_data_to_be_updated')()
        for k, _ in data_to_be_updated.items():
            sql += f"{k} = :{k}, "
        sql = sql.rstrip().rstrip(',')

        sql += " WHERE "
        for pk in model.pks:
            sql += f"{pk} = :{pk} AND "
        sql = cls.__format_where(sql)

        return sql

    ###################
    # Delete
    ###################
    @ classmethod
    def build_delete(
            cls,
            model_class: Type[BaseModel],
            where: Optional[str] = None) -> str:
        """Build delete statement.

        Parameters
        ----------
        model_class : Type[BaseModel]
            Target model class type
        where : Optional[str], optional
            Where clause str, by default None

        Returns
        -------
        str
            Built delete statement str
        """
        sql = f"DELETE FROM {model_class.get_table_name()}"
        if where is not None:
            sql += f" WHERE {where}"
        return sql

    @ classmethod
    def build_delete_by_model(cls, model: BaseModel) -> str:
        """Build delete statement.

        Parameters
        ----------
        model : BaseModel
            Target model

        Returns
        -------
        str
            Built delete statement str
        """
        sql = f"DELETE FROM {model.table_name} WHERE "
        member_list = model.pks if 0 < len(model.pks) else model.member_names
        for member_name in member_list:
            sql += f"{member_name} = :{member_name} AND "
        sql = cls.__format_where(sql)
        return sql
