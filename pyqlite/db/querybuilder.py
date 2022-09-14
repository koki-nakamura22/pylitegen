import re
from typing import List, Optional, Tuple, Type, Union

from pyqlite.model import BaseModel


class QueryBuilder:
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
            keys: List[str]) -> str:
        if len(keys) == 0:
            raise ValueError('The values of keys must be 1 or more')

        sql = f"SELECT * FROM {model_class.get_table_name()} WHERE "
        for k in keys:
            sql += f"{k} = ? AND "
        sql = cls.__format_where(sql)
        return sql

    @classmethod
    def build_select(cls,
                     model_class: Type[BaseModel],
                     where: Optional[str] = None) -> str:
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
        if len(models) == 0:
            return "", []

        params_str = '?, ' * len(models[0].member_names)
        params_str = params_str.rstrip().rstrip(',')
        or_ignore_str = " OR IGNORE" if insert_or_ignore else ''
        sql = f"INSERT{or_ignore_str} INTO {models[0].table_name} VALUES "
        sql += f"({params_str}), " * len(models)
        sql = sql.rstrip().rstrip(',')

        param_list = []
        for model in models:
            param_list.extend(model.values)
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
            condition: Optional[Union[dict, List]] = None) -> str:
        sql = f"UPDATE {model_class.get_table_name()} SET "

        for k in data_to_be_updated:
            if where is None or isinstance(condition, dict):
                sql += f"{k} = :{k}, "
            else:
                sql += f"{k} = ?, "
        sql = sql.rstrip().rstrip(',')

        if where is not None:
            sql += f" WHERE {where}"

        return sql

    @ classmethod
    def build_update_by_model(cls, model: BaseModel) -> str:
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
        sql = f"DELETE FROM {model_class.get_table_name()}"
        if where is not None:
            sql += f" WHERE {where}"
        return sql

    @ classmethod
    def build_delete_by_model(cls, model: BaseModel) -> str:
        sql = f"DELETE FROM {model.table_name} WHERE "
        member_list = model.pks if 0 < len(model.pks) else model.member_names
        for member_name in member_list:
            sql += f"{member_name} = :{member_name} AND "
        sql = cls.__format_where(sql)
        return sql
