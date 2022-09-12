import re
from typing import List, Optional, Tuple, Type

from example.model import BaseModel


class QueryBuilder:
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
        sql = re.sub(' AND $', '', sql)
        return sql

    @classmethod
    def build_select(cls,
                     model_class: Type[BaseModel],
                     where: Optional[str] = None) -> str:
        sql = f"SELECT * FROM {model_class.get_table_name()}"
        if where is not None:
            sql += f" WHERE {where}"
        return sql

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

    @ classmethod
    def build_update(
            cls,
            model_class: Type[BaseModel],
            data_to_be_updated: dict,
            condition: Optional[dict]) -> Tuple[str, List]:
        sql = f"UPDATE {model_class.get_table_name()} SET "
        param_list = list()
        for k, v in data_to_be_updated.items():
            sql += f"{k} = ?, "
            param_list.append(v)
        sql = sql.rstrip().rstrip(',')

        if condition is not None:
            sql += " WHERE 1 = 1"
            for k, v in condition.items():
                sql += f" AND {k} = ?"
                param_list.append(v)

        return sql, param_list

    @ classmethod
    def build_update_by_model(cls, model: BaseModel) -> Tuple[str, List]:
        sql = f"UPDATE {model.table_name} SET "
        data_to_be_updated = model._BaseModel__get_data_to_be_updated()  # type: ignore
        param_list = list()
        for k, v in data_to_be_updated.items():
            sql += f"{k} = ?, "
            param_list.append(v)
        sql = sql.rstrip().rstrip(',')

        sql += " WHERE 1 = 1"
        model_dict = model.to_dict()
        for pk in model.pks:
            sql += f" AND {pk} = ?"
            param_list.append(model_dict[pk])

        return sql, param_list

    @ classmethod
    def build_delete(
            cls,
            model_class: Type[BaseModel],
            condition: Optional[dict]) -> Tuple[str, List]:
        sql = f"DELETE FROM {model_class.get_table_name()} WHERE 1 = 1"
        param_list = list()
        if condition is not None:
            for k in condition:
                sql += f" AND {k} = ?"
                param_list.append(condition[k])
        return sql, param_list

    @ classmethod
    def build_delete_by_model(cls, model: BaseModel) -> Tuple[str, List]:
        sql = f"DELETE FROM {model.table_name} WHERE 1 = 1"
        param_list = list()
        member_list = model.pks if 0 < len(model.pks) else model.member_names
        for member_name in member_list:
            sql += f" AND {member_name} = ?"
            param_list.append(getattr(model, member_name))
        return sql, param_list
