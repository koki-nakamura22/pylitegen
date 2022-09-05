from abc import ABC
import dataclasses
from dataclasses import field
from typing import ClassVar, List, Type


class BaseModel(ABC):
    table_name: ClassVar[str]
    pks: ClassVar[List[str]]
    __cache: dict = field(default_factory=lambda: dict(), init=False)

    def __set_cache(self) -> None:
        members = vars(self)
        for k in members:
            self.__cache[k] = members[k]

    def __get_data_to_be_updated(self) -> dict:
        data_to_be_updated = dict()
        members = vars(self)
        for k in members:
            if members[k] != self.__cache[k]:
                data_to_be_updated[k] = members[k]
        return data_to_be_updated

    @classmethod
    def get_class_type(cls) -> Type:
        return cls

    @property
    def class_type(self) -> Type:
        return self.__class__

    @classmethod
    def get_member_names(cls) -> List[str]:
        member_names = list()
        for k, v in cls.__annotations__.items():
            if not hasattr(v, '__origin__'):
                member_names.append(k)
            elif v.__origin__ is not ClassVar:
                member_names.append(k)
        return member_names

    @property
    def member_names(self) -> List[str]:
        member_list = list()
        members = vars(self)
        for k in members:
            if k == 'pks':
                continue
            member_list.append(k)
        return member_list

    @property
    def values(self) -> List:
        val_list = list()
        members = vars(self)
        for k in members:
            if k == 'pks':
                continue
            val_list.append(members[k])
        return val_list

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)
