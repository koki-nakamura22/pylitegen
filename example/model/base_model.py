from abc import ABC
import copy
from dataclasses import dataclass, field
from typing import ClassVar, Dict, List, Type


@dataclass()
class BaseModel(ABC):
    table_name: ClassVar[str]
    pks: ClassVar[List[str]]
    __cache: dict = field(
        default_factory=lambda: dict(),
        init=False,
        compare=False)

    def __post_init__(self):
        self.__set_cache()

    def __set_cache(self) -> None:
        members = self.members
        for k in members:
            self._BaseModel__cache[k] = members[k]  # type: ignore

    def __get_data_to_be_updated(self) -> dict:
        data_to_be_updated = dict()
        members = self.members
        for k in members:
            if members[k] != self._BaseModel__cache[k]:  # type: ignore
                data_to_be_updated[k] = members[k]
        return data_to_be_updated

    @classmethod
    def get_class_type(cls) -> Type:
        return cls

    @property
    def class_type(self) -> Type:
        return self.__class__

    @property
    def members(self) -> Dict:
        excludes = ['_BaseModel__cache']
        members = copy.deepcopy(vars(self))
        for keyword in excludes:
            del members[keyword]
        return members

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
        return list(self.members.keys())

    @property
    def values(self) -> List:
        return list(self.members.values())

    def to_dict(self) -> dict:
        return self.members
