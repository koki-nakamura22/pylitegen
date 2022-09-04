from abc import ABC, abstractmethod
import dataclasses
from typing import Final, List


class BaseModel(ABC):
    table_name: str
    pk_names: List[str]

    def member_names_as_list(self) -> List:
        member_list = list()
        members = vars(self)
        for k in members:
            member_list.append(k)
        return member_list

    def values_as_list(self) -> List:
        val_list = list()
        members = vars(self)
        for k in members:
            val_list.append(members[k])
        return val_list

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)
