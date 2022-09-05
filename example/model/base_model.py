from abc import ABC
import dataclasses
from typing import List


class BaseModel(ABC):
    table_name: str
    pk_names: List[str]

    @property
    def member_names(self) -> List:
        member_list = list()
        members = vars(self)
        for k in members:
            member_list.append(k)
        return member_list

    @property
    def values(self) -> List:
        val_list = list()
        members = vars(self)
        for k in members:
            val_list.append(members[k])
        return val_list

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)
