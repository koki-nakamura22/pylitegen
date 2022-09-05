from dataclasses import dataclass, field
from typing import ClassVar, Final, List

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Users(BaseModel):
    id: int
    name: str
    phone: str
    address: str
    table_name: ClassVar[str] = 'users'
    pks: ClassVar[List[str]] = ['id']
