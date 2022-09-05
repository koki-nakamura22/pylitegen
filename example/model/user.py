from dataclasses import dataclass, field
from typing import ClassVar, Final, List, Optional

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class User(BaseModel):
    id: int
    name: str
    phone: str
    address: Optional[str] = None
    table_name: ClassVar[str] = 'users'
    pks: ClassVar[List[str]] = ['id']
