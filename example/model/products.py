from dataclasses import dataclass, field
from typing import ClassVar, Final, List

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Products(BaseModel):
    id: int
    name: str
    amount: int
    table_name: ClassVar[str] = 'products'
    pks: ClassVar[List[str]] = ['id']
