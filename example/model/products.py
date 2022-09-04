from dataclasses import dataclass, field
from typing import Final, List

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Products(BaseModel):
    id: int
    name: str
    amount: int
    table_name: Final[str] = field(default='products', init=False)
    pk_names: Final[List[str]] = field(
        default_factory=lambda: ['id'], init=False)
