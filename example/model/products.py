from dataclasses import dataclass
from typing import Final, List


@dataclass(init=True, eq=True, frozen=True)
class Products:
    id: int
    name: str
    amount: int
    table_name: Final[str] = 'products'
    pk_names: Final[List[str]] = ['id']
