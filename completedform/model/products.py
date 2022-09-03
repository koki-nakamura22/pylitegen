from dataclasses import dataclass


@dataclass(init=True, eq=True, frozen=True)
class Products:
    id: int
    name: str
    amount: int
    table_name: str = 'products'
