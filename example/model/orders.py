from dataclasses import dataclass
from typing import Final, List, Optional


@dataclass(init=True, eq=True, frozen=True)
class Orders:
    id: int
    quantity: int
    amount_total: float
    note: Optional[str]
    user_id: int
    product_id: int
    table_name: Final[str] = 'orders'
    pk_names: Final[List[str]] = ['id']
