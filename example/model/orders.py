from dataclasses import dataclass, field
from typing import Final, List, Optional

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Orders(BaseModel):
    id: int
    quantity: int
    amount_total: float
    note: Optional[str]
    user_id: int
    product_id: int
    table_name: Final[str] = field(default='orders', init=False)
    pk_names: Final[List[str]] = field(
        default_factory=lambda: ['id'], init=False)
