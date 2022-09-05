from dataclasses import dataclass, field
from typing import ClassVar, Final, List, Optional

from example.model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Orders(BaseModel):
    id: int
    quantity: int
    amount_total: float
    note: Optional[str]
    user_id: int
    product_id: int
    table_name: ClassVar[str] = 'orders'
    pks: ClassVar[List[str]] = ['id']
