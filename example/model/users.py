from dataclasses import dataclass, field
from typing import Final, List

from model import BaseModel


@dataclass(init=True, eq=True, frozen=True)
class Users(BaseModel):
    id: int
    name: str
    phone: str
    address: str
    table_name: Final[str] = field(default='users', init=False)
    pk_names: Final[List[str]] = field(
        default_factory=lambda: ['id'], init=False)
