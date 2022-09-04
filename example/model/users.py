from dataclasses import dataclass
from typing import Final, List


@dataclass(init=True, eq=True, frozen=True)
class Users:
    id: int
    name: str
    phone: str
    address: str
    table_name: Final[str] = 'users'
    pk_names: Final[List[str]] = ['id']
