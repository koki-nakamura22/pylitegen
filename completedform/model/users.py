from dataclasses import dataclass


@dataclass(init=True, eq=True, frozen=True)
class Users:
    id: int
    name: str
    phone: str
    address: str
    table_name: str = 'users'
