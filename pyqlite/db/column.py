from dataclasses import dataclass
from typing import Any, Optional


@dataclass(init=True, eq=True, frozen=True)
class Column:
    column_index: int
    name: str
    data_type: str
    notnull: bool
    default_value: Optional[Any]
    pk: int

    def is_not_null(self) -> bool:
        return self.notnull == 1

    def is_pk(self) -> bool:
        return self.pk == 1

    def data_type_to_py_type_str(self) -> str:
        type_d = {
            'TEXT': 'str',
            'NUMERIC': 'Any',
            'INTEGER': 'int',
            'INT': 'int',
            'REAL': 'float',
            'NULL': 'Any',
        }
        return type_d[self.data_type]
