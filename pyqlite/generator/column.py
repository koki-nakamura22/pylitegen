from dataclasses import dataclass
from typing import Any, Final, Optional


@dataclass(init=True, eq=True, frozen=True)
class Column:
    column_index: Final[int]
    name: Final[str]
    data_type: Final[str]
    notnull: Final[bool]
    default_value: Final[Optional[Any]]
    pk: Final[int]

    def is_not_null(self) -> bool:
        return self.notnull == 1

    def is_pk(self) -> bool:
        return self.pk == 1

    def data_type_to_py_type_str(self) -> str:
        type_d = {
            'TEXT': 'str',
            'NUMERIC': 'Any',
            'INTEGER': 'int',
            'REAL': 'float',
            'NULL': 'Any',
        }
        return type_d[self.data_type]
