from dataclasses import dataclass
from typing import Any, Final, Optional


@dataclass(init=True, eq=True, frozen=True)
class Column:
    """Represents SQLite column data.

    Attributes
    ----------
    column_index: Final[int]
        Column index
    name: Final[str]
        Column name
    data_type: Final[str]
        SQLite column data type str
    notnull: Final[bool]
        Column allow null data or not
    default_value: Final[Optional[Any]]
        Column default value
    pk: Final[int]
        The column is primary key or not
    ----------
    """

    column_index: Final[int]
    name: Final[str]
    data_type: Final[str]
    notnull: Final[bool]
    default_value: Final[Optional[Any]]
    pk: Final[int]

    def is_not_null(self) -> bool:
        """Get the column allow null value or not.

        Returns
        -------
        bool
            True: The column does not allow null value
            False: The column allow null value
        """
        return self.notnull == 1

    def is_pk(self) -> bool:
        """Get the column is primary key or not.

        Returns
        -------
        bool
            True: The column is primary key
            False: The column is not primary key
        """
        return self.pk == 1

    def data_type_to_py_type_str(self) -> str:
        """Convert SQLite data type string to python data type string.

        Returns
        -------
        str
            Python data type string
        """
        type_d = {
            'TEXT': 'str',
            'NUMERIC': 'Any',
            'INTEGER': 'int',
            'REAL': 'float',
            'NULL': 'Any',
        }
        return type_d[self.data_type]
