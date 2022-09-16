from enum import Enum


class IsolationLevel(Enum):
    """Isolation level for SQLite

    Parameters
    ----------
    Enum : str
        Isolation level
    """

    DEFERRED = 'DEFERRED'
    IMMEDIATE = 'IMMEDIATE'
    EXCLUSIVE = 'EXCLUSIVE'
