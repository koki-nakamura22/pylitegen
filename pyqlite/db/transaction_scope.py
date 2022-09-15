import contextlib

from pyqlite.db import DB, IsolationLevel


@contextlib.contextmanager
def transaction_scope(
        db_filepath: str,
        isolation_level: IsolationLevel = IsolationLevel.DEFERRED):
    con = DB(db_filepath, isolation_level=isolation_level)
    with contextlib.closing(con) as tran:
        try:
            yield tran
        finally:
            tran.rollback()
            tran.close()
