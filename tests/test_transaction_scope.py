
import tests.import_path_resolver
import os
import sys
import pytest
from pytest import main
from typing import Final

from pyqlite.db import IsolationLevel, transaction_scope
from example.model.user import User
from tests.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')


class TestTransactionScope:
    @classmethod
    def setup_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)
        db_creator = DBForTestCreator(currnet_dir)
        db_creator.create()

    @classmethod
    def teardown_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)

    @pytest.mark.transaction_scope
    def test_transaction_scope_init_isolation_level_set_default(self):
        with transaction_scope(db_filepath) as transaction:
            assert transaction.con.isolation_level == 'DEFERRED'

    @pytest.mark.transaction_scope
    def test_transaction_scope_init_isolation_level_set_deferred(self):
        with transaction_scope(
                db_filepath,
                isolation_level=IsolationLevel.DEFERRED) as transaction:
            assert transaction.con.isolation_level == 'DEFERRED'

    @pytest.mark.transaction_scope
    def test_transaction_scope_init_isolation_level_set_immediate(self):
        with transaction_scope(
                db_filepath,
                isolation_level=IsolationLevel.IMMEDIATE) as transaction:
            assert transaction.con.isolation_level == 'IMMEDIATE'

    @pytest.mark.transaction_scope
    def test_transaction_scope_init_isolation_level_set_exclusive(self):
        with transaction_scope(
                db_filepath,
                isolation_level=IsolationLevel.EXCLUSIVE) as transaction:
            assert transaction.con.isolation_level == 'EXCLUSIVE'

    @pytest.mark.transaction_scope
    def test_transaction_scope_with_no_commits(self):
        with transaction_scope(db_filepath) as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, 1)
            assert user_on_tran is not None

            # Cannot get data that was inserted on other transaction
            with transaction_scope(db_filepath) as transaction2:
                user_on_tran = transaction2.find(User, 1)
                assert user_on_tran is None

    @pytest.mark.transaction_scope
    def test_transaction_scope_with_commits(self):
        with transaction_scope(db_filepath) as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, 1)
            assert user_on_tran is not None
            transaction.commit()

        with transaction_scope(db_filepath) as transaction2:
            # Data that was inserted on other transaction can be got
            user_on_tran = transaction2.find(User, 1)
            assert user_on_tran is not None


if __name__ == '__main__':
    sys.exit(main())
