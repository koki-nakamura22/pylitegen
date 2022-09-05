import path_resolver
import os
import sys
import pytest
from pytest import main
from typing import Final

from example.db import DB
from example.model import User, UserEditedHistory
from example.script.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')


class TestDB:

    @classmethod
    def setup_class(cls):
        db_creator = DBForTestCreator(currnet_dir)
        db_creator.create()

    @classmethod
    def teardown_class(cls):
        if os.path.exists(db_filepath):
            os.remove(db_filepath)

    ###################
    # Select
    ###################
    ###################
    # find
    ###################
    def test_find_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            found_user = transaction.find(User, {'id': 1})
            assert isinstance(found_user, User)
            assert found_user == user

    def test_find_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            found_user = transaction.find(User, {'id': 1})
            assert found_user is None

    def test_find_primary_keys_and_condition_do_not_match(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(ValueError) as e:
                found_user = transaction.find(
                    User, {'id': 1, 'name': 'TestUser'})
            assert str(e.value) == 'Primary keys do not match'

    def test_find_no_primary_key_model(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user_edited_history = UserEditedHistory(
                '2022/10/31 10:12:34', 'aaa')
            transaction.insert(user_edited_history)

            with pytest.raises(ValueError) as e:
                found_data = transaction.find(
                    UserEditedHistory, {
                        'date': '2022/10/31 10:12:34'})
            assert str(e.value) == 'Primary keys do not match'

    ###################
    # find_by
    ###################
    def test_find_by_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            found_user = transaction.find_by(User, {'address': 'Japan'})
            assert isinstance(found_user, User)
            assert found_user == user

    def test_find_by_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            found_user = transaction.find_by(User, {'address': 'Australia'})
            assert found_user is None

    def test_find_by_unmatch_condition_keys_and_columns(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(ValueError) as e:
                found_user = transaction.find_by(User, {'gender': 'male'})
            assert str(e.value) == 'Conditions and columns do not match'

    ###################
    # where
    ###################

    ###################
    # Insert
    ###################

    ###################
    # Update
    ###################

    ###################
    # Delete
    ###################

    ###################
    # Transaction
    ###################


if __name__ == '__main__':
    sys.exit(main())
