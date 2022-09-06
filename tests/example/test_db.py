import import_path_resolver
import os
import sqlite3
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
    def test_where_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            found_users = transaction.where(User, {'address': 'Japan'})
            assert found_users == user, user2

    def test_where_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            found_users = transaction.where(User, {'address': 'USA'})
            assert found_users == []

    def test_where_data_with_no_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users_to_be_insert = [
                User(1, 'TestUser', '123', 'Japan'),
                User(2, 'TestUser2', '123', 'Japan'),
                User(3, 'TestUse3', '123', 'Australia')
            ]
            for u in users_to_be_insert:
                transaction.insert(u)

            found_users = transaction.where(User, {})
            assert found_users == users_to_be_insert

    ###################
    # Insert
    ###################
    @pytest.mark.skip(reason="This test is already done at test_find_data_found.")
    def test_insert_not_existing_data(self):
        pass

    def test_insert_duplicate_data(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(sqlite3.IntegrityError) as e:
                transaction.insert(user, False)
            assert str(
                e.value) == 'UNIQUE constraint failed: users.id'

    def test_insert_duplicate_data_but_ignore(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            transaction.insert(user)

            found_user = transaction.find(User, {'id': 1})
            assert found_user == user

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
