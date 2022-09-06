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
            assert found_users == [user, user2]

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
    def test_update_with_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            user2 = User(2, 'TestUser2', '123', 'Japan')
            users_to_be_insert = [
                user,
                user2,
                User(3, 'TestUse3', '123', 'Australia')
            ]
            for u in users_to_be_insert:
                transaction.insert(u)

            transaction.update(User, {'address': 'USA'}, {'address': 'Japan'})

            user.address = user2.address = 'USA'

            found_users = transaction.where(User, {'address': 'USA'})
            assert found_users == [user, user2]

    def test_update_with_no_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            user2 = User(2, 'TestUser2', '123', 'Australia')
            user3 = User(3, 'TestUser3', '123', 'India')
            users_to_be_insert = [user, user2, user3]
            for u in users_to_be_insert:
                transaction.insert(u)

            transaction.update(User, {'address': 'USA'}, {})

            user.address = user2.address = user3.address = 'USA'

            found_users = transaction.where(User, {'address': 'USA'})
            assert found_users == [user, user2, user3]

    @pytest.mark.skip(reason="This case is not needed run because sqlite library throws an error.")
    def test_update_edited_pk(self):
        pass

    def test_update_by_model_with_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            user.name = 'Taro'
            transaction.update_by_model(user)

            user_before_change = transaction.find_by(
                User, {'name': 'TestUser'})
            assert user_before_change is None
            user_after_change = transaction.find_by(User, {'name': 'Taro'})
            assert user_after_change == user

    def test_update_by_model_with_no_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user_edited_history = UserEditedHistory(
                '2022/10/31 10:12:34', 'note')
            transaction.insert(user_edited_history)

            user_edited_history.note = 'aaa'
            with pytest.raises(ValueError) as e:
                transaction.update_by_model(user_edited_history)
            assert str(
                e.value) == 'Cannot use this function with no primary key model'

    @pytest.mark.skip(reason='This case does not happen because primary key cannot be edited in a model class.')
    def test_update_by_model_edited_pk(self):
        pass

    ###################
    # Delete
    ###################
    def test_delete_with_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            found_user = transaction.find(User, {'id': 1})
            assert found_user is not None

            transaction.delete(User, {"id": 1})

            found_user = transaction.find(User, {'id': 1})
            assert found_user is None

    def test_delete_with_no_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users_to_be_insert = [
                User(1, 'TestUser', '123', 'Japan'),
                User(2, 'TestUser2', '123', 'Japan'),
                User(3, 'TestUser3', '123', 'Japan')
            ]
            for u in users_to_be_insert:
                transaction.insert(u)

            found_users = transaction.where(User, {})
            assert len(found_users) == 3

            transaction.delete(User, {})

            found_users = transaction.where(User, {})
            assert len(found_users) == 0

    def test_delete_by_model_with_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            found_user = transaction.find(User, {'id': 1})
            assert found_user is not None

            transaction.delete_by_model(user)

            found_user = transaction.find(User, {'id': 1})
            assert found_user is None

    def test_delete_by_model_with_no_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user_edited_history = UserEditedHistory(
                '2022/10/31 10:12:34', 'note')
            user_edited_histories = [
                user_edited_history,
                UserEditedHistory(
                    '2022/11/01 10:12:34', 'note2'),
                UserEditedHistory(
                    '2022/11/02 10:12:34', 'note3'),
            ]
            for history in user_edited_histories:
                transaction.insert(history)

            found_history = transaction.find_by(
                UserEditedHistory, {'note': 'note'})
            assert found_history is not None
            found_histories = transaction.where(UserEditedHistory, {})
            assert len(found_histories) == 3

            transaction.delete_by_model(user_edited_history)

            found_history = transaction.find_by(
                UserEditedHistory, {'note': 'note'})
            assert found_history is None
            found_histories = transaction.where(UserEditedHistory, {})
            assert len(found_histories) == 2

    ###################
    # Transaction
    ###################
    def test_transaction_with_no_commits(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, {'id': 1})
            assert user_on_tran is not None

            # Cannot get data that was inserted on other transaction
            with db.transaction_scope() as transaction2:
                user_on_tran = transaction2.find(User, {
                    'id': 1})
                assert user_on_tran is None

    def test_transaction_with_commits(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, {'id': 1})
            assert user_on_tran is not None
            transaction.commit()

        with db.transaction_scope() as transaction2:
            # Data that was inserted on other transaction can be got
            user_on_tran = transaction2.find(User, {
                'id': 1})
            assert user_on_tran is not None


if __name__ == '__main__':
    sys.exit(main())
