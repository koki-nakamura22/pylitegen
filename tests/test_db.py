import tests.import_path_resolver
from logging import INFO, WARNING
import os
import sqlite3
import sys
import pytest
from pytest import main
from typing import Final

from pyqlite.db import DB
from example.model import User, UserEditedHistory
from example.script.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')


class TestDB:

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

    ###################
    # Select
    ###################
    ###################
    # find
    ###################
    @pytest.mark.find
    def test_find_data_found(self):
        db = DB(db_filepath)
        db.log_level = INFO
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            found_user = transaction.find(User, 1)
            assert isinstance(found_user, User)
            assert found_user == user

    @pytest.mark.find
    def test_find_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            found_user = transaction.find(User, 1)
            assert found_user is None

    @pytest.mark.find
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
            assert str(
                e.value) == 'Cannot use find method because this class does not have any primary keys'

    @pytest.mark.find
    def test_find_primary_keys_and_primary_key_values_do_not_match(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(ValueError) as e:
                found_user = transaction.find(User, 1, "TestUser")
            assert str(
                e.value) == 'The number of primary keys and primary key values do not match'

    @pytest.mark.find
    def test_find_not_specified_primary_key_values(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(ValueError) as e:
                found_user = transaction.find(User)
            assert str(
                e.value) == 'The number of primary keys and primary key values do not match'

    ###################
    # find_by
    ###################
    @pytest.mark.find_by
    def test_find_by_with_qmark_params_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            where = 'address = ?'
            values = ['Japan']
            found_user = transaction.find_by(User, where, values)
            assert isinstance(found_user, User)
            assert found_user == user

    @pytest.mark.find_by
    def test_find_by_with_qmark_params_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            where = 'address = ?'
            values = ['Australia']
            found_user = transaction.find_by(User, where, values)
            assert found_user is None

    @pytest.mark.find_by
    def test_find_by_with_named_params_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            where = 'address = :address'
            values = {
                'address': 'Japan'
            }
            found_user = transaction.find_by(User, where, values)
            assert isinstance(found_user, User)
            assert found_user == user

    @pytest.mark.find_by
    def test_find_by_with_named_params_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            where = 'address = :address'
            values = {
                'address': 'Australia'
            }
            found_user = transaction.find_by(User, where, values)
            assert found_user is None

    @pytest.mark.find_by
    def test_find_by_with_not_specified_where_and_values(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '456', 'Japan')
            transaction.insert(user2)

            found_user = transaction.find_by(User)
            assert isinstance(found_user, User)
            assert found_user == user

    @pytest.mark.find_by
    def test_find_by_with_only_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            with pytest.raises(ValueError) as e:
                transaction.find_by(User, 'address = ?')
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.find_by
    def test_find_by_with_only_values(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            params = ['Australia']
            with pytest.raises(ValueError) as e:
                transaction.find_by(User, params=params)
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    ###################
    # where
    ###################
    @pytest.mark.where
    def test_where_with_qmark_params_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            where = 'address = ?'
            values = ['Japan']
            found_users = transaction.where(User, where, values)
            assert found_users == [user, user2]

    @pytest.mark.where
    def test_where_with_qmark_params_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            where = 'address = ?'
            values = ['USA']
            found_users = transaction.where(User, where, values)
            assert found_users == []

    @pytest.mark.where
    def test_where_with_named_params_data_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            where = 'address = :address'
            values = {
                'address': 'Japan'
            }
            found_users = transaction.where(User, where, values)
            assert found_users == [user, user2]

    @pytest.mark.where
    def test_where_with_named_params_data_not_found(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            where = 'address = :address'
            values = {
                'address': 'USA'
            }
            found_users = transaction.where(User, where, values)
            assert found_users == []

    @pytest.mark.where
    def test_where_with_not_specified_where_and_values(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            user2 = User(2, 'TestUser2', '123', 'Japan')
            transaction.insert(user2)
            user3 = User(3, 'TestUse3', '123', 'Australia')
            transaction.insert(user3)

            found_users = transaction.where(User)
            assert found_users == [user, user2, user3]

    @pytest.mark.where
    def test_where_with_only_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            with pytest.raises(ValueError) as e:
                transaction.where(User, 'address = ?')
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.where
    def test_where_with_only_values(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            params = ['Australia']
            with pytest.raises(ValueError) as e:
                transaction.where(User, params=params)
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    ###################
    # Insert
    ###################
    @pytest.mark.insert
    @pytest.mark.skip(reason="This test is already done at test_find_data_found.")
    def test_insert_not_existing_data(self):
        pass

    @pytest.mark.insert
    def test_insert_duplicate_data(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            with pytest.raises(sqlite3.IntegrityError) as e:
                transaction.insert(user, False)
            assert str(
                e.value) == 'UNIQUE constraint failed: users.id'

    @pytest.mark.insert
    def test_insert_duplicate_data_but_ignore(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)
            transaction.insert(user)

            found_user = transaction.find(User, 1)
            assert found_user == user

    @pytest.mark.bulk_insert
    def test_bulk_insert_with_all_same_type_model(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users = [
                User(1, 'Taro', '123', 'Japan'),
                User(2, 'Jiro', '456', 'Australia'),
                User(3, 'Saburo', '789', 'USA')
            ]
            transaction.bulk_insert(users)

            found_users = transaction.where(User)
            assert found_users == users

    @pytest.mark.bulk_insert
    def test_bulk_insert_with_object_not_having_chass_type(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            models = [
                User(1, 'Taro', '123', 'Japan'),
                str('test')
            ]
            with pytest.raises(ValueError) as e:
                transaction.bulk_insert(models)
            assert str(
                e.value) == 'All parameter models must be inherited BaseModel'

    @pytest.mark.bulk_insert
    def test_bulk_insert_with_diferent_types_models(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            models = [
                User(1, 'Taro', '123', 'Japan'),
                UserEditedHistory('2022/10/31 12:34:56')
            ]
            with pytest.raises(ValueError) as e:
                transaction.bulk_insert(models)
            assert str(
                e.value) == 'Multiple types of models cannot be specified'

    @pytest.mark.bulk_insert
    def test_bulk_insert_duplicate_data(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users = [
                User(1, 'Taro', '123', 'Japan'),
                User(2, 'Jiro', '456', 'Australia'),
                User(2, 'Jiro', '456', 'Australia')
            ]

            with pytest.raises(sqlite3.IntegrityError) as e:
                transaction.bulk_insert(users, False)
            assert str(
                e.value) == 'UNIQUE constraint failed: users.id'

    @pytest.mark.bulk_insert
    def test_bulk_insert_duplicate_data_but_ignore(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user1 = User(1, 'Taro', '123', 'Japan')
            user2 = User(2, 'Jiro', '456', 'Australia')
            user3 = User(2, 'Jiro', '456', 'Australia')
            users = [user1, user2, user3]
            transaction.bulk_insert(users)

            found_users = transaction.where(User)
            assert len(found_users) == 2
            assert found_users == [user1, user2]

    ###################
    # Update
    ###################
    @pytest.mark.update
    def test_update_with_qmark_params_and_where(self):
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

            where_for_update = 'address = ?'
            r = transaction.update(
                User,
                {'address': 'USA'},
                where_for_update,
                ['Japan'])

            where_for_where = 'address = :address'
            found_users = transaction.where(
                User, where_for_where, {'address': 'USA'})
            user.address = user2.address = 'USA'
            assert found_users == [user, user2]

    @pytest.mark.update
    def test_update_with_qmark_params_and_no_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users = [
                User(1, 'TestUser', '123', 'Japan'),
                User(2, 'TestUser2', '123', 'Japan'),
                User(3, 'TestUse3', '123', 'Australia')
            ]
            for u in users:
                transaction.insert(u)

            transaction.update(
                User,
                {'address': 'Australia'})

            found_users = transaction.where(User)
            for u in users:
                u.address = 'Australia'
            assert found_users == users

    @pytest.mark.update
    def test_update_with_named_params_and_where(self):
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

            where_for_update = 'address = :p_address'
            r = transaction.update(
                User,
                {'address': 'USA'},
                where_for_update,
                {'p_address': 'Japan'})

            where_for_where = 'address = :address'
            found_users = transaction.where(
                User, where_for_where, {'address': 'USA'})
            user.address = user2.address = 'USA'
            assert found_users == [user, user2]

    @pytest.mark.update
    def test_update_with_named_params_and_no_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users = [
                User(1, 'TestUser', '123', 'Japan'),
                User(2, 'TestUser2', '123', 'Japan'),
                User(3, 'TestUse3', '123', 'Australia')
            ]
            for u in users:
                transaction.insert(u)

            r = transaction.update(
                User,
                {'address': 'Canada'})

            found_users = transaction.where(User)
            for u in users:
                u.address = 'Canada'
            assert found_users == users

    @pytest.mark.update
    @pytest.mark.skip(reason="This case is not needed run because done in other cases")
    def test_update_with_not_specified_where_and_values(self):
        pass

    @pytest.mark.update
    def test_update_with_only_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            with pytest.raises(ValueError) as e:
                transaction.update(User, {'name': 'TestUser'}, 'id = ?')
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.update
    def test_update_with_only_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            with pytest.raises(ValueError) as e:
                transaction.update(
                    User, {
                        'name': 'TestUser'}, params={
                        'id': 1})
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.update
    @pytest.mark.skip(reason="This case is not needed run because sqlite library throws an error.")
    def test_update_edited_pk(self):
        pass

    @pytest.mark.update_by_model
    def test_update_by_model_with_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            user.name = 'Taro'
            transaction.update_by_model(user)

            where = 'name = :name'
            user_before_change = transaction.find_by(
                User, where, {'name': 'TestUser'})
            assert user_before_change is None
            user_after_change = transaction.find_by(
                User, where, {'name': 'Taro'})
            assert user_after_change == user

    @pytest.mark.update_by_model
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

    @pytest.mark.update_by_model
    @pytest.mark.skip(reason='This case does not happen because primary key cannot be edited in a model class.')
    def test_update_by_model_edited_pk(self):
        pass

    ###################
    # Delete
    ###################
    @pytest.mark.delete
    def test_delete_with_qmark_params_and_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            found_user = transaction.find(User, 1)
            assert found_user is not None

            where = 'id = ?'
            params = [1]
            transaction.delete(User, where, params)

            found_user = transaction.find(User, 1)
            assert found_user is None

    @pytest.mark.delete
    def test_delete_with_qmark_params_and_no_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            users = [
                User(1, 'TestUser', '123', 'Japan'),
                User(2, 'TestUser2', '123', 'Japan'),
                User(3, 'TestUse3', '123', 'Australia')
            ]
            for u in users:
                transaction.insert(u)

            found_users = transaction.where(User)
            assert len(found_users) == 3

            transaction.delete(User)

            found_users = transaction.where(User)
            assert len(found_users) == 0

    @pytest.mark.delete
    def test_delete_with_named_params_and_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            found_user = transaction.find(User, 1)
            assert found_user is not None

            where = 'id = :id'
            params = {'id': 1}
            transaction.delete(User, where, params)

            found_user = transaction.find(User, 1)
            assert found_user is None

    @pytest.mark.delete
    @pytest.mark.skip(reason='This case is not needed run because done in other cases')
    def test_delete_with_named_params_and_no_where(self):
        pass

    @pytest.mark.delete
    @pytest.mark.skip(reason='This case is not needed run because done in other cases')
    def test_delete_with_not_specified_where_and_values(self):
        pass

    @pytest.mark.delete
    def test_delete_by_with_only_where(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            with pytest.raises(ValueError) as e:
                transaction.delete(User, 'address = ?')
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.delete
    def test_delete_with_only_condition(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            params = ['Australia']
            with pytest.raises(ValueError) as e:
                transaction.delete(User, params=params)
            assert str(
                e.value) == 'Both where and values must be passed, or not passed both'

    @pytest.mark.delete_by_model
    def test_delete_by_model_with_pk(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            user = User(1, 'TestUser', '123', 'Japan')
            transaction.insert(user)

            found_user = transaction.find(User, 1)
            assert found_user is not None

            transaction.delete_by_model(user)

            found_user = transaction.find(User, 1)
            assert found_user is None

    @pytest.mark.delete_by_model
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

            where_for_find_by = 'note = :note'
            found_history = transaction.find_by(
                UserEditedHistory, where_for_find_by, {'note': 'note'})
            assert found_history is not None
            found_histories = transaction.where(UserEditedHistory)
            assert len(found_histories) == 3

            transaction.delete_by_model(user_edited_history)

            found_history = transaction.find_by(
                UserEditedHistory, where_for_find_by, {'note': 'note'})
            assert found_history is None
            found_histories = transaction.where(UserEditedHistory)
            assert len(found_histories) == 2

    ###################
    # Transaction
    ###################
    @pytest.mark.transaction
    def test_transaction_with_no_commits(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, 1)
            assert user_on_tran is not None

            # Cannot get data that was inserted on other transaction
            with db.transaction_scope() as transaction2:
                user_on_tran = transaction2.find(User, 1)
                assert user_on_tran is None

    @pytest.mark.transaction
    def test_transaction_with_commits(self):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.insert(User(1, 'TestUser', '123', 'Japan'))

            user_on_tran = transaction.find(User, 1)
            assert user_on_tran is not None
            transaction.commit()

        with db.transaction_scope() as transaction2:
            # Data that was inserted on other transaction can be got
            user_on_tran = transaction2.find(User, 1)
            assert user_on_tran is not None

    ###################
    # Log
    ###################
    @pytest.mark.log
    def test_output_sql_executed_log_with_no_params(self, caplog):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.log_level = INFO
            transaction.where(User)

            assert len(caplog.records) == 1
            record = caplog.records[0]
            assert record.name == 'DB'
            assert record.levelname == 'INFO'
            assert record.msg == 'sql executed: SELECT * FROM users'

    @pytest.mark.log
    def test_output_sql_executed_log_with_qmark_params(self, caplog):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.log_level = INFO
            where = 'name = ? AND address = ?'
            transaction.where(User, where, ['Taro', 'Japan'])

            assert len(caplog.records) == 1
            record = caplog.records[0]
            assert record.name == 'DB'
            assert record.levelname == 'INFO'
            assert record.msg == 'sql executed: SELECT * FROM users WHERE name = ? AND address = ?: Taro, Japan'

    @pytest.mark.log
    def test_output_sql_executed_log_with_named_params(self, caplog):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.log_level = INFO
            where = 'name = :name AND address = :address'
            transaction.where(
                User, where, {
                    'name': 'Taro', 'address': 'Japan'})

            assert len(caplog.records) == 1
            record = caplog.records[0]
            assert record.name == 'DB'
            assert record.levelname == 'INFO'
            assert record.msg == 'sql executed: SELECT * FROM users WHERE name = :name AND address = :address: Taro, Japan'

    @pytest.mark.log
    @pytest.mark.skip(reason="This test is already done at test_output_sql_executed_log_with_no_params and test_output_sql_executed_log_with_params.")
    def test_not_output_sql_executed_log_with_log_level_info(self, caplog):
        pass

    @pytest.mark.log
    def test_not_output_sql_executed_log_with_log_level_warning(self, caplog):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.log_level = WARNING
            transaction.where(User)

            assert len(caplog.records) == 0

    @pytest.mark.log
    def test_not_output_sql_executed_log_with_log_level_none(self, caplog):
        db = DB(db_filepath)
        with db.transaction_scope() as transaction:
            transaction.log_level = None
            transaction.where(User)

            assert len(caplog.records) == 0


if __name__ == '__main__':
    sys.exit(main())
