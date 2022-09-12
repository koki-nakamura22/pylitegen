import sys
import pytest
from pytest import main

import import_path_resolver
from example.db import QueryBuilder
from example.model import User, UserEditedHistory


class TestQueryBuilder:
    ###################
    # Build Select
    ###################
    @pytest.mark.build_select_with_qmark_parameters
    def test_build_select_with_qmark_parameters_with_key(self):
        keys = ['id']
        sql = QueryBuilder.build_select_with_qmark_parameters(User, keys)
        assert sql == 'SELECT * FROM users WHERE id = ?'

    @pytest.mark.build_select_with_qmark_parameters
    def test_build_select_with_qmark_parameters_with_multiple_keys(self):
        keys = ['id', 'email']
        sql = QueryBuilder.build_select_with_qmark_parameters(User, keys)
        assert sql == 'SELECT * FROM users WHERE id = ? AND email = ?'

    @pytest.mark.build_select_with_qmark_parameters
    def test_build_select_with_qmark_parameters_with_empty_keys(self):
        keys = []
        with pytest.raises(ValueError) as e:
            sql = QueryBuilder.build_select_with_qmark_parameters(User, keys)
        assert str(e.value) == 'The values of keys must be 1 or more'

    @pytest.mark.build_select
    def test_build_select_with_where(self):
        where = 'id = :id AND email = :email'
        sql = QueryBuilder.build_select(User, where)
        assert sql == 'SELECT * FROM users WHERE id = :id AND email = :email'

    @pytest.mark.build_select
    def test_build_select_with_no_where(self):
        sql = QueryBuilder.build_select(User)
        assert sql == 'SELECT * FROM users'

    ###################
    # Build Insert
    ###################
    @pytest.mark.build_insert
    def test_build_insert_ignore(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql, param_list = QueryBuilder.build_insert(user, True)
        assert sql == 'INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)'
        assert param_list == [1, 'Taro', '12345', 'Japan']

    @pytest.mark.build_insert
    def test_build_insert_not_ignore(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql, param_list = QueryBuilder.build_insert(user, False)
        assert sql == 'INSERT INTO users VALUES (?, ?, ?, ?)'
        assert param_list == [1, 'Taro', '12345', 'Japan']

    @pytest.mark.build_bulk_insert
    def test_build_bulk_insert_ignore(self):
        users = [
            User(1, 'Taro', '123', 'Japan'),
            User(2, 'Jiro', '456', 'Australia'),
            User(3, 'Saburo', '789', 'USA')
        ]
        sql, param_list = QueryBuilder.build_bulk_insert(users)
        assert sql == 'INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)'
        param_list_for_testing = []
        for u in users:
            param_list_for_testing.extend(u.values)
        assert param_list == param_list_for_testing

    @pytest.mark.build_bulk_insert
    def test_build_bulk_insert_not_ignore(self):
        users = [
            User(1, 'Taro', '123', 'Japan'),
            User(2, 'Jiro', '456', 'Australia'),
            User(3, 'Saburo', '789', 'USA')
        ]
        sql, param_list = QueryBuilder.build_bulk_insert(users, False)
        assert sql == 'INSERT INTO users VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)'
        param_list_for_testing = []
        for u in users:
            param_list_for_testing.extend(u.values)
        assert param_list == param_list_for_testing

    ###################
    # Build Update
    ###################
    @pytest.mark.build_update
    def test_build_update_with_qmark_params_and_where(self):
        data_to_be_updated = {
            'address': 'Tokyo'
        }
        where = 'address = ?'
        condition = ['USA']
        sql = QueryBuilder.build_update(
            User, data_to_be_updated, where, condition)
        assert sql == "UPDATE users SET address = ? WHERE address = ?"

    @pytest.mark.build_update
    def test_build_update_with_qmark_params_and_no_where(self):
        data_to_be_updated = {
            'address': 'Tokyo'
        }
        sql = QueryBuilder.build_update(User, data_to_be_updated)
        assert sql == "UPDATE users SET address = :address"

    @pytest.mark.build_update
    def test_build_update_with_named_params_and_where(self):
        data_to_be_updated = {
            'address': 'Tokyo'
        }
        where = 'address = :p_address'
        condition = {
            'p_address': 'USA'
        }
        sql = QueryBuilder.build_update(
            User, data_to_be_updated, where, condition)
        assert sql == "UPDATE users SET address = :address WHERE address = :p_address"

    @pytest.mark.build_update
    @pytest.mark.skip(reason="This case is not needed run because the same as test_build_update_with_qmark_params_and_no_where.")
    def test_build_update_with_named_params_and_no_where(self):
        pass

    @pytest.mark.build_update_by_model
    def test_build_update_by_model(self):
        user = User(1, 'Taro', '12345', 'Japan')
        user.name = 'Jiro'
        user.address = 'Australia'
        sql = QueryBuilder.build_update_by_model(user)
        assert sql == "UPDATE users SET name = :name, address = :address WHERE id = :id"

    @pytest.mark.skip(reason="This case does not happen because runs check outside.")
    def test_build_update_by_model_changed_pk(self):
        pass

    ###################
    # Build Delete
    ###################
    @pytest.mark.build_delete
    def test_build_delete_with_qmark_params_and_where(self):
        where = 'id = ? AND email = ?'
        sql = QueryBuilder.build_delete(User, where)
        assert sql == 'DELETE FROM users WHERE id = ? AND email = ?'

    @pytest.mark.build_delete
    def test_build_delete_with_qmark_params_and_no_where(self):
        sql = QueryBuilder.build_delete(User)
        assert sql == 'DELETE FROM users'

    @pytest.mark.build_delete
    def test_build_delete_with_named_params_and_where(self):
        where = 'id = :id AND email = :email'
        sql = QueryBuilder.build_delete(User, where)
        assert sql == 'DELETE FROM users WHERE id = :id AND email = :email'

    @pytest.mark.build_delete
    @pytest.mark.skip(reason="This case is not needed run because the same as test_build_delete_with_qmark_params_and_no_where.")
    def test_build_delete_with_named_params_and_no_where(self):
        pass

    @pytest.mark.build_delete_by_model
    def test_build_delete_by_model_with_pks(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql = QueryBuilder.build_delete_by_model(user)
        assert sql == 'DELETE FROM users WHERE id = :id'

    @pytest.mark.build_delete_by_model
    def test_build_delete_by_model_with_no_pks(self):
        user_edited_history = UserEditedHistory(
            '2022/10/31 12:34:56', 'notenotenote')
        sql = QueryBuilder.build_delete_by_model(user_edited_history)
        assert sql == 'DELETE FROM user_edited_histories WHERE datetime = :datetime AND note = :note'


if __name__ == '__main__':
    sys.exit(main())
