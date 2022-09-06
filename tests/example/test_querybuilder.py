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
    def test_build_select_with_no_condition(self):
        condition = None
        sql, param_list = QueryBuilder.build_select(User, condition)
        assert sql == 'SELECT * FROM users WHERE 1 = 1'
        assert len(param_list) == 0

    def test_build_select_with_condition(self):
        condition = {
            'id': 1,
            'name': 'Taro'
        }
        sql, param_list = QueryBuilder.build_select(User, condition)
        assert sql == 'SELECT * FROM users WHERE 1 = 1 AND id = ? AND name = ?'
        assert param_list == [1, 'Taro']

    ###################
    # Build Insert
    ###################
    def test_build_insert_ignore(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql, param_list = QueryBuilder.build_insert(user, True)
        assert sql == 'INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)'
        assert param_list == [1, 'Taro', '12345', 'Japan']

    def test_build_insert_not_ignore(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql, param_list = QueryBuilder.build_insert(user, False)
        assert sql == 'INSERT INTO users VALUES (?, ?, ?, ?)'
        assert param_list == [1, 'Taro', '12345', 'Japan']

    ###################
    # Build Update
    ###################
    def test_build_update_with_no_condition(self):
        user = User(1, 'Taro', '12345', 'Japan')
        data_to_be_updated = {
            'address': 'Tokyo'
        }
        condition = None
        sql, param_list = QueryBuilder.build_update(
            User, data_to_be_updated, condition)
        assert sql == 'UPDATE FROM users SET address = ?'
        assert param_list == ['Tokyo']

    def test_build_update_with_condition(self):
        user = User(1, 'Taro', '12345', 'Japan')
        data_to_be_updated = {
            'address': 'Tokyo'
        }
        condition = {
            'address': 'Japan'
        }
        sql, param_list = QueryBuilder.build_update(
            User, data_to_be_updated, condition)
        assert sql == 'UPDATE FROM users SET address = ? WHERE 1 = 1 AND address = ?'
        assert param_list == ['Tokyo', 'Japan']

    def test_build_update_by_model(self):
        user = User(1, 'Taro', '12345', 'Japan')
        user.name = 'Jiro'
        user.address = 'Australia'
        sql, param_list = QueryBuilder.build_update_by_model(user)
        assert sql == "UPDATE FROM users SET name = ?, address = ? WHERE 1 = 1 AND id = ?"
        assert param_list == ['Jiro', 'Australia', 1]

    @pytest.mark.skip(reason="This case does not happen because runs check outside.")
    def test_build_update_by_model_changed_pk(self):
        pass

    ###################
    # Build Delete
    ###################
    def test_build_delete_with_no_condition(self):
        condition = None
        sql, param_list = QueryBuilder.build_delete(User, condition)
        assert sql == 'DELETE FROM users WHERE 1 = 1'
        assert len(param_list) == 0

    def test_build_delete_with_condition(self):
        condition = {
            'id': 1,
            'name': 'Taro'
        }
        sql, param_list = QueryBuilder.build_select(User, condition)
        assert sql == 'SELECT * FROM users WHERE 1 = 1 AND id = ? AND name = ?'
        assert param_list == [1, 'Taro']

    def test_build_delete_by_model_with_pks(self):
        user = User(1, 'Taro', '12345', 'Japan')
        sql, param_list = QueryBuilder.build_delete_by_model(user)
        assert sql == 'DELETE FROM users WHERE 1 = 1 AND id = ?'
        assert param_list == [1]

    def test_build_delete_by_model_with_no_pks(self):
        user_edited_history = UserEditedHistory(
            '2022/10/31 12:34:56', 'notenotenote')
        sql, param_list = QueryBuilder.build_delete_by_model(
            user_edited_history)
        assert sql == 'DELETE FROM user_edited_histories WHERE 1 = 1 AND datetime = ? AND note = ?'
        assert param_list == ['2022/10/31 12:34:56', 'notenotenote']


if __name__ == '__main__':
    sys.exit(main())
