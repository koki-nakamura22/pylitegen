import os
import sqlite3
import sys
import pytest
from pytest import main
from typing import Final

import tests.import_path_resolver
from pyqlite.generator.column import Column
from pyqlite.generator.dbmetadata import DBMetaData
from tests.create_test_db import DBForTestCreator

currnet_dir: Final[str] = os.path.dirname(__file__)
db_filepath: Final[str] = os.path.join(currnet_dir, 'test.db')


class TestDBMetaData:
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

    @pytest.mark.dbmetadata
    def test_select_table_names(self):
        con = sqlite3.connect(db_filepath)
        got_table_names = DBMetaData.select_table_names(con)
        expected_table_names = [
            'users',
            'user_edited_histories',
            'all_optional_columns',
            'backup_users'
        ]
        assert set(got_table_names) == set(expected_table_names)

    @pytest.mark.dbmetadata
    def test_select_columns_metadata_with_pk_column(self):
        con = sqlite3.connect(db_filepath)
        table_name = 'users'
        got_columns = DBMetaData.select_columns_metadata(con, table_name)

        expected_columns = [
            Column(0, 'id', 'INTEGER', True, None, 1),
            Column(1, 'name', 'TEXT', True, None, 0),
            Column(2, 'phone', 'TEXT', True, None, 0),
            Column(3, 'address', 'TEXT', False, None, 0),
        ]
        assert set(got_columns) == set(expected_columns)

    @pytest.mark.dbmetadata
    def test_select_columns_metadata_with_no_pk_column(self):
        con = sqlite3.connect(db_filepath)
        table_name = 'user_edited_histories'
        got_columns = DBMetaData.select_columns_metadata(con, table_name)

        expected_columns = [
            Column(0, 'datetime', 'TEXT', True, None, 0),
            Column(1, 'note', 'TEXT', False, None, 0),
        ]
        assert set(got_columns) == set(expected_columns)


if __name__ == '__main__':
    sys.exit(main())
