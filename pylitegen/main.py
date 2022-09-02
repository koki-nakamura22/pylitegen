import os
import sqlite3

from db.metadata import MetaData
from db.model_gen import gen


def main():
    current_dir = os.path.dirname(__file__)
    db_file_path = os.path.join(current_dir, 'tweets-collector.db')
    con = sqlite3.connect(db_file_path)

    table_names = MetaData.select_table_names(con)
    for table_name in table_names:
        columns = MetaData.select_columns_metadata(con, table_name)
        gen(table_name, columns)


if __name__ == '__main__':
    main()
