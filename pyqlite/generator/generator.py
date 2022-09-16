import os
import sqlite3

from threading import local
from pyqlite.generator import Column, DBMetaData, ModelFileGenerator


class Generator:
    """Generator
    """

    @classmethod
    def generate_model_files(cls, db_filepath: str, output_path: str) -> None:
        """Generate model files by db metadata.

        Parameters
        ----------
        db_filepath : str
            File path of the database to use
        output_path : str
            Model files output path
        """
        con = None
        try:
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            con = sqlite3.connect(db_filepath)

            table_names = DBMetaData.select_table_names(con)
            for table_name in table_names:
                columns = DBMetaData.select_columns_metadata(con, table_name)
                ModelFileGenerator.generate(table_name, columns, output_path)

        finally:
            if con is not None:
                con.close()
