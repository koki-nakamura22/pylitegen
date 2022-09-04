from db.db import DB


def main():
    import os
    current_dir = os.path.dirname(__file__)
    db_filepath = os.path.join(current_dir, 'db', 'test.db')
    db = DB(db_filepath)
    with db.transaction_scope() as tran:
        pass


if __name__ == '__main__':
    main()
