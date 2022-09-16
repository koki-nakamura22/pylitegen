from argparse import ArgumentParser
import os
import sys

from pyqlite.generator import Generator


def main() -> int:
    parser = ArgumentParser()
    parser.add_argument(
        '-gm',
        '--generate-model',
        required=True,
        action='store_true',
        help='Flag to create model files from a db file. Must use with -d or --db-path.')
    parser.add_argument(
        '-d',
        '--db-path',
        required=True,
        help='Specify a db file path that is used to create model files. Must use with -g model or --generate model')
    parser.add_argument(
        '-o',
        '--output-path',
        help='A created model files output path.')

    try:
        args = parser.parse_args(sys.argv[1:])
        if args.output_path is None:
            output_path = os.getcwd()
        else:
            output_path = args.output_path
        Generator.generate_model_files(args.db_path, output_path)
        return 0

    except Exception as e:
        from pprint import pprint
        pprint(e)
        return 1


if __name__ == '__main__':
    main()
