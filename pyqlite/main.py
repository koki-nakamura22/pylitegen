import argparse
from ast import parse
import sys


def main():
    # args = sys.argv

    parser = argparse.ArgumentParser()
    parser.add_argument('-g model', '--generate model', help='test help')
    parser.add_argument('-d', '--db-path', help='DB file path')
    parser.add_argument('-o', '--output-path', help='Model files output path')

    argv = ['this file path', '-g model', 'nya-n']
    # args = parser.parse_args(argv[1:])
    args = parser.parse_args(sys.argv[1:])
    print(args)
    if 'generate model' in args:
        pass
    else:
        pass
        # error


if __name__ == '__main__':
    main()
