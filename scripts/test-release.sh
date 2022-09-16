#!/bin/sh

# Cleanup the previous released directories.
rm -Rf ./build
rm -Rf ./dist
rm -Rf ./pyqlite.egg-info

# Build then release
python setup.py sdist
python setup.py bdist_wheel
twine upload --repository testpypi dist/*
