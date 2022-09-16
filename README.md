# pyqlite

pyqlite - Lightweight SQLite O/R Mapper and Model Generator for Python.

## How to install

```sh
pip install pyqlite
```

## Usage

### Generate model class files

```sh
pyqlite -gm -d (db file path) [-o (output path)]
```

- Not specify output path

When not specified output path, model files are outputted to the current directory.
```sh
pyqlite -gm -d test.db
```

- Specify output path

Model files are outputted to the specified directory.
```sh
pyqlite -gm -d test.db -o ./models
```

### DB Operation

Please have a look tests in this repository.  
(I am sorry, but, I have a plan I will example in the future.)


## License

Apache-2.0 license
