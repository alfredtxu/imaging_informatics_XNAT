#!/usr/bin/python
import psycopg2
from configparser import ConfigParser
from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)


def config(filename=f"{path}/dbconnection/database.ini", section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...', end='')
        conn = psycopg2.connect(**params)
        print('Connected!!')

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    connect()
