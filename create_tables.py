"""
Create the fact and dimension tables for the star schema in Redshift.
This script contains two functions that will be used to drop and create tables using SQL queries,
which are defined in another module 'sql_queries'. The functions are called in the 'main' function
which sets up the Redshift database connection using parameters specified in a configuration file 
'dwh.cfg'. Finally, the connection to the database is closed.

"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    print("Running the create_tables.py file...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()