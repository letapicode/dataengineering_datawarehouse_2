"""
The code is used to load and insert data into staging and production tables in a Redshift 
database. It also executes SQL queries to count the number of records in each table and 
provide insights such as the top 10 most played songs, the most popular hours for song 
plays, and user demographics.

"""    
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, count_table_queries, insights_table_queries


def load_staging_tables(cur, conn):
    # Loop over all queries to copy staging tables
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # Loop over all queries to insert data into fact and dimension tables
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    print("Running the etl.py file...")
    
    # Read Redshift cluster configuration from configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    # Load data into staging tables
    load_staging_tables(cur, conn)
    
    # Insert data into fact and dimension tables
    insert_tables(cur, conn)   
    
    
    print("The number of records in each table: ")
    for query in count_table_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print(row)
        print("\n")
   

    count = 0
    # Print insights about the data
    for query in insights_table_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchall()
        if count == 0:
            print("most_played_songs_query selects the top 10 most played songs:\n")
            print("(song title, artists name, play count): \n")
            count = count + 1
        elif count == 1:
            print("The peak_usage_hours_query selects the most popular hours for song plays: \n")
            print("(hour, song play count):\n")
            count = count + 1
        else:
            print("The user_demographics_query selects the count of users by gender and level (paid or free): \n")
            print("(user gender, subscription level, number of such users): \n")
        for row in results:
            print(row)
        print("\n")     
     
    # Close connection to Redshift cluster
    conn.close()


if __name__ == "__main__":
    main()