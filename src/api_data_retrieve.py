import mysql.connector
import pandas as pd 
from typing import List, Tuple
# import pickle

genre_cache = {}
director_cache = {}
movie_cache = {}
actor_cache = {}

def run_query(cursor: mysql.connector, connection, query: str, data_inserted: List[Tuple[str]]) -> None:
    try:
        cursor.executemany(query, data_inserted)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error during batch insert: {err}")
        for data in data_inserted:
            try:
                cursor.execute(query, data)
                connection.commit()
            except mysql.connector.Error as single_err:
                print(f"Failed to insert data: {data} with error: {single_err}")
    return


def populate_actors(cursor: mysql.connector.cursor.MySQLCursor, df: pd.DataFrame, connection: mysql.connector.MySQLConnection) -> None:
    # set to prevent repeats to conserve run time
    global actor_cache
    data_inserted = []
    id = 1
    for index, row in df.iterrows():
        if not isinstance(row['actors'], str) or row['actors'] == '':
            print(f"Skipping non-string value at index {index}: {row['actors']}")
            continue
        split_values = row['actors'].split(', ')
        for actor_name in split_values:
            if actor_name in actor_cache:
                continue
            data_inserted.append((id,actor_name))
            actor_cache[actor_name] = id
            id+=1
    query = """
INSERT INTO actors(id,name)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE name = VALUES(name)
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    # save_cache(cache_file_path)
    return

def populate_generes(cursor :mysql.connector,df: pd.DataFrame, connection):
    # set to prevent repeats to conserve run time
    global genre_cache
    data_inserted = []
    id = 1
    for index, row in df.iterrows():
        if not isinstance(row['genre'], str):
            print(f"Skipping non-string value at index {index}: {row['genre']}")
            continue
        split_values = row['genre'].split(', ')
        for genre_name in split_values:
            if genre_name in genre_cache:
                continue
            data_inserted.append((id,genre_name))
            genre_cache[genre_name] = id
            id+=1
    query = """
INSERT INTO genres(id,name)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE name = VALUES(name)
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    # save_cache(cache_file_path)
    return

def populate_directors(cursor :mysql.connector,df: pd.DataFrame, connection):
    # set to prevent repeats to conserve run time
    global director_cache
    data_inserted = []
    id = 1
    for index, row in df.iterrows():
        if not isinstance(row['director'], str):
            print(f"Skipping non-string value at index {index}: {row['director']}")
            continue
        split_values = row['director'].split(', ')
        for director_name in split_values:
            if director_name in director_cache:
                continue
            data_inserted.append((id,director_name))
            director_cache[director_name] = id
            id+=1
    query = """
INSERT INTO directors(id,name)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE name = VALUES(name)
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    # save_cache(cache_file_path)
    return    

def populate_movies(cursor :mysql.connector,df: pd.DataFrame, connection):
    # set to prevent repeats to conserve run time
    global movie_cache
    id = 1
    data_inserted = []
    for index, row in df.iterrows():
        orignal_title = row['original_title']
        date_published = row['date_published']
        description = row['description']
        budget = row['budget']
        box_office = row['worlwide_gross_income'] 
        avg_vote = row['avg_vote']
        num_of_votes = row['votes']
        data_inserted.append((id,orignal_title,date_published,description,budget,box_office,avg_vote,num_of_votes))
        #we need both in case there is a unique name
        movie_cache[(orignal_title,date_published)] = id
        id+=1
    query = """
INSERT INTO movies(id,name,date_published,description,budget,box_office,avg_vote,num_of_votes)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE 
    name = VALUES(name), 
    date_published = VALUES(date_published),
    description = VALUES(description),
    budget = VALUES(budget),
    box_office = VALUES(box_office),
    avg_vote = VALUES(avg_vote),
    num_of_votes = VALUES(num_of_votes);
"""
    # save_cache(cache_file_path)
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    return    


def populate_movieActors(cursor: mysql.connector.cursor.MySQLCursor, df: pd.DataFrame, connection: mysql.connector.MySQLConnection) -> None:
    global movie_cache,actor_cache
    data_inserted = []
    for index, row in df.iterrows():
        movie_name = row['original_title']
        movie_date = row['date_published']
        movie_id = movie_cache.get((movie_name,movie_date))
        split_values = row['actors'].split(', ')
        if movie_id is None:
                raise Exception("movie_id isn't wasn't defined")
        for actor_name in split_values:
            actor_id = actor_cache.get(actor_name)
            if actor_id is None:
                raise Exception("actor_id isn't wasn't defined")
            data_inserted.append((movie_id,actor_id))
    query = """
INSERT INTO movieActors(movie_id,actor_id)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE movie_id = VALUES(movie_id), actor_id = VALUES(actor_id);
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    return
def populate_movieDirectors(cursor: mysql.connector.cursor.MySQLCursor, df: pd.DataFrame, connection: mysql.connector.MySQLConnection) -> None:
    global movie_cache,director_cache
    data_inserted = []
    for index, row in df.iterrows():
        movie_name = row['original_title']
        movie_date = row['date_published']
        movie_id = movie_cache.get((movie_name,movie_date))
        split_values = row['director'].split(', ')
        if movie_id is None:
                raise Exception("movie_id isn't wasn't defined")
        for director_name in split_values:
            director_id = director_cache.get(director_name)
            if director_id is None:
                raise Exception("director_id isn't wasn't defined")
            data_inserted.append((movie_id,director_id))
    query = """
INSERT INTO movieDirectors(movie_id,director_id)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE movie_id = VALUES(movie_id), director_id = VALUES(director_id);
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    return

def populate_movieGenres(cursor: mysql.connector.cursor.MySQLCursor, df: pd.DataFrame, connection: mysql.connector.MySQLConnection) -> None:
    global movie_cache,genre_cache
    data_inserted = []
    for index, row in df.iterrows():
        movie_name = row['original_title']
        movie_date = row['date_published']
        movie_id = movie_cache.get((movie_name,movie_date))
        split_values = row['genre'].split(', ')
        if movie_id is None:
                raise Exception("movie_id isn't wasn't defined")
        for genre_name in split_values:
            genre_id = genre_cache.get(genre_name)
            if genre_id is None:
                raise Exception("genre_id isn't wasn't defined")
            data_inserted.append((movie_id,genre_id))
    query = """
INSERT INTO movieGenres(movie_id,genre_id)
VALUES(%s,%s)
ON DUPLICATE KEY UPDATE movie_id = VALUES(movie_id), genre_id = VALUES(genre_id);
"""
    if data_inserted:
        run_query(cursor,connection,query,data_inserted)
    return



def main():
    # Connecting to mysql database
    global movie_cache,actor_cache
    connection = mysql.connector.connect(
    host="localhost",
    port=3305,
    user="talweiss2",
    password="talweiss30336",
    database="talweiss2",
    )
    cursor = connection.cursor()

    connection.commit()
    df = pd.read_csv('./data/cleaned_trimmed_fixed_data4.csv' ,encoding='utf-8')
    df = df.where(pd.notnull(df), None)
    print("Usually takes about 1-2 minutes...")
    try:
        populate_actors(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating actors table {e}")
    try:
        populate_generes(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating generes table {e}")
    try:
        populate_directors(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating generes table {e}")
    try:
        populate_movies(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating movies table {e}")
    try:
        populate_movieActors(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating movieActors table {e}")
    try:
        populate_movieDirectors(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating movieActors table {e}")
    try:
        populate_movieGenres(cursor,df,connection)
    except Exception as e:
        print(f"Failed populating movieActors table {e}")
    


    cursor.close()
    connection.close()
    return

main()

