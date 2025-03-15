import mysql.connector
from typing import List, Tuple

# Connect to the MySQL database
# Notice it is not a security breach because it is not up or my credentials

connection = mysql.connector.connect(
    host="localhost",
    port=3305,
    user="talweiss2",
    password="talweiss30336",
    database="talweiss2",
    )

cursor = connection.cursor()

def run_select_query(cursor: mysql.connector.cursor.MySQLCursor, connection: mysql.connector.MySQLConnection, query: str, params: Tuple) -> List[Tuple]:
    try:
        print(f"\nparamaters: {params}")
        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"error: {e}")
        return []
    

def query_1(cursor,connection,word_searched_in_description, avg_vote_threshold):
    if not isinstance(word_searched_in_description,str):
        raise("word given to query_1 not a string")
    if not (isinstance(avg_vote_threshold,float) or isinstance(avg_vote_threshold,int)) :
        raise("Threshold given to query_1 not a int or float")
    
    query = """
SELECT name AS film_title ,revenue, date_published, avg_vote, description
FROM movies
WHERE MATCH(description) AGAINST(%s)
	AND avg_vote >= %s
ORDER BY revenue DESC
"""
    return run_select_query(cursor,connection,query,(word_searched_in_description,avg_vote_threshold))
    



def query_2(cursor,connection,word_searched_in_title,num_of_votes_threshold):
    if not isinstance(word_searched_in_title,str):
        raise("word given to query_2 not a string")
    if not isinstance(num_of_votes_threshold,int):
        raise("Threshold given to query_2 not an int")
    query = """
SELECT name AS film_title, revenue, date_published, avg_vote,
num_of_votes , description
FROM movies
WHERE MATCH(name) AGAINST(%s IN BOOLEAN MODE)
	AND num_of_votes >= %s
ORDER BY revenue DESC;
"""
## the + * allows for a substring full index search
    return run_select_query(cursor,connection,query,('+ ' + word_searched_in_title + '*', num_of_votes_threshold))
    
    

def query_3(cursor,connection,min_film_threshold,movie_name,num_of_top_actors):
    if not isinstance(movie_name,str):
        raise("movie_name given to query_3 not a string")
    if not isinstance(min_film_threshold,int):
        raise("num given to query_3 not int")
    if min_film_threshold < 0:
        raise("num given to query_3 non positive")
    if not isinstance(num_of_top_actors,int):
        raise("num given to query_3 not int")
    if num_of_top_actors < 0:
        raise("num given to query_3 non positive")

    main_query = """
WITH actor_and_number_of_movies AS(
SELECT actor_id , count(*) AS num_of_movies
FROM movieActors
GROUP BY actor_id
)
SELECT actors.name, AVG(revenue) as average_movies_revenue
FROM movies, movieActors, actors
WHERE movies.id = movieActors.movie_id
AND actors.id = movieActors.actor_id
AND actors.id  IN (
	SELECT actor_id
	FROM actor_and_number_of_movies
    WHERE num_of_movies >= %s
)
AND actors.id IN(
	SELECT actor_id
	FROM actors, movieActors, movies
		WHERE actors.id = movieActors.actor_id
		AND movies.id = movieActors.movie_id
		AND movies.name = %s
)
GROUP BY actors.id, actors.name
ORDER BY average_movies_revenue DESC
LIMIT %s
"""
    return run_select_query(cursor,connection,main_query,(min_film_threshold,movie_name, num_of_top_actors))

def query_4(cursor,connection,genre_name,num_of_min_films,num_of_top_directors):
    if not isinstance(genre_name,str):
        raise("genre given to query_4 not a string")
    if not isinstance(num_of_min_films,int):
        raise("num given to query_4 not int")
    if num_of_min_films < 0:
        raise("num given to query_4 non positive")
    if not isinstance(num_of_top_directors,int):
        raise("num given to query_4 not int")
    if num_of_top_directors < 0:
        raise("num given to query_4 non positive")


    main_query = """
WITH director_and_number_of_movies AS (
    SELECT movieDirectors.director_id, COUNT(*) AS num_of_movies
    FROM movieDirectors, genres, movies, movieGenres
    WHERE movieDirectors.movie_id = movies.id
    AND movieGenres.movie_id = movies.id
    AND movieGenres.genre_id = genres.id
    AND genres.name = %s
    GROUP BY director_id
),
actor_average_movie_revenue AS (
    SELECT actors.id, actors.name, AVG(movies.revenue) AS avg_actor_revenue
    FROM movieActors
    JOIN actors ON movieActors.actor_id = actors.id
    JOIN movies ON movieActors.movie_id = movies.id
    JOIN movieGenres ON movieGenres.movie_id = movies.id
    JOIN genres ON movieGenres.genre_id = genres.id 
    WHERE genres.name = %s
    GROUP BY actors.id, actors.name
)
SELECT directors.id, directors.name, AVG(actor_average_movie_revenue.avg_actor_revenue) AS director_actors_average
FROM actor_average_movie_revenue
	JOIN movieActors ON movieActors.actor_id = actor_average_movie_revenue.id
	JOIN movies ON movieActors.movie_id = movies.id
	JOIN movieDirectors ON movieDirectors.movie_id = movies.id
	JOIN directors ON movieDirectors.director_id = directors.id
	JOIN movieGenres ON movieGenres.movie_id = movies.id
	JOIN genres ON movieGenres.genre_id = genres.id

AND directors.id IN (
    SELECT director_id 
    FROM director_and_number_of_movies
    WHERE num_of_movies >= %s
)
GROUP BY directors.id, directors.name
ORDER BY director_actors_average DESC
LIMIT %s;
"""
    return run_select_query(cursor,connection,main_query,(genre_name,genre_name,num_of_min_films,num_of_top_directors))
    

    
def query_5(cursor,connection,start_date,end_date,num_of_top_genres):
    if not isinstance(start_date,str):
        raise("start_date given to query_5 not a string")
    if not isinstance(end_date,str):
        raise("end_date given to query_5 not a string")
    if num_of_top_genres < 0:
        raise("num given to query_5 non positive")
    if not isinstance(num_of_top_genres,int):
        raise("num given to query_5 not int")


    main_query = """
WITH movie_in_dates AS (
    SELECT id
    FROM movies
    WHERE %s <= date_published
    AND date_published <= %s
),
top_film_in_date AS (
    SELECT genres.id AS genre_id, movies.id AS movie_id, movies.name AS top_film, movies.revenue AS max_revenue
    FROM movies
    JOIN movieGenres ON movieGenres.movie_id = movies.id
    JOIN genres ON movieGenres.genre_id = genres.id
    WHERE movies.id IN (SELECT id FROM movie_in_dates)
    AND (movies.revenue, genres.id) IN (
        SELECT MAX(movies.revenue), genres.id
        FROM movies
        JOIN movieGenres ON movieGenres.movie_id = movies.id
        JOIN genres ON movieGenres.genre_id = genres.id
        WHERE movies.id IN (SELECT id FROM movie_in_dates)
        GROUP BY genres.id
    )
)
SELECT genres.name, AVG(movies.revenue) AS avg_genre_revenue, top_film_in_date.top_film as top_film_in_genre
FROM movie_in_dates
	JOIN movieGenres ON movieGenres.movie_id = movie_in_dates.id
	JOIN genres ON genres.id = movieGenres.genre_id
	JOIN movies ON movies.id = movie_in_dates.id
	JOIN top_film_in_date ON top_film_in_date.genre_id = genres.id
GROUP BY genres.id, genres.name, top_film_in_date.top_film
ORDER BY avg_genre_revenue DESC
LIMIT %s ;
"""
    return run_select_query(cursor,connection,main_query,(start_date,end_date,num_of_top_genres))



cursor.close()
connection.close()