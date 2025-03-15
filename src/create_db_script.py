import mysql.connector

# Connecting to mysql database
# Notice it is not a security breach because it is not up or my credentials

connection = mysql.connector.connect(
    host="localhost",
    port=3305,
    user="talweiss2",
    password="talweiss30336",
    database="talweiss2",
)

cursor = connection.cursor()
table_queries = {}

# Defining the table creation tables
# Added Unique to prevent repeting inserts
table_queries["movies"] = """
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    date_published DATE,
    description TEXT,
    budget INT UNSIGNED,
    box_office INT UNSIGNED,
    avg_vote FLOAT(2,1) UNSIGNED,
    num_of_votes INT UNSIGNED,
    revenue BIGINT AS (COALESCE(box_office, 0) - COALESCE(budget, 0)) STORED,
    FULLTEXT (description),
    FULLTEXT (name),
    INDEX idx_date (date_published) USING BTREE,
    INDEX idx_revenue (revenue) USING BTREE,
    INDEX name_hash_index (name) USING HASH

)
"""
table_queries["genres"] = """
CREATE TABLE IF NOT EXISTS genres (
    id INT AUTO_INCREMENT PRIMARY KEY ,
    name VARCHAR(255) NOT NULL,
    INDEX name_hash_index (name) USING HASH,
    UNIQUE(name)
)
"""
# unique key is the combination of movie_id and genre_id
table_queries["movieGenres"] = """
CREATE TABLE IF NOT EXISTS movieGenres (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    FOREIGN KEY(movie_id) REFERENCES movies(id),
    FOREIGN KEY(genre_id) REFERENCES genres(id),
    PRIMARY KEY(movie_id,genre_id)

)
"""
table_queries["actors"] = """
CREATE TABLE IF NOT EXISTS actors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) COLLATE utf8_bin NOT NULL,
    UNIQUE(name)
)
"""
# unique key is the combination of movie_id and actor_id
table_queries["movieActors"] = """
CREATE TABLE IF NOT EXISTS movieActors (
    movie_id INT NOT NULL,
    actor_id INT NOT NULL,
    FOREIGN KEY(movie_id) REFERENCES movies(id),
    FOREIGN KEY(actor_id) REFERENCES actors(id),
    PRIMARY KEY(movie_id,actor_id)
)
"""
table_queries["directors"] = """
CREATE TABLE IF NOT EXISTS directors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) COLLATE utf8_bin NOT NULL,
    UNIQUE(name)
)
"""
# unique key is the combination of movie_id and director_id
table_queries["movieDirectors"] = """
CREATE TABLE IF NOT EXISTS movieDirectors (
    movie_id INT NOT NULL,
    director_id INT NOT NULL,
    FOREIGN KEY(movie_id) REFERENCES movies(id),
    FOREIGN KEY(director_id) REFERENCES directors(id),
    PRIMARY KEY(movie_id,director_id)
)
"""
for table in table_queries.keys():
    try:
        cursor.execute(table_queries[table])
        connection.commit()
    except Exception as e:
        print(f"Failed To create {table}")
        print(f"error {e}")



cursor.close()
connection.close()