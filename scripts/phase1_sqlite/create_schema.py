import sqlite3


with sqlite3.connect("../../data/imdb.db") as conn:
    c = conn.cursor()

    # Table Cast

    c.execute("""
    CREATE TABLE cast (
    movie_id VARCHAR(20),
    person_id VARCHAR(20),
    character_id INTEGER,
              
    PRIMARY KEY (movie_id, person_id, character_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id),
    FOREIGN KEY (character_id) REFERENCES characters(character_id)
    );
    """)

    # Table Characters

    c.execute("""
    CREATE TABLE characters (
    character_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100)
    )    
    """)

    # Table movie_genres

    c.execute("""
    CREATE TABLE movie_genres (
    movie_id VARCHAR(20),
    genre_id INTEGER,
              
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
    )
    """)

    # Table Genres

    c.execute("""
    CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre VARCHAR(50)
    )
    """)

    # Table knownformovies

    c.execute("""
    CREATE TABLE knownformovies (
    person_id VARCHAR(20),
    movie_id VARCHAR(20),
              
    PRIMARY KEY (person_id, movie_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id),        
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )
    """)

    # Table Movies

    c.execute("""
    CREATE TABLE movies (
        movie_id VARCHAR(20) PRIMARY KEY,
        isAdult BOOLEAN,
        startYear INTEGER,
        endYear REAL,
        runtimeMinutes REAL,
              
        CHECK(startYear > 1900),
        CHECK(endYear > 1900 AND endYear >= startYear),
        CHECK(runtimeMinutes > 0)
    )
    """)

    # Table Persons

    c.execute("""
    CREATE TABLE persons (
        person_id VARCHAR(20) PRIMARY KEY,
        primaryName VARCHAR(100),
        birthYear REAL,
        deathYear REAL,
              
        CHECK(birthYear > 1800),
        CHECK(deathYear > 1850 AND deathYear > birthYear)
    )   
    """)

    # Table Principals

    c.execute("""
    CREATE TABLE principals (
        movie_id VARCHAR(20),
        ordering INTEGER,
        person_id VARCHAR(20),
        profession_id INTEGER,
        category VARCHAR(100),
              
        PRIMARY KEY (movie_id, ordering),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id),
        FOREIGN KEY (profession_id) REFERENCES professions(profession_id)
    )
    """)

    # Table person_profession

    c.execute("""
    CREATE TABLE person_profession (
    person_id VARCHAR(20),
    profession_id INTEGER,
              
    PRIMARY KEY (person_id, profession_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id),
    FOREIGN KEY (profession_id) REFERENCES professions(profession_id)
    )   
    """)

    # Table professions

    c.execute("""
    CREATE TABLE professions (
        profession_id INTEGER PRIMARY KEY AUTOINCREMENT,
        jobName VARCHAR(100)
    )
    """)

    # Table ratings

    c.execute("""
    CREATE TABLE ratings (
        movie_id VARCHAR(20) PRIMARY KEY,
        averageRating REAL,
        numVotes INTEGER,
              
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        
        CHECK(averageRating >= 0 AND averageRating <= 10),
        CHECK(numVotes > 0)
    )
    """)

    # Table titles

    c.execute("""
    CREATE TABLE titles (
        movie_id VARCHAR(20),
        ordering INTEGER,
        title VARCHAR(200),
        region VARCHAR(20),
        language VARCHAR(20),
        types VARCHAR(100),
        attributes VARCHAR(100),
        isOriginalTitle BOOLEAN,
        isPrimary BOOLEAN,
              
        PRIMARY KEY (movie_id, ordering),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )
    """)

