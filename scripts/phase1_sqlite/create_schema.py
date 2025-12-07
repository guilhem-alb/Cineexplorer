import sqlite3

with sqlite3.connect("../../data/imdb.db") as conn:
    c = conn.cursor()

    conn.execute("BEGIN TRANSACTION")

    try:
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
        name VARCHAR(100) UNIQUE NOT NULL COLLATE NOCASE
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
        genre_name VARCHAR(50) UNIQUE NOT NULL COLLATE NOCASE
        )
        """)

        # Table knownformovies

        c.execute("""
        CREATE TABLE known_for_movies (
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
            year INTEGER,
            runtime_minutes INTEGER,
                
            CHECK(year >= 1895),
            CHECK(runtime_minutes > 0)
        )
        """)

        # Table Persons

        c.execute("""
        CREATE TABLE persons (
            person_id VARCHAR(20) PRIMARY KEY,
            name VARCHAR(100),
            birth_year INTEGER,
            death_year INTEGER,
                
            CHECK(birth_year > 1800),
            CHECK(death_year > 1850 AND death_year >= birth_year)
        )   
        """)

        # Table Principals

        c.execute("""
        CREATE TABLE principals (
            movie_id VARCHAR(20),
            ordering INTEGER,
            person_id VARCHAR(20),
            profession_id INTEGER,
            job VARCHAR(100),
                
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
            job_name VARCHAR(100) UNIQUE NOT NULL COLLATE NOCASE
        )
        """)

        # Table ratings

        c.execute("""
        CREATE TABLE ratings (
            movie_id VARCHAR(20) PRIMARY KEY,
            average_rating REAL,
            num_votes INTEGER,
                
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            
            CHECK(average_rating >= 0 AND average_rating <= 10),
            CHECK(num_votes >= 0)
        )
        """)

        # Table title_ordering

        c.execute("""
        CREATE TABLE title_ordering (
            movie_id VARCHAR(20),
            title_id INTEGER,
            ordering INTEGER,
                  
            region VARCHAR(20),
            language VARCHAR(20),
            types VARCHAR(200),
            attributes VARCHAR(200),
        
            PRIMARY KEY (movie_id, title_id, ordering),
            FOREIGN KEY (movie_id, title_id) REFERENCES movie_titles(movie_id, title_id)
        )
        """)

        # Table movie_titles

        c.execute("""
        CREATE TABLE movie_titles (
            movie_id VARCHAR(20),
            title_id INTEGER,
            is_primary BOOLEAN,
            is_original BOOLEAN,
                
            PRIMARY KEY (movie_id, title_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY (title_id) REFERENCES titles(title_id)
        )
        """)

        # Table titles

        c.execute("""
        CREATE TABLE titles (
            title_id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_name VARCHAR(200) UNIQUE NOT NULL COLLATE NOCASE 
        )
        """)

        conn.commit()
        
    except sqlite3.Error as e:
        print("base de donnée non crée, cause:", e)
        conn.rollback()
    
