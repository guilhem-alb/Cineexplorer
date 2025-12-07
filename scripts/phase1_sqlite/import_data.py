import sqlite3
import pandas as pd

DB_PATH = "../../data/imdb.db"
CSV_REPO = "../../data/csv/"

# no dependency
def import_characters(conn):
    TRANSAC_NAME = "characters"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    charactersDf = pd.read_csv(CSV_REPO + "characters.csv")

    charactersSerie = charactersDf["name"].drop_duplicates()

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, elt in charactersSerie.items():
            if (pd.isna(elt) or elt == ""): # skip invalid values
                continue
            try:
                c.execute("""
                    INSERT INTO characters (name)
                    VALUES (?)
                """, (
                    elt,
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on tables movies, persons, characters
def import_cast(conn):
    TRANSAC_NAME = "cast"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    charactersDf = pd.read_csv(CSV_REPO + "characters.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in charactersDf.iterrows():
            if pd.isna(row["name"]) or row["name"] == "":
                continue
            try:
                c.execute("""
                    INSERT INTO cast(movie_id, person_id, character_id)
                    VALUES(?, ?, (SELECT character_id FROM characters WHERE name = ? COLLATE NOCASE LIMIT 1))
                """, (
                    row["mid"],
                    row["pid"],
                    row["name"]
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# No dependency
def import_genres(conn):
    TRANSAC_NAME = "genres"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    genresDf = pd.read_csv(CSV_REPO + "genres.csv")

    genresSerie = genresDf["genre"].drop_duplicates()

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, elt in genresSerie.items():
            if (pd.isna(elt) or elt == ""): # skip invalid values
                continue
            try:
                c.execute("""
                    INSERT INTO genres (genre_name)
                    VALUES (?)
                """, (
                    elt,
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)


# depends on tables genres and movies
def import_movie_genres(conn):
    TRANSAC_NAME = "movie_genres"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    genresDf = pd.read_csv(CSV_REPO + "genres.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in genresDf.iterrows():
            if pd.isna(row["genre"]) or row["genre"] == "":
                continue
            try:
                c.execute("""
                    INSERT INTO movie_genres(movie_id, genre_id)
                    VALUES(?, (SELECT genre_id FROM genres WHERE genre_name = ? COLLATE NOCASE LIMIT 1))
                """, (
                    row["mid"],
                    row["genre"]
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on tables persons and movies 
def import_knownformovies(conn):
    TRANSAC_NAME = "known_for_movies"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    knownDf = pd.read_csv(CSV_REPO + "knownformovies.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in knownDf.iterrows():
            try:
                
                c.execute("""
                    INSERT INTO known_for_movies (person_id, movie_id)
                    VALUES (?, ?)
                """, (
                    row["pid"],
                    row["mid"]
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# No dependency
def import_movies(conn):
    TRANSAC_NAME = "movies"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    moviesDf = pd.read_csv(CSV_REPO + "movies.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in moviesDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO movies (movie_id, year, runtime_minutes)
                    VALUES (?, ?, ?)
                """, (
                    row["mid"],
                    int(row["startYear"]) if not pd.isna(row["startYear"]) else None,
                    int(row["runtimeMinutes"]) if not pd.isna(row["runtimeMinutes"]) else None,
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# No dependency
def import_persons(conn):
    TRANSAC_NAME = "persons"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    personsDf = pd.read_csv(CSV_REPO + "persons.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in personsDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO persons (person_id, name, birth_year, death_year)
                    VALUES (?, ?, ?, ?)
                """, (
                    row["pid"],
                    row["primaryName"] if not pd.isna(row["primaryName"]) and not row["primaryName"] == "" else None,
                    int(row["birthYear"]) if not pd.isna(row["birthYear"]) else None,
                    int(row["deathYear"]) if not pd.isna(row["deathYear"]) else None
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# No dependency
def import_professions(conn):
    TRANSAC_NAME = "professions"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    professionsDf = pd.read_csv(CSV_REPO + "professions.csv")

    professionsSerie = professionsDf["jobName"].drop_duplicates()
    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, elt in professionsSerie.items():
            if (pd.isna(elt) or elt == ""): # skip invalid values
                continue
            try:
                c.execute("""
                    INSERT INTO professions (job_name)
                    VALUES (?)
                """, (
                    elt,
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on tables persons and professions
def import_person_profession(conn):
    TRANSAC_NAME = "person_profession"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    professionsDf = pd.read_csv(CSV_REPO + "professions.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in professionsDf.iterrows():
            if pd.isna(row["jobName"]) or row["jobName"] == "":
                continue
            try:
                c.execute("""
                    INSERT INTO person_profession(person_id, profession_id)
                    VALUES(?, (SELECT profession_id FROM professions WHERE job_name = ? COLLATE NOCASE LIMIT 1))
                """, (
                    row["pid"],
                    row["jobName"]
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on table movies
def import_ratings(conn):
    TRANSAC_NAME = "ratings"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    ratingsDf = pd.read_csv(CSV_REPO + "ratings.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in ratingsDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO ratings (movie_id, average_rating, num_votes)
                    VALUES (?, ?, ?)
                """, (
                    row["mid"],
                    float(row["averageRating"]) if not pd.isna(row["averageRating"]) else None,
                    int(row["numVotes"]) if not pd.isna(row["numVotes"]) else None
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# No dependencies
def import_titles(conn):
    TRANSAC_NAME = "titles"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    titlesDf = pd.read_csv(CSV_REPO + "titles.csv")
    moviesDf = pd.read_csv(CSV_REPO + "movies.csv")

    titlesSerie = pd.concat([
        titlesDf["title"],
        moviesDf["originalTitle"],
        moviesDf["primaryTitle"]
    ]).drop_duplicates()

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, elt in titlesSerie.items():
            try:
                c.execute("""
                    INSERT INTO titles (title_name)
                    VALUES (?)
                """, (
                    elt,
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on movies and titles:
def import_movie_title(conn):
    TRANSAC_NAME = "movie_titles"
    insert_from_titles = 0
    failure_from_titles = 0

    c = conn.cursor()

    # création d'une lookup table pour éviter de faire un SELECT par ligne, la table titles étant très grosse
    moviesDf = pd.read_csv(CSV_REPO + "movies.csv")
    primaryLUT = set(zip(moviesDf["mid"], moviesDf["primaryTitle"]))
    originalLUT = set(zip(moviesDf["mid"], moviesDf["originalTitle"]))

    titlesDf = pd.read_csv(CSV_REPO + "titles.csv")
    titlesLUT = set(zip(titlesDf["mid"], titlesDf["title"]))
    try:

        # Complete with table movies
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in titlesDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO movie_titles (movie_id, title_id, is_primary, is_original)
                    VALUES (?, 
                        (SELECT title_id FROM titles WHERE title_name = ? COLLATE NOCASE),
                        ?, ?)
                """, (
                    row["mid"],
                    row["title"],
                    int((row["mid"], row["title"]) in primaryLUT),
                    int(bool(row["isOriginalTitle"]) or ((row["mid"], row["title"]) in originalLUT))
                ))
                insert_from_titles += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_from_titles += 1

        insert_from_movies = 0
        failure_from_movies = 0
        # populate with the titles in movies as well for completeness
        for _, row in moviesDf.iterrows():
            try:
                # store in variables to avoid looking up multiple times
                mid = row["mid"]
                # skip NaN or empty strings
                originalTitle = row["originalTitle"] if pd.notna(row["originalTitle"]) and row["originalTitle"] != "" else None
                primaryTitle  = row["primaryTitle"]  if pd.notna(row["primaryTitle"])  and row["primaryTitle"]  != "" else None


                # No need to update existing values, as boolean fields are already completed
                insertOriginal = False if (originalTitle is not None) and (mid, originalTitle) in titlesLUT else True
                insertPrimary = False if (primaryTitle is not None) and (mid, primaryTitle) in titlesLUT else True

                # Si les titres sont les mêmes, on insère une seule fois 
                if insertOriginal and insertPrimary and originalTitle == primaryTitle:
                    c.execute("""
                        INSERT INTO movie_titles (movie_id, title_id, is_original, is_primary)
                        VALUES (?, 
                            (SELECT title_id FROM titles WHERE title_name = ? COLLATE NOCASE),
                            1, 1)
                    """, (
                        mid,
                        originalTitle
                    ))

                    insert_from_movies += 1
                    continue
                if insertOriginal:
                    c.execute("""
                        INSERT INTO movie_titles (movie_id, title_id, is_original, is_primary)
                        VALUES (?, 
                            (SELECT title_id FROM titles WHERE title_name = ? COLLATE NOCASE),
                            1, 0)
                    """, (
                        mid,
                        originalTitle
                    ))
                    insert_from_movies += 1
                if insertPrimary:
                    c.execute("""
                    INSERT INTO movie_titles (movie_id, title_id, is_original, is_primary)
                        VALUES (?, 
                            (SELECT title_id FROM titles WHERE title_name = ? COLLATE NOCASE),
                            0, 1)
                """, (
                    mid,
                    primaryTitle
                ))
                    insert_from_movies += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_from_movies += 1

        #----------Nettoyage----------

        # Si plusieurs titres primaires ou secondaires: en garder 1 seul
        c.execute("""
            UPDATE movie_titles
            SET is_primary = 0
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM movie_titles
                WHERE is_primary = 1
                GROUP BY movie_id
            )
            AND is_primary = 1;
        """)

        c.execute("""
            UPDATE movie_titles
            SET is_original = 0
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM movie_titles
                WHERE is_original = 1
                GROUP BY movie_id
            )
            AND is_original = 1;
        """)

        # Si une seule des deux valeurs manquante: garder la deuxième
        c.execute("""
            UPDATE movie_titles
            SET is_original = 1
            WHERE rowid IN (
                SELECT mt1.rowid
                FROM movie_titles mt1
                WHERE mt1.is_primary = 1
                AND mt1.movie_id IN (
                    SELECT movie_id
                    FROM movie_titles
                    GROUP BY movie_id
                    HAVING SUM(is_original) = 0
                )
            );
        """)
        c.execute("""
            UPDATE movie_titles
            SET is_primary = 1
            WHERE rowid IN (
                SELECT mt1.rowid
                FROM movie_titles mt1
                WHERE mt1.is_original = 1
                AND mt1.movie_id IN (
                    SELECT movie_id
                    FROM movie_titles
                    GROUP BY movie_id
                    HAVING SUM(is_primary) = 0
                )
            );
        """)

        # Si les deux valeurs manquantes: prendre un titre au hasard
        c.execute("""
            UPDATE movie_titles
            SET is_primary = 1,
                is_original = 1
            WHERE rowid IN (
                SELECT rowid
                FROM movie_titles
                WHERE movie_id IN (
                    SELECT movie_id
                    FROM movie_titles
                    GROUP BY movie_id
                    HAVING SUM(is_primary) = 0 AND SUM(is_original) = 0
                )
                GROUP BY movie_id
            );
        """)

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")

        print("Lignes insérées depuis titles.csv: ", insert_from_titles)
        print("Lignes abandonnées depuis titles.csv: ", failure_from_titles)
        
        print("\nLignes insérées depuis movies.csv: ", insert_from_movies)
        print("Lignes abandonnées depuis movies.csv: ", failure_from_movies)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on movie_titles
def import_title_ordering(conn):
    TRANSAC_NAME = "title_ordering"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    titlesDf = pd.read_csv(CSV_REPO + "titles.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in titlesDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO title_ordering (movie_id, title_id, ordering, region, language, types, attributes)
                    VALUES (?, 
                        (SELECT title_id FROM titles WHERE title_name = ? COLLATE NOCASE),
                        ?, ?, ?, ?, ?)
                """, (
                    row["mid"],
                    row["title"],
                    row["ordering"],
                    row["region"] if not pd.isna(row["region"]) and not row["region"] == "" else None,
                    row["language"] if not pd.isna(row["language"]) and not row["language"] == "" else None,
                    row["types"] if not pd.isna(row["types"]) and not row["types"] == "" else None,
                    row["attributes"] if not pd.isna(row["attributes"]) and not row["attributes"] == "" else None
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)

# Depends on movies, persons and professions
def import_principals(conn):
    TRANSAC_NAME = "principals"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()
    principalsDf = pd.read_csv(CSV_REPO + "principals.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in principalsDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO principals (movie_id, ordering, person_id, profession_id, job)
                    VALUES (?, ?, ?,
                        (SELECT profession_id FROM professions WHERE job_name = ? COLLATE NOCASE LIMIT 1),
                    ?)
                """, (
                    row["mid"],
                    row["ordering"],
                    row["pid"],
                    row["category"] if not pd.isna(row["category"]) and not row["category"] == "" else None,
                    row["job"] if not pd.isna(row["job"]) and not row["job"] == "" else None
                ))
                insert_cnt += 1
            except sqlite3.IntegrityError as e:
                # Skip invalid rows
                failure_cnt += 1

        conn.commit()
        print("Transaction " + TRANSAC_NAME + " terminée avec succès: ")
        print("Lignes insérées: ", insert_cnt)
        print("Lignes abandonnées: ", failure_cnt)

    except sqlite3.Error as e:
        conn.rollback()
        print("!!! TRANSACTION Echouée !!!:\n", e)


with sqlite3.connect(DB_PATH) as conn:
    # No dependencies
    import_movies(conn)
    import_persons(conn)
    import_genres(conn)
    import_characters(conn)
    import_professions(conn)
    import_titles(conn)

    # depends on movies
    import_ratings(conn)

    # depends on movies and genres
    import_movie_genres(conn)
    # depends on movies and titles
    import_movie_title(conn)
    # depends on persons and professions
    import_person_profession(conn)

    # depends on movie_titles
    import_title_ordering(conn)

    # depends on persons, movies, professions and characters
    import_knownformovies(conn)
    import_cast(conn)
    import_principals(conn)
