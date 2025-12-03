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
                    INSERT INTO genres (genre)
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
                    VALUES(?, (SELECT genre_id FROM genres WHERE genre = ? COLLATE NOCASE LIMIT 1))
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
                    INSERT INTO movies (movie_id, isAdult, startYear, endYear, runtimeMinutes)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["mid"],
                    bool(row["isAdult"]) if not pd.isna(row["isAdult"]) else None,
                    int(row["startYear"]) if not pd.isna(row["startYear"]) else None,
                    int(row["endYear"]) if not pd.isna(row["endYear"]) else None,
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
                    INSERT INTO persons (person_id, primaryName, birthYear, deathYear)
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
                    INSERT INTO professions (jobName)
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
                    VALUES(?, (SELECT profession_id FROM professions WHERE jobName = ? COLLATE NOCASE LIMIT 1))
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
                    INSERT INTO ratings (movie_id, averageRating, numVotes)
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

# Depends on table movies
def import_titles(conn):
    TRANSAC_NAME = "titles"
    insert_cnt = 0
    failure_cnt = 0

    c = conn.cursor()

    # création d'une lookup table pour éviter de faire un SELECT par ligne, la tabel titles étant très grosse
    moviesDf = pd.read_csv(CSV_REPO + "movies.csv")
    primaryLUT = set(zip(moviesDf["mid"], moviesDf["primaryTitle"]))
                     
    titlesDf = pd.read_csv(CSV_REPO + "titles.csv")

    try:
        print("--------------TRANSACTION " + TRANSAC_NAME + "--------------")
        conn.execute("BEGIN TRANSACTION")

        for _, row in titlesDf.iterrows():
            try:
                c.execute("""
                    INSERT INTO titles (
                        movie_id,
                        ordering,
                        title,
                        region,
                        language,
                        types,
                        attributes,
                        isOriginal,
                        isPrimary
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["mid"],
                    row["ordering"],
                    row["title"] if not pd.isna(row["title"]) and not row["title"] == "" else None,
                    row["region"] if not pd.isna(row["region"]) and not row["region"] == "" else None,
                    row["language"] if not pd.isna(row["language"]) and not row["language"] == "" else None,
                    row["types"] if not pd.isna(row["types"]) and not row["types"] == "" else None,
                    row["attributes"] if not pd.isna(row["attributes"]) and not row["attributes"] == "" else None,
                    int(bool(row["isOriginalTitle"])) if not pd.isna(row["isOriginalTitle"]) else None,
                    # Check if the title is the primary for this movie to populate isPrimary
                    int((row["mid"], row["title"]) in primaryLUT) 
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
                    INSERT INTO principals (movie_id, ordering, person_id, profession_id, category)
                    VALUES (?, ?, 
                        (SELECT profession_id FROM professions WHERE jobName = ? COLLATE NOCASE LIMIT 1),
                    ?, ?)
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
    import_movies(conn)
    import_persons(conn)
    import_genres(conn)
    import_characters(conn)
    import_professions(conn)
    import_ratings(conn)
    import_movie_genres(conn)
    import_titles(conn)
    import_person_profession(conn)
    import_knownformovies(conn)
    import_cast(conn)
    import_principals(conn)
