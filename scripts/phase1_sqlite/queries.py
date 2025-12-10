def query_actor_filmography(conn, actor_name: str, explain: bool=False) -> list:
    """
    Retourne la filmographie d'un acteur.
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l'acteur (ex: "Tom Hanks")
    Returns:
        Liste de tuples (titre, année, personnage, note)
    SQL utilisé:
        SELECT ... FROM ... JOIN ... LEFT JOIN ... WHERE ... LIKE ... AND ... ORDER BY ... DESC
    """
    
    sql = """
    SELECT t.title_name, m.year, c.name, r.average_rating
    FROM movies m
    JOIN principals p ON m.movie_id = p.movie_id
    JOIN persons pe ON p.person_id = pe.person_id
    JOIN movie_titles mt ON m.movie_id = mt.movie_id
    JOIN titles t ON mt.title_id = t.title_id
    LEFT JOIN cast ca ON pe.person_id = ca.person_id AND m.movie_id = ca.movie_id
    LEFT JOIN characters c ON c.character_id = ca.character_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE pe.name LIKE ?
    AND mt.is_primary = TRUE
    ORDER BY m.year DESC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

def query_top_N_movies(conn, genre: str, start_year: int, end_year: int, N: int, explain: bool=False) -> list:
    """
    Retourne les N meilleurs films d'un genre sur une période donnée selon leurs notes moyennes.
    Args:
        conn: Connexion SQLite
        genre: Genre considéré (ex: "Comedy")
        start_year: Année de début de la période (inclue)
        end_year: Année de fin de la période (inclue)
        N: Nombre de films selectionnés
    Returns:
        Liste de N tuples (movie_id, titre, année, note, nombre_de_votants)
    SQL utilisé:
        SELECT ... FROM ... JOIN ... WHERE ... LIKE ... AND ... ORDER BY ... DESC ...LIMIT
    """
    sql = """
    SELECT m.movie_id, t.title_name, m.year, r.average_rating, r.num_votes
    FROM movies m
    JOIN movie_titles mt ON m.movie_id = mt.movie_id
    JOIN titles t ON mt.title_id = t.title_id
    JOIN movie_genres mg ON m.movie_id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.genre_id
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE g.genre_name LIKE ?
    AND m.year >= ?
    AND m.year <= ?
    AND mt.is_primary = TRUE
    ORDER BY r.average_rating DESC
    LIMIT ?
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql, (f'%{genre}%', start_year, end_year, N)).fetchall()

def query_multi_role_actors(conn, explain: bool=False) -> list:
    """
    Retourne tous les acteurs ayant joués plusieurs personnages dans un même film, triés par nombre de roles.
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (person_id, nom de l'acteur, movie_id, nom du film, nombre de roles dans le film)
    SQL utilisé:
        SELECT ... COUNT ... FROM ... JOIN ... WHERE ... like ... GROUP BY ... HAVING ... COUNT ... ORDER BY
    """
    sql = """
    SELECT pe.person_id, pe.name, m.movie_id, t.title_name, COUNT(ca.character_id)
    FROM persons pe
    JOIN cast ca ON pe.person_id = ca.person_id
    JOIN movies m ON ca.movie_id = m.movie_id
    JOIN movie_titles mt on m.movie_id = mt.movie_id
    JOIN titles t ON mt.title_id = t.title_id
    WHERE mt.is_primary = TRUE
    GROUP BY ca.person_id, pe.name, m.movie_id, t.title_name
    HAVING COUNT(ca.character_id) > 1
    ORDER BY COUNT(ca.character_id) DESC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql).fetchall()

def query_collaborations(conn, actor_name, explain: bool=False) -> list:
    """
    Retourne tous les réalisateurs ayant travaillé avec un acteur, et le nombre de films réalisés ensemble
    Args:
        conn: Connexion SQLite
        actor_name: Prénom et nom de l'acteur
    Returns:
        Liste de tuples (nom du directeur, nombre de films)
    SQL utilisé:
        SELECT ... COUNT ... FROM ... JOIN ... LEFT JOIN ... WHERE ... IN ... GROUP BY ...COUNT ... DISTINCT ... ORDER BY ... DESC
    """
    sql = """
    SELECT pe.name, COUNT(pr.movie_id)
    FROM persons pe
    JOIN principals pr ON pe.person_id = pr.person_id
    LEFT JOIN professions p ON pr.profession_id = p.profession_id
    WHERE p.job_name LIKE 'director'
    AND pr.movie_id IN (
        SELECT pr.movie_id
        FROM persons pe
        JOIN principals pr ON pe.person_id = pr.person_id
        LEFT JOIN professions p ON pr.profession_id = p.profession_id
        WHERE pe.name LIKE ?
        AND p.job_name LIKE 'actor'
    )
    GROUP BY pe.name
    ORDER BY COUNT( DISTINCT(pr.movie_id)) DESC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

def query_popular_genres(conn, explain: bool=False) -> list:
    """
    Retourne les genres ayant une note moyenne > 7.0 et plus de 50 films
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (nom du genre, note moyenne, nombre de films)
    SQL utilisé:
        SELECT ... AVG ... FROM ... JOIN ...GROUP BY ... HAVING ... COUNT ... ORDER BY ... DESC
    """
    sql = """
    SELECT g.genre_name, AVG(r.average_rating), COUNT(mg.movie_id)
    FROM genres g
    JOIN movie_genres mg ON g.genre_id = mg.genre_id
    JOIN movies m ON mg.movie_id = m.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY g.genre_id, g.genre_name
    HAVING AVG(r.average_rating) > 7.0
    AND COUNT(mg.movie_id) > 50
    ORDER BY AVG(r.average_rating) DESC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql).fetchall()

def query_career_evolution(conn, actor_name, explain: bool=False) -> list:
    """
    Retourne le nombre de films dans lequel l'acteur a joué, par décennie avec leur note moyenne.
    Args:
        conn: Connexion SQLite
        actor_name: Prénom et nom de l'acteur
    Returns:
        Liste de tuples (décennie, nombre de films, note moyenne)
    SQL utilisé:
        WITH ... AS ... SELECT ... COUNT ... DISTINCT ... AVG ... FROM ...
        JOIN ... LEFT JOIN ... WHERE ...GROUP BY ...ORDER BY ... DESC
    """
    sql = """
    WITH cte AS (
        SELECT movie_id, year - (year % 10) AS decade 
        FROM movies
    )
    SELECT cte.decade, COUNT(DISTINCT(cte.movie_id)), AVG(r.average_rating)
    FROM cte
    JOIN cast ca ON cte.movie_id = ca.movie_id
    JOIN persons pe ON ca.person_id = pe.person_id
    LEFT JOIN ratings r ON cte.movie_id = r.movie_id
    WHERE pe.name LIKE ?
    GROUP BY cte.decade
    ORDER BY cte.decade DESC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

def query_genre_ranking(conn, explain: bool=False) -> list:
    """
    Retourne pour chaque genre les 3 meilleurs film, classés selon leurs notes, avec leur rang
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (genre, film, rang dans le genre)
    SQL utilisé:
        WITH ... AS ... RANK() ... OVER ... PARTITION BY ... ORDER BY ... DESC ...
        SELECT ... FROM ... JOIN ... WHERE
    """
    sql = """
    WITH cte AS (
        SELECT g.genre_name, t.title_name, RANK() OVER (
            PARTITION BY g.genre_id
            ORDER BY r.average_rating DESC
        ) AS rank
        FROM genres g
        JOIN movie_genres mg ON g.genre_id = mg.genre_id
        JOIN movies m ON mg.movie_id = m.movie_id
        JOIN movie_titles mt ON m.movie_id = mt.movie_id
        JOIN titles t ON mt.title_id = t.title_id
        JOIN ratings r ON m.movie_id = r.movie_id
        WHERE mt.is_primary = TRUE
    )
    SELECT genre_name, title_name, rank
    FROM cte
    WHERE rank <= 3
    ORDER BY genre_name
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql).fetchall()

def query_propulsated_careers(conn, explain: bool=False) -> list:
    """
    Retourne toutes les personnes ayant vues leurs carrières propulsées grace à un film
    particulier, le critère utilisé est le nombre de votes des films dans lequel elles ont joué
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (nom de la personne, titre du film qui les a fait connaître, année du film)
    SQL utilisé:
        WITH ... AS ... SELECT ... FROM ... ROW_NUMBER() ... OVER ... 
        PARTITION BY ... ORDER BY ... ASC ... JOIN ... LEFT JOIN ... WHERE
    """
    sql = """
    WITH first_hit AS ( 
        SELECT person_id, movie_id, year FROM (
            SELECT pe.person_id, m.movie_id, m.year, ROW_NUMBER() OVER (
                PARTITION BY pe.person_id
                ORDER BY m.year ASC
            ) as hit_num, r.num_votes
            FROM persons pe
            JOIN principals pr ON pe.person_id = pr.person_id
            JOIN movies m ON pr.movie_id = m.movie_id
            JOIN ratings r ON m.movie_id = r.movie_id
            WHERE r.num_votes > 200000
        ) WHERE hit_num = 1
    ), last_non_hit AS (
        SELECT person_id, movie_id, year FROM (
            SELECT pe.person_id, m.movie_id, m.year, ROW_NUMBER() OVER (
                PARTITION BY pe.person_id
                ORDER BY m.year DESC
            ) as nhit_num, r.num_votes
            FROM persons pe
            JOIN principals pr ON pe.person_id = pr.person_id
            JOIN movies m ON pr.movie_id = m.movie_id
            JOIN ratings r ON m.movie_id = r.movie_id
            WHERE r.num_votes < 200000
        ) WHERE nhit_num = 1)

    SELECT pe.name, t.title_name, fh.year
    FROM first_hit fh
    LEFT JOIN last_non_hit lnh ON fh.person_id = lnh.person_id
    JOIN persons pe ON fh.person_id = pe.person_id
    JOIN movie_titles mt ON fh.movie_id = mt.movie_id
    JOIN titles t ON mt.title_id = t.title_id
    WHERE (lnh.year IS NULL OR fh.year > lnh.year)
    AND mt.is_primary = TRUE
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql).fetchall()

# Requête libre
def query_children_stars(conn, explain: bool=False) -> list:
    """
    Retourne tous les acteurs ayant joué dans un film populaire (> 200000 votes) avant leurs 18 ans,
    triés par acteur et l'âge qu'ils avaient quand ils ont joués dans le film.
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (nom de l'acteur, film, nombre de votes, âge lors du film)
    SQL utilisé:
        WITH ... AS ... SELECT ... FROM ... JOIN ... WHERE ... ORDER BY ... ASC
    """
    sql = """
    WITH cte AS (
        SELECT pe.name, t.title_name, r.num_votes, (m.year - pe.birth_year) as age
        FROM cast ca
        JOIN movies m ON ca.movie_id = m.movie_id
        JOIN movie_titles mt ON m.movie_id = mt.movie_id
        JOIN titles t ON mt.title_id = t.title_id
        JOIN ratings r ON m.movie_id = r.movie_id
        JOIN persons pe ON ca.person_id = pe.person_id
        WHERE r.num_votes > 200000
        AND mt.is_primary = TRUE
    )
    SELECT name, title_name, num_votes, age
    FROM cte
    WHERE age < 18
    ORDER BY name, age ASC
    """
    if explain:
        sql = "EXPLAIN QUERY PLAN " + sql
    return conn.execute(sql).fetchall()