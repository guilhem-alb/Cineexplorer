from django.db import connection

from typing import Literal

def get_top_N_movies(N: int) -> list:
    with connection.cursor() as c:
        sql = """
            WITH rm AS(SELECT m.movie_id, r.average_rating, r.num_votes,  RANK() OVER (
                    ORDER BY r.average_rating DESC
                ) as rank
                FROM movies m
                JOIN ratings r on m.movie_id = r.movie_id
                WHERE r.num_votes > 100000
            )

            SELECT rm.movie_id, t.title_name, rm.rank, rm.average_rating, rm.num_votes
            FROM rm
            JOIN movie_titles mt ON rm.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id 
            WHERE rank <= ?
            AND mt.is_primary = TRUE
            LIMIT 10
        """

        res = c.execute(sql, (N,)).fetchall()
    res = [{"mid": t[0], "title": t[1], "rank": t[2], "rating": t[3], "votes": t[4]} for t in res]

    return res

def get_basic_stats() -> dict:
    with connection.cursor() as c:
        movieCnt = c.execute("SELECT COUNT(*) FROM MOVIES").fetchone()[0]
        actorCnt = c.execute("""
            SELECT COUNT(*) FROM persons pe
            JOIN person_profession pp ON pe.person_id = pp.person_id
            JOIN professions pr on pp.profession_id = pr.profession_id 
            WHERE pr.job_name LIKE 'actor'
        """).fetchone()[0]
        directorCnt = c.execute("""
            SELECT COUNT(*) FROM persons pe
            JOIN person_profession pp ON pe.person_id = pp.person_id
            JOIN professions pr on pp.profession_id = pr.profession_id 
            WHERE pr.job_name LIKE 'director'
        """).fetchone()[0]
    return {"movieCnt": movieCnt, "actorCnt": actorCnt, "directorCnt": directorCnt}

def get_random_movies(N: int) -> list:
    with connection.cursor() as c:

        sql = f"""
            SELECT m.movie_id, t.title_name
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id 
            WHERE mt.is_primary = TRUE
            ORDER BY RANDOM()
            LIMIT {N}
        """
        res = c.execute(sql).fetchall()

    res = [{"mid": t[0], "title": t[1]} for t in res]
    return res

def get_film_list(
        page: int,
        filters: list,
        sort_on: Literal["title", "year", "note"],
        order_by: Literal["ASC", "DESC"]
    ) -> list:
    match sort_on:
        case "title":
            sort_on = "t.title_name"
        case "year":
            sort_on = "m.year"
        case "note":
            sort_on = "r.average_rating"
        case _:
            raise(ValueError("wrong sort_on value in get_film_list: ", sort_on))
        
    if(order_by not in ["ASC", "DESC"]):
        raise(ValueError("wrong order_by value in get_film_list: ", order_by))
    
    offset = (page - 1) * 20
    with connection.cursor() as c:
        sql = f"""
            SELECT DISTINCT(m.movie_id), t.title_name, m.year, r.average_rating
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id 
            JOIN ratings r ON m.movie_id = r.movie_id
            JOIN movie_genres mg ON m.movie_id = mg.movie_id
            JOIN genres g ON mg.genre_id = g.genre_id
            WHERE g.genre_name LIKE ?
            AND m.year >= ?
            AND m.year <= ?
            AND r.average_rating >= ?
            AND mt.is_primary = TRUE
            ORDER BY {sort_on} {order_by}
            LIMIT 20 OFFSET ?
        """

        res = c.execute(sql, (f"%{filters[0]}%", filters[1], filters[2], filters[3], offset)).fetchall()
    res = [{"mid": t[0], "title": t[1], "year": t[2], "note": t[3]} for t in res]
    return res

def get_film_list_size(filters: list) -> int:
    with connection.cursor() as c:
        sql = """
            SELECT COUNT(DISTINCT(m.movie_id))
            FROM movies m
            JOIN ratings r ON m.movie_id = r.movie_id
            JOIN movie_genres mg ON m.movie_id = mg.movie_id
            JOIN genres g ON mg.genre_id = g.genre_id
            WHERE g.genre_name LIKE ?
            AND m.year >= ?
            AND m.year <= ?
            AND r.average_rating >= ?
        """
        res = c.execute(sql, (f"%{filters[0]}%", filters[1], filters[2], filters[3])).fetchone()[0]
    return res

def get_genre_list() -> list:
    with connection.cursor() as c:
        res = c.execute("SELECT genre_name FROM genres").fetchall()
    return [r[0] for r in res]

def search_movies_from_title(title: str, page: int) -> list:
    offset = (page - 1) * 20
    with connection.cursor() as c:
        sql = """
            SELECT DISTINCT(m.movie_id), t.title_name
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id
            WHERE mt.is_primary = TRUE
            AND t.title_name LIKE ?
            ORDER BY t.title_name ASC
            LIMIT 20 OFFSET ?
        """

        res = c.execute(sql, (f"%{title}%", offset)).fetchall()
    res = [{"mid": r[0], "title": r[1]} for r in res]
    return res

def get_list_from_title_size(title: str):
    with connection.cursor() as c:
        sql = """
            SELECT COUNT(DISTINCT(m.movie_id))
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id
            WHERE mt.is_primary = TRUE
            AND t.title_name LIKE ?
        """

        res = c.execute(sql, (f"%{title}%",)).fetchone()[0]
    return res

def search_movies_from_person(person_name: str, page: int) -> list:
    offset = (page - 1) * 20
    with connection.cursor() as c:
        sql = """
            SELECT DISTINCT(m.movie_id), t.title_name
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id
            JOIN principals pr ON m.movie_id = pr.movie_id
            JOIN persons pe ON pr.person_id = pe.person_id
            WHERE mt.is_primary = TRUE
            AND pe.name LIKE ?
            ORDER BY pe.name ASC
            LIMIT 20 OFFSET ?
        """

        res = c.execute(sql, (f"%{person_name}%", offset)).fetchall()
    res = [{"mid": r[0], "title": r[1]} for r in res]
    return res


def get_list_from_person_size(person_name: str):
    with connection.cursor() as c:
        sql = """
            SELECT COUNT(DISTINCT(m.movie_id))
            FROM movies m
            JOIN movie_titles mt ON m.movie_id = mt.movie_id
            JOIN titles t ON mt.title_id = t.title_id
            JOIN principals pr ON m.movie_id = pr.movie_id
            JOIN persons pe ON pr.person_id = pe.person_id
            WHERE mt.is_primary = TRUE
            AND pe.name LIKE ?
        """

        res = c.execute(sql, (f"%{person_name}%",)).fetchone()[0]
    return res

def get_movies_count_by_genre() -> dict:
    with connection.cursor() as c:
        sql = """
            SELECT COUNT(DISTINCT(mg.movie_id)), g.genre_name
            FROM movie_genres mg
            JOIN genres g ON mg.genre_id = g.genre_id
            GROUP BY g.genre_name
        """
        res = c.execute(sql).fetchall()
    res = [{"movie_count": r[0], "genre": r[1]} for r in res]
    return res

def get_movies_count_by_decade() -> dict:
    with connection.cursor() as c:
        sql = """
            WITH cte AS (
                SELECT movie_id, year - (year % 10) AS decade 
                FROM movies
            )
            SELECT COUNT(movie_id), decade
            FROM cte
            GROUP BY decade
        """
        res = c.execute(sql).fetchall()
    res = [{"movie_count": r[0], "decade": r[1]} for r in res]
    return res