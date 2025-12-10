import sqlite3
import time
from queries import *

def print_exec_time(conn, query, query_name: str, *args) -> None:
    """
    Affiche le temps d'exécution de la requête spécifiée.
    Args:
        conn: Connexion SQLite
        query: fonction de la requête
        query_name: nom de la requête
        *args: Arguments à passer à la requête
    """
    t_ini = time.time()
    query(conn, *args)
    t_fin = time.time()
    print(query_name + ": ", t_fin - t_ini, "secondes.")


# Affiche le temps d'éxecution de toutes les requêtes
def print_all_query_times(conn):
    print_exec_time(conn, query_actor_filmography, "filmographie", "brad pitt")
    print_exec_time(conn, query_top_N_movies, "top N movies", "Comedy", 2000, 2010, 15)
    print_exec_time(conn, query_multi_role_actors, "multi_role_actors")
    print_exec_time(conn, query_collaborations, "collaborations","Brad Pitt")
    print_exec_time(conn, query_popular_genres, "genres populaires")
    print_exec_time(conn, query_career_evolution, "evolution de carriere", "Brad pitt")
    print_exec_time(conn, query_genre_ranking, "classement par genre")
    print_exec_time(conn, query_propulsated_careers, "carriere propulsee")
    print_exec_time(conn, query_children_stars, "enfants star")

with sqlite3.connect("../../data/imdb.db") as conn:
    print_all_query_times(conn)
    # print(query_multi_role_actors(conn, explain=True))
    # print(query_collaborations(conn, "brad Pitt", explain=True))
    # print(query_popular_genres(conn, explain=True))
    # print(query_career_evolution(conn, "brad pitt", explain=True))
    # print(query_genre_ranking(conn, explain=True))
    # print(query_propulsated_careers(conn, explain=True))
    # print(query_children_stars(conn, explain=True))