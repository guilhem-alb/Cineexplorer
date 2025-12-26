from pymongo import MongoClient
from queries_mongo import *
import time

def print_exec_time(db, query, query_name: str, *args) -> None:
    """
    Affiche le temps d'exécution de la requête spécifiée.
    Args:
        db: Base de données mongoDB
        query: fonction de la requête
        query_name: nom de la requête
        *args: Arguments à passer à la requête
    """
    t_ini = time.time()
    query(db, *args)
    t_fin = time.time()
    print(query_name + ": ", t_fin - t_ini, "secondes.")

def print_all_query_times(db):
    print_exec_time(db, mg_query_actor_filmography, "filmographie", "brad pitt")
    print_exec_time(db, mg_query_top_N_movies, "top N movies", "Comedy", 2000, 2010, 15)
    print_exec_time(db, mg_query_multi_role_actors, "multi_role_actors")
    print_exec_time(db, mg_query_collaborations, "collaborations","Brad Pitt")
    print_exec_time(db, mg_query_popular_genres, "genres populaires")
    print_exec_time(db, mg_query_career_evolution, "evolution de carriere", "Brad pitt")
    print_exec_time(db, mg_query_genre_ranking, "classement par genre")
    print_exec_time(db, mg_query_propulsated_careers, "carriere propulsee")
    print_exec_time(db, mg_query_children_stars, "enfants star")


with MongoClient('mongodb://localhost:27017/') as client:
    db = client["imdb"]

    print_all_query_times(db)
