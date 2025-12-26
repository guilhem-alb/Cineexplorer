def mg_query_actor_filmography(db, actor_name: str) -> list:
    """
    Retourne la filmographie d'un acteur.
    Args:
        db: Base de données MongoDB ('imdb')
        actor_name: Nom de l'acteur (ex: "Tom Hanks")
    """
    pipeline = [
        {"$match": {"name": {
            "$regex": actor_name,
            "$options": "i"
        }}},
        {"$lookup": {
            "from": "principals",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "pr"
        }},
        {"$unwind": "$pr"},
        {"$lookup": {
            "from": "movie_titles",
            "localField": "pr.movie_id",
            "foreignField": "movie_id",
            "as": "mt"
        }},
        {"$unwind": "$mt"},
        {"$match": {
            "mt.is_primary": True
        }},
        {"$lookup": {
            "from": "movies",
            "localField": "mt.movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$lookup": {
            "from": "cast",
            "let": {"person_id": "$person_id", "movie_id": "$mt.movie_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$person_id", "$$person_id"]},
                            {"$eq": ["$movie_id", "$$movie_id"]}
                        ]
                    }
                }}
            ],
            "as": "ca"
        }},
        {"$lookup": {
            "from": "characters",
            "localField": "ca.character_id",
            "foreignField": "character_id",
            "as": "c"
        }},
        {"$lookup": {
            "from": "ratings",
            "localField": "mt.movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$sort": {
            "m.year": -1
        }},
        {"$project": {
            "_id": 0,
            "title": "$t.title_name",
            "year": "$m.year",
            "character_name": "$c.name",
            "average_rating": "$r.average_rating"
        }}
    ]

    return [r for r in db["persons"].aggregate(pipeline)]

def mg_query_top_N_movies(db, genre: str, start_year: int, end_year: int, N: int) -> list:
    """
    Retourne les N meilleurs films d'un genre sur une période donnée selon leurs notes moyennes.
    Args:
        db: Base de données MongoDB ('imdb')
        genre: Genre considéré (ex: "Comedy")
        start_year: Année de début de la période (inclue)
        end_year: Année de fin de la période (inclue)
        N: Nombre de films selectionnés
    Returns:
        Liste de N dicts {movie_id, titre, année, note, nombre_de_votants}
    """
    pipeline = [
        {"$match": {
            "year" : {"$gte": start_year, "$lte": end_year}
        }},
        {"$lookup": {
            "from": "movie_genres",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "mg"
        }},
        {"$unwind": "$mg"},
        {"$lookup": {
            "from": "genres",
            "let": {"genre_id": "$mg.genre_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$eq" : ["$genre_id", "$$genre_id"]
                    }
                }},
                {"$match": {
                    "genre_name":{
                        "$regex": genre,
                        "$options": "i"
                    }
                }}
            ],
            "as": "g"
        }},
        {"$unwind": "$g"},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$sort": {"r.average_rating": -1}},
        {"$limit": N},
        {"$lookup": {
            "from": "movie_titles",
            "let": {"movie_id": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$movie_id"]}}},
                {"$match": {"is_primary": True}}
            ],
            "as": "mt"
        }},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$project": {
            "_id": 0,
            "movie_id": 1,
            "title": "$t.title_name",
            "year": 1,
            "average_rating": "$r.average_rating",
            "num_votes": "$r.num_votes"
        }}
    ]

    return [r for r in db["movies"].aggregate(pipeline)]


def mg_query_multi_role_actors(db) -> list:
    """
    Retourne tous les acteurs ayant joués plusieurs personnages dans un même film, triés par nombre de roles.
    Args:
        db: Base de données MongoDB ('imdb')
    Returns:
        Liste de dicts {person_id, nom de l'acteur, movie_id, nom du film, nombre de roles dans le film}
    """
    pipeline = [
        {"$group": {
            "_id": {
                "person_id": "$person_id",
                "movie_id": "$movie_id"
            },
            "charactersPlayed": {"$addToSet": "$character_id"}
        }},
        {"$addFields": {
            "charactersCnt": {"$size": "$charactersPlayed"}
        }},
        {"$match": {
            "charactersCnt": {"$gt": 1}
        }},
        {"$sort": {"charactersCnt": -1}},
        {"$lookup": {
            "from": "persons",
            "localField": "_id.person_id",
            "foreignField": "person_id",
            "as": "pe"
        }},
        {"$unwind": "$pe"},
        {"$lookup": {
            "from": "movie_titles",
            "localField": "_id.movie_id",
            "foreignField": "movie_id",
            "as": "mt"
        }},
        {"$unwind": "$mt"},
        {"$match": {"mt.is_primary": True}},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$project": {
            "_id": 0,
            "person_id": "$_id.person_id",
            "actor_name": "$pe.name",
            "movie_id": 1,
            "title": "$t.title_name",
            "charactersCnt": 1,
        }}
    ]

    return [r for r in db["cast"].aggregate(pipeline)]

def mg_query_collaborations(db, actor_name) -> list:
    """
    Retourne tous les réalisateurs ayant travaillé avec un acteur, et le nombre de films réalisés ensemble
    Args:
        db: Base de données MongoDB ('imdb')
        actor_name: Prénom et nom de l'acteur
    Returns:
        Liste de dicts {nom du directeur, nombre de films}
    """

    pmPipeline = [
        {"$match": {
            "name": {
                "$regex": actor_name,
                "$options": "i"
            }
        }},
        {"$lookup": {
            "from": "principals",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "pr"
        }},
        {"$lookup": {
            "from": "professions",
            "localField": "pr.profession_id",
            "foreignField": "profession_id",
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$match": {
            "p.job_name": {
                "$regex": "actor",
                "$options": "i"
            }
        }},
        {"$project": {
            "_id": 0,
            "movie_id": "$pr.movie_id"
        }}
    ]

    # array contenant les films joués par l'acteur
    playedMovies = next(db["persons"].aggregate(pmPipeline))["movie_id"]

    pipeline = [
        {"$match": {
            "movie_id": {"$in": playedMovies}
        }},
        {"$lookup": {
            "from": "professions",
            "localField": "profession_id",
            "foreignField": "profession_id",
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$match": {
            "p.job_name": {
                "$regex": "director",
                "$options": "i"
            }
        }},
        {"$lookup": {
            "from": "persons",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "pe"
        }},
        {"$unwind": "$pe"},
        {"$group": {
            "_id": {"person_id": "$person_id"},
            "name": {"$first": "$pe.name"},
            "movies": {"$addToSet": "$movie_id"}
        }},
        {"$addFields": {"movieCnt": {"$size": "$movies"}}},
        {"$sort": {"movieCnt": -1}},
        {"$project": {
            "_id": 0,
            "name": 1,
            "movieCnt": 1
        }}
    ]

    return [r for r in db["principals"].aggregate(pipeline)]

def mg_query_popular_genres(db) -> list:
    """
    Retourne les genres ayant une note moyenne > 7.0 et plus de 50 films
    Args:
        db: Base de données MongoDB ('imdb')
    Returns:
        Liste de dicts {nom du genre, note moyenne, nombre de films}
    """
    pipeline = [
        {"$lookup": {
            "from": "genres",
            "localField": "genre_id",
            "foreignField": "genre_id",
            "as": "g"
        }},
        {"$unwind": "$g"},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": "m.movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$group": {
            "_id": {"genre_id": "$genre_id"},
            "genre_name": {"$first": "$g.genre_name"},
            "avg_rating": {"$avg": "$r.average_rating"},
            "movies": {"$addToSet": "$movie_id"}
        }},
        {"$addFields": {
            "movieCnt": {"$size": "$movies"}
        }},
        {"$match": {
            "avg_rating": {"$gt": 7.0},
            "movieCnt": {"$gt": 50}
        }},
        {"$project": {
            "_id": 0,
            "genre_name": 1,
            "avg_rating": 1,
            "movieCnt": 1
        }}  
    ]

    return [r for r in db["movie_genres"].aggregate(pipeline)]

def mg_query_career_evolution(db, actor_name) -> list:
    """
    Retourne le nombre de films dans lequel l'acteur a joué, par décennie avec leur note moyenne.
    Args:
        db: Base de données MongoDB ('imdb')
        actor_name: Prénom et nom de l'acteur
    Returns:
        Liste de dicts {décennie, nombre de films, note moyenne}
    """
    pipeline = [
        {"$lookup": {
            "from": "cast",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "c"
        }},
        {"$unwind": "$c"},
        {"$lookup": {
            "from": "persons",
            "localField": "c.person_id",
            "foreignField": "person_id",
            "as": "pe"
        }},
        {"$unwind": "$pe"},
        {"$match": {
            "pe.name": {
                "$regex": actor_name,
                "$options": "i"
            }
        }},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$addFields": {
            "decade": {
                "$subtract": [
                    "$year",
                    {"$mod": [
                        "$year",
                        10
                    ]}
                ]
            }
        }},
        {"$group": {
            "_id": "$decade",
            "movie_list": {"$addToSet": "$movie_id"},
            "avg_rating": {"$avg": "$r.average_rating"}
        }},
        {"$addFields": {
            "movieCnt": {"$size": "$movie_list"}
        }},
        {"$project": {
            "_id": 0,
            "decade": "$_id",
            "movieCnt": 1,
            "avg_rating": 1
        }},
        {"$sort": {
            "decade": -1
        }}
    ]

    return [r for r in db["movies"].aggregate(pipeline)]

def mg_query_genre_ranking(db) -> list:
    """
    Retourne pour chaque genre les 3 meilleurs film, classés selon leurs notes, avec leur rang
    Args:
        db: Base de données MongoDB ('imdb')
    Returns:
        Liste de dicts {genre, film, rang dans le genre}
    """
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$setWindowFields": {
            "partitionBy": "$genre_id",
            "sortBy": {"r.average_rating": -1},
            "output": {
                "rank": {"$rank": {}}
            }
        }},
        {"$match": {
            "rank": {"$lte": 3}
        }},
        {"$lookup": {
            "from": "genres",
            "localField": "genre_id",
            "foreignField": "genre_id",
            "as": "g"
        }},
        {"$unwind": "$g"},
        {"$lookup": {
            "from": "movie_titles",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "mt"
        }},
        {"$unwind": "$mt"},
        {"$match": {"mt.is_primary": True}},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$sort": {"g.genre_name": 1}},
        {"$project": {
            "_id": 0,
            "genre": "$g.genre_name",
            "title": "$t.title_name",
            "rank": 1
        }}
    ]

    return [r for r in db["movie_genres"].aggregate(pipeline)]

def mg_query_propulsated_careers(db) -> list:
    """
    Retourne toutes les personnes ayant vues leurs carrières propulsées grace à un film
    particulier, le critère utilisé est le nombre de votes des films dans lequel elles ont joué
    Args:
        db: Base de données MongoDB ('imdb')
    Returns:
        Liste de dicts {nom de la personne, titre du film qui les a fait connaître, année du film}
    """
    fh_pipeline = [
        {"$match": {"$expr": {"$eq": ["$person_id", "$$person_id"]}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {
            "r.num_votes": {"$gt": 200000}
        }},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$sort": {"m.year": 1}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "movie_id": 1,
            "year": "$m.year"
        }}
    ]

    lnh_pipeline = [
        {"$match": {"$expr": {"$eq": ["$person_id", "$$person_id"]}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {
            "r.num_votes": {"$lt": 200000}
        }},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$sort": {"m.year": -1}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "movie_id": 1,
            "year": "$m.year"
        }}
    ]

    pipeline = [
        {"$lookup": {
            "from": "principals",
            "let": {"person_id": "$person_id"},
            "pipeline": fh_pipeline,
            "as": "fh"
        }},
        {"$unwind": "$fh"},
        {"$lookup": {
            "from": "principals",
            "let": {"person_id": "$person_id"},
            "pipeline": lnh_pipeline,
            "as": "lnh"
        }},
        {"$unwind": {
            "path": "$lnh",
            "preserveNullAndEmptyArrays": True
        }},
        {"$match": {
            "$or": [
                {"lnh": None},
                {"$expr": {"$gt": ["$fh.year", "$lnh.year"]}}
            ]
        }},
        {"$lookup": {
            "from": "movie_titles",
            "localField": "fh.movie_id",
            "foreignField": "movie_id",
            "as": "mt"
        }},
        {"$unwind": "$mt"},
        {"$match": {"mt.is_primary": True}},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$project": {
            "_id": 0,
            "name": 1,
            "first_hit_title": "$t.title_name",
            "first_hit_year": "$fh.year"
        }}
    ]
    return [r for r in db["persons"].aggregate(pipeline)]

# Requête libre
def mg_query_children_stars(db) -> list:
    """
    Retourne tous les acteurs ayant joué dans un film populaire (> 200000 votes) avant leurs 18 ans,
    triés par acteur et l'âge qu'ils avaient quand ils ont joués dans le film.
    Args:
        db: Base de données MongoDB ('imdb')
    Returns:
        Liste de dicts {nom de l'acteur, film, nombre de votes, âge lors du film}
    """
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {
            "r.num_votes": {"$gt": 200000}
        }},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "persons",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "pe"
        }},
        {"$unwind": "$pe"},
        {"$addFields": {
            "age": {
                "$subtract": ["$m.year", "$pe.birth_year"]
            }
        }},
        {"$match": {
            "age": {"$lt": 18}
        }},
        {"$lookup": {
            "from": "movie_titles",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "mt"
        }},
        {"$unwind": "$mt"},
        {"$match": {"mt.is_primary": True}},
        {"$lookup": {
            "from": "titles",
            "localField": "mt.title_id",
            "foreignField": "title_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$sort": {
            "pe.name": 1,
            "age": 1
        }},
        {"$project": {
            "_id": 0,
            "name": "$pe.name",
            "movie_title": "$t.title_name",
            "votes": "$r.num_votes",
            "age": 1
        }}
    ]

    return [r for r in db["cast"].aggregate(pipeline)]