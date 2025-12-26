from pymongo import MongoClient

def query_movies_complete_from_flat(db, movie_id=None):
    """
    Returns the movie complete documents fetched from the flat collections
    Args:
        db: MongoDB database ('imdb')
        movie_id: If given a value, query the document of said movie only
    """

    pipeline = [
        {"$lookup": {
            "from": "movie_titles",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": 
                    {"$eq": ["$movie_id", "$$mid"]},
                    "is_primary": True
                }},
                {"$lookup": {
                    "from": "titles",
                    "localField": "title_id",
                    "foreignField": "title_id",
                    "as": "t"
                }},
                {"$unwind": "$t"},
                {"$project": {
                    "_id": 0,
                    "title": "$t.title_name"
                }}
            ],
            "as": "pt"
        }},
        {"$unwind": "$pt"},
        {"$lookup": {
            "from": "title_ordering",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$lookup": {
                    "from": "titles",
                    "localField": "title_id",
                    "foreignField": "title_id",
                    "as": "t"
                }},
                {"$unwind": "$t"},
                {"$project": {
                    "_id": 0,
                    "region": 1,
                    "title": "$t.title_name"
                }}
            ],
            "as": "titles"
        }},
        {"$lookup": {
            "from": "movie_genres",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$lookup": {
                    "from": "genres",
                    "localField": "genre_id",
                    "foreignField": "genre_id",
                    "as": "g"
                }},
                {"$unwind": "$g"},
                {"$project": {
                    "_id": 0,
                    "genre": "$g.genre_name"
                }}
            ],
            "as": "g"
        }},
        {"$lookup": {
            "from": "ratings",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$project": {
                    "_id": 0,
                    "average": "$average_rating",
                    "votes": "$num_votes"
                }}
            ],
            "as": "rating"
        }},
        {"$unwind": "$rating"},
        {"$lookup": {
            "from": "principals",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$lookup": {
                    "from": "professions",
                    "localField": "profession_id",
                    "foreignField": "profession_id",
                    "as": "pr"
                }},
                {"$unwind": "$pr"},
                {"$match": {"pr.job_name": "director"}},
                {"$lookup": {
                    "from": "persons",
                    "localField": "person_id",
                    "foreignField": "person_id",
                    "as": "pe"
                }},
                {"$unwind": "$pe"},
                {"$project": {
                    "_id": 0,
                    "person_id": 1,
                    "name": "$pe.name"
                }}
            ],
            "as": "directors"
        }},
        {"$lookup": {
            "from": "cast",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$lookup": {
                    "from": "principals",
                    "let": {
                        "mid": "$movie_id",
                        "pid": "$person_id"
                    },
                    "pipeline": [
                        {"$match": {"$expr": {"$and": [
                            {"$eq": ["$movie_id", "$$mid"]},
                            {"$eq": ["$person_id", "$$pid"]}
                        ]}}}
                    ],
                    "as": "pr"
                }},
                {"$unwind": "$pr"},
                {"$lookup": {
                    "from": "characters",
                    "localField": "character_id",
                    "foreignField": "character_id",
                    "as": "c"
                }},
                {"$unwind": "$c"},
                {"$lookup": {
                    "from": "persons",
                    "localField": "person_id",
                    "foreignField": "person_id",
                    "as": "pe"
                }},
                {"$unwind": "$pe"},
                {"$group": {
                    "_id": "$person_id",
                    "name": {"$first": "$pe.name"},
                    "characters": {"$addToSet": "$c.name"},
                    "ordering": {"$first": "$pr.ordering"}
                }},
                {"$project": {
                    "_id": 0,
                    "person_id": "$_id",
                    "name": 1,
                    "characters": 1,
                    "ordering": 1
                }}
            ],
            "as": "cast"
        }},
        {"$lookup": {
            "from": "principals",
            "let": {"mid": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
                {"$lookup": {
                    "from": "professions",
                    "localField": "profession_id",
                    "foreignField": "profession_id",
                    "as": "pr"
                }},
                {"$unwind": "$pr"},
                {"$match": {"pr.job_name": "writer"}},
                {"$lookup": {
                    "from": "persons",
                    "localField": "person_id",
                    "foreignField": "person_id",
                    "as": "pe"
                }},
                {"$unwind": "$pe"},
                {"$project": {
                    "_id": 0,
                    "person_id": 1,
                    "name": "$pe.name",
                    "category": "$job"
                }}
            ],
            "as": "writers"
        }},
        {"$project": {
            "_id": "$movie_id",
            "title": "$pt.title",
            "year": 1,
            "runtime": "$runtime_minutes",
            "genres": "$g.genre",
            "rating": 1,
            "directors": 1,
            "cast": 1,
            "writers": 1,
            "titles": 1
        }},
        {"$limit": 370}
    ]

    if movie_id:
        pipeline.insert(0, {"$match": {"movie_id": movie_id}})
    return [r for r in db["movies"].aggregate(pipeline)]

def create_movies_complete(db):
    """
    Creates the movies_complete collection in the 'db' database
    Args:
        db: MongoDB database ('imdb')
    The imdb database must be correctly populated before calling this function
    """
    dependencies = [
        "movies",
        "ratings",
        "movie_genres",
        "genres",
        "cast",
        "principals",
        "persons",
        "person_profession",
        "characters",
        "movie_titles",
        "titles"
    ]

    # Ensures that the collection isn't created incomplete
    collection_list = db.list_collection_names()
    for d in dependencies:
        if not d in collection_list:
            print("Can not create movies_complete without collection: ", d)
            exit()

    db.create_collection("movies_complete")
    res = db["movies_complete"].insert_many(query_movies_complete_from_flat(db))

    print("Collection movies_complete created succesfully")
    print("Inserted documents: ", len(res.inserted_ids))


with MongoClient('mongodb://localhost:27017/') as client:
    db = client["imdb"]

    create_movies_complete(db)
