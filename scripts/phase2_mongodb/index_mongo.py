from pymongo import MongoClient, ASCENDING

with MongoClient('mongodb://localhost:27017/') as client:
    db = client["imdb"]

    db["cast"].create_index("movie_id")
    db["cast"].create_index("person_id")

    db["principals"].create_index("movie_id")
    db["principals"].create_index("person_id")
    db["principals"].create_index(
        ("person_id", ASCENDING),
        ("profession_id", ASCENDING),
        ("movie_id", ASCENDING)
    )

    db["movies"].create_index("movie_id")

    db["persons"].create_index("person_id")

    db["movie_titles"].create_index("movie_id")
    db["movie_titles"].create_index("title_id")

    db["title_ordering"].create_index("movie_id")
    db["title_ordering"].create_index("title_id")

    db["titles"].create_index("title_id")

    db["movie_genres"].create_index("movie_id")
    db["movie_genres"].create_index("genre_id")

    db["genres"].create_index("genre_id")

    db["ratings"].create_index("movie_id")
