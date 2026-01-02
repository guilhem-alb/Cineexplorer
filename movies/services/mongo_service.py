from django.conf import settings
from pymongo import MongoClient

from scripts.phase2_mongodb.queries_mongo import *

_client = MongoClient(settings.DATABASES["mongo"]["HOST"])
_db = _client[settings.DATABASES["mongo"]["NAME"]]

def get_movie_complete(movie_id: str):
    return _db.movies_complete.find_one({"_id": movie_id})
    
def get_movie_and_title(movie_id: str):
    pipeline = [
        {"$match": {"_id": movie_id}},
        {"$project": {
            "_id": 1,
            "title": 1
        }}
    ]
    return _db.movies_complete.aggregate(pipeline)

def get_rd_movies_from_directors(directors: list, N: int, original_id: str) -> list:
    pipeline = [
        {"$match": {
            "directors.person_id": {"$in": directors},
            "_id": {"$ne": original_id}
        }},
        {"$sample": {"size": N}},
        {"$project": {
            "_id": 0,
            "mid": "$_id",
            "title": 1
        }}
    ]

    return _db.movies_complete.aggregate(pipeline)

def get_rd_movies_from_genres(genres: list, N: int, original_id: str) -> list:
    pipeline = [
        {"$match": {
            "genres": {"$in": genres},
            "_id": {"$ne": original_id}
        }},
        {"$sample": {"size": N}},
        {"$project": {
            "_id": 0,
            "mid": "$_id",
            "title": 1
        }}
    ]

    return _db.movies_complete.aggregate(pipeline)

