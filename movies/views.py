from django.shortcuts import render
from django.http import Http404

from .services.mongo_service import *
from .services.sqlite_service import *

def home_view(request):
    top_movies = get_top_N_movies(10)
    stats = get_basic_stats()
    rd_movies = get_random_movies(10)

    context = {
        "top_movies": top_movies,
        "stats": stats,
        "rd_movies": rd_movies
    }

    return render(request, "movies/home.html", context)

def pages_view(request):
    page = int(request.GET.get('page', 1))

    # filters
    genre    = request.GET.get('genre', "")
    min_year = int(request.GET.get('min_year', 0) or 0)
    max_year = int(request.GET.get('max_year', 9000) or 9000)
    min_note = float(request.GET.get('min_note', 0) or 0)
    filters  = [genre, min_year, max_year, min_note]

    # sorting
    sort  = request.GET.get('sort', "title")
    order = request.GET.get('order', "ASC")
    
    film_list  = get_film_list(page, filters, sort, order)
    genre_list = get_genre_list()

    film_list_size = get_film_list_size(filters)
    page_count     = (film_list_size + 19) // 20
    page_nav_range = range(max(1, page - 3), min(page_count + 1, page + 5))

    queryDict = request.GET.copy()
    queryDict.pop("page", None)
    base_qs = queryDict.urlencode()

    context = {
        "films": film_list,
        "genres": genre_list,
        "page_num": page,
        "page_count": page_count,
        "page_nav_range": page_nav_range,
        "base_qs": base_qs
    }

    return render(request, "movies/list.html", context)

def movie_complete_view(request, movie_id):
    movie_comp = get_movie_complete(movie_id)

    if not movie_comp:
        return render(request, "movies/movie_not_found.html", {"movie_id": movie_id})
    movie_comp["mid"] = movie_comp["_id"] # django doesn't accept underscores before variables
    movie_comp.pop("_id")

    directors     = [d["person_id"] for d in movie_comp["directors"]]
    directors_rec = get_rd_movies_from_directors(directors, 10, movie_id)
    genres_rec    = get_rd_movies_from_genres(movie_comp["genres"], 10, movie_id)

    movie_comp["directors_rec"] = directors_rec
    movie_comp["genres_rec"]    = genres_rec
    return render(request, "movies/detail.html", movie_comp)

def search_view(request):
    query      = request.GET.get('q', '')
    query_type = request.GET.get('type', 'title')
    page       = int(request.GET.get('page', 1) or 1)
    search_result = []
    res_size = 0
    match query_type:
        case 'title':
            search_result = search_movies_from_title(query, page)
            res_size = get_list_from_title_size(query)
        case 'person':
            search_result = search_movies_from_person(query, page)
            res_size = get_list_from_person_size(query)
        case _:
            raise Http404('bad query')
    
    page_count     = (res_size + 19) // 20
    page_nav_range = range(max(1, page - 3), min(page_count + 1, page + 5))

    queryDict = request.GET.copy()
    queryDict.pop("page", None)
    base_qs = queryDict.urlencode()

    context = {
        "films": search_result,
        "page_num": page,
        "page_count": page_count,
        "page_nav_range": page_nav_range,
        "base_qs": base_qs
    }

    return render(request, "movies/search.html", context)

def stats_view(request):
    context = {
        "movies_by_genre":  get_movies_count_by_genre(),
        "movies_by_decade": get_movies_count_by_decade(),
        "ratings_dist":     get_ratings_distribution(1),
        "prolific_actors":  get_top_N_prolific_actors(10)
    }

    return render(request, "movies/stats.html", context)