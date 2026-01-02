from django.urls import path
from . import views

urlpatterns = [
    path('', views.home_view),
    path('movies/<str:movie_id>', views.movie_complete_view),
    path('movies', views.pages_view),
    path('search', views.search_view),
    path('stats', views.stats_view),
]