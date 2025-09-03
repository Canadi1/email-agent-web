from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('progress/<str:command_id>/', views.progress_stream, name='progress_stream'),
]