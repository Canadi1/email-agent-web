from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('progress/<str:command_id>/', views.progress_stream, name='progress_stream'),
    path('auth/google/login/', views.google_login, name='google_login'),
    path('auth/google/callback/', views.google_callback, name='google_callback'),
    path('auth/google/logout/', views.google_logout, name='google_logout'),
]