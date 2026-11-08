from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('classify/', views.classify, name='classify'),
    path('export-excel/', views.export_excel, name='export_excel'),
]
