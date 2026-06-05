from django.urls import path
from clickhouse.views import migrate_temp_collections

urlpatterns = [
    # General Urls
    path(
        "migrate_temp_collections/",
        migrate_temp_collections,
        name="migrate_temp_collections",
    ),
]
