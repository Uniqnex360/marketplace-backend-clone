from django.urls import path
from clickhouse.views import (
    create_order_list_schema_clickhouse,
    migrate_order_list_to_clickhouse_view,
    fetchAllorders_clickhouse,
)

urlpatterns = [
    # General Urls
    path(
        "create_order_list_schema_clickhouse/",
        create_order_list_schema_clickhouse,
        name="create_order_list_schema_clickhouse",
    ),
    path(
        "migrate_order_list_to_clickhouse_view/",
        migrate_order_list_to_clickhouse_view,
        name="migrate_order_list_to_clickhouse_view",
    ),
    path(
        "fetchAllorders_clickhouse/",
        fetchAllorders_clickhouse,
        name="fetchAllorders_clickhouse",
    ),
]
