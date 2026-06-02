from django.urls import path

from clickhouse.views import clickhouse_test_url, sandbox_clickhouse
from clickhouse.helpers import (
    migrate_mongo_order_item_to_clickhouse,
    get_metrics_by_date_range_clickhouse,
)

urlpatterns = [
    # General Urls
    path("clickhouse_test_url/", clickhouse_test_url, name="clickhouse_test_url"),
    path("sandbox_clickhouse/", sandbox_clickhouse, name="sandbox_clickhouse"),
    # migration urls
    path(
        "migrate_mongo_order_item_to_clickhouse/",
        migrate_mongo_order_item_to_clickhouse,
        name="migrate_mongo_order_item_to_clickhouse",
    ),
    # query urls
    path(
        "get_metrics_by_date_range_clickhouse/",
        get_metrics_by_date_range_clickhouse,
        name="get_metrics_by_date_range_clickhouse",
    ),
]
