from django.urls import path

from clickhouse.views import clickhouse_test_url, sandbox_clickhouse, get_all_order_ids
from clickhouse.helpers import (
    migrate_mongo_order_item_to_clickhouse,
    get_metrics_by_date_range_clickhouse,
    create_fact_order_items_table,
    drop_fact_order_items_table,
    truncate_fact_order_items_table,
    all_market_place_data_clickhouse,
)

urlpatterns = [
    # General Urls
    path("clickhouse_test_url/", clickhouse_test_url, name="clickhouse_test_url"),
    path("sandbox_clickhouse/", sandbox_clickhouse, name="sandbox_clickhouse"),
    path("get_all_order_ids/", get_all_order_ids, name="get_all_order_ids"),
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
    path(
        "all_market_place_data_clickhouse/",
        all_market_place_data_clickhouse,
        name="all_market_place_data_clickhouse",
    ),
    #create index urls
    path(
        "create_fact_order_items_table/",
        create_fact_order_items_table,
        name="create_fact_order_items_table",
    ),
    # delete tables
    path(
        "drop_fact_order_items_table/",
        drop_fact_order_items_table,
        name="drop_fact_order_items_table",
    ),
    #trucate tables
    path(
        "truncate_fact_order_items_table/",
        truncate_fact_order_items_table,
        name="truncate_fact_order_items_table",
    ),
]
