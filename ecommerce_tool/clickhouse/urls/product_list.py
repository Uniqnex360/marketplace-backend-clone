from django.urls import path
from clickhouse.views import (
    create_product_list_schema_clickhouse,
    migrate_product_list_to_clickhouse_view,
    getProductList_clickhouse,
)

urlpatterns = [
    # General Urls
    path(
        "create_product_list_schema_clickhouse/",
        create_product_list_schema_clickhouse,
        name="create_product_list_schema_clickhouse",
    ),
    path(
        "migrate_product_list_to_clickhouse_view/",
        migrate_product_list_to_clickhouse_view,
        name="migrate_product_list_to_clickhouse_view",
    ),
    path(
        "getProductList_clickhouse/",
        getProductList_clickhouse,
        name="getProductList_clickhouse",
    ),
]
