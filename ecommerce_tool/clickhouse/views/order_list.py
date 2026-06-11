import re
import pytz
import json
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt

from clickhouse.config import client
from clickhouse.helpers import migrate_order_list_to_clickhouse_task
from omnisight.models import Marketplace, custom_order


@csrf_exempt
def create_order_list_schema_clickhouse(request):
    if request.method != "POST":
        return {"error": "Only POST allowed"}

    try:
        query = """
            CREATE TABLE order_list
            (
                id String,
                purchase_order_id String,

                order_date DateTime64(3, 'UTC'),

                order_status LowCardinality(String),

                order_total Decimal(18, 2),

                currency LowCardinality(String),

                items_order_quantity UInt32,

                marketplace_id String,

                marketplace_name String,

                country LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(order_date)
            ORDER BY (
                order_date,
                marketplace_id,
                purchase_order_id
            );
        """
        client.command(query)

        return {
            "success": True,
            "message": "order list clickhouse schema created successfully",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@csrf_exempt
def migrate_order_list_to_clickhouse_view(request):
    """migrate data required for order list to Mongo DB to Clickhouse"""

    data = migrate_order_list_to_clickhouse_task()
    return data


@csrf_exempt
def fetchAllorders_clickhouse(request):
    data = {}

    json_request = json.loads(request.body or "{}")
    print(json_request, type(json_request))

    limit = int(json_request.get("limit", 100))
    skip = int(json_request.get("skip", 0))

    market_place_id = json_request.get("marketplace_id")

    sort_by = json_request.get("sort_by")
    sort_by_value = json_request.get("sort_by_value", -1)

    search_query = json_request.get("search_query")
    order_status_filter = json_request.get("order_status")

    if search_query and search_query.strip():
        skip = 0
        if not limit or limit > 100:
            limit = 25

    pacific_tz = pytz.timezone("US/Pacific")
    current_time_pacific = datetime.now(pacific_tz)

    # -------------------------------------------------------
    # CUSTOM ORDERS (UNCHANGED)
    # -------------------------------------------------------
    if market_place_id == "custom":

        base_pipeline = []

        if search_query and search_query.strip():
            safe_search = re.escape(search_query.strip())

            base_pipeline.append(
                {
                    "$match": {
                        "order_id": {
                            "$regex": safe_search,
                            "$options": "i",
                        }
                    }
                }
            )

        count_pipeline = base_pipeline + [{"$count": "total_count"}]

        total_count_result = list(custom_order.objects.aggregate(*count_pipeline))

        total_count = total_count_result[0]["total_count"] if total_count_result else 0

        pipeline = base_pipeline + [
            {
                "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "order_id": {"$ifNull": ["$order_id", ""]},
                    "customer_name": {"$ifNull": ["$customer_name", ""]},
                    "shipping_address": {"$ifNull": ["$shipping_address", ""]},
                    "total_quantity": {"$ifNull": ["$total_quantity", 0]},
                    "total_price": {
                        "$ifNull": [
                            {"$round": ["$total_price", 2]},
                            0.0,
                        ]
                    },
                    "purchase_order_date": {"$ifNull": ["$purchase_order_date", None]},
                    "expected_delivery_date": {
                        "$ifNull": ["$expected_delivery_date", None]
                    },
                    "order_status": "$order_status",
                    "currency": {"$ifNull": ["$currency", "USD"]},
                }
            }
        ]

        if sort_by:
            pipeline.append({"$sort": {sort_by: int(sort_by_value)}})
        else:
            pipeline.append({"$sort": {"id": -1}})

        pipeline.extend(
            [
                {"$skip": skip},
                {"$limit": limit},
            ]
        )

        data["manual_orders"] = list(custom_order.objects.aggregate(*pipeline))

        data["total_count"] = total_count
        data["status"] = "custom"

    # -------------------------------------------------------
    # CLICKHOUSE ORDERS
    # -------------------------------------------------------
    else:

        where_conditions = ["1 = 1"]

        if market_place_id and market_place_id != "all":
            where_conditions.append(f"marketplace_id = '{market_place_id}'")

        if order_status_filter and order_status_filter != "all":
            where_conditions.append(f"order_status = '{order_status_filter}'")

        if search_query and search_query.strip():
            safe_search = search_query.replace("'", "\\'")
            where_conditions.append(f"purchase_order_id ILIKE '%{safe_search}%'")

        where_clause = " AND ".join(where_conditions)

        # --------------------------
        # Total Count
        # --------------------------

        count_query = f"""
        SELECT count(*) AS total_count
        FROM order_list
        WHERE {where_clause}
        """

        total_count = client.query(count_query).result_rows[0][0]

        # --------------------------
        # Sorting
        # --------------------------

        allowed_sort_fields = {
            "purchase_order_id",
            "order_date",
            "order_status",
            "order_total",
            "currency",
            "items_order_quantity",
            "marketplace_name",
        }

        if sort_by in allowed_sort_fields:
            sort_field = sort_by
            sort_direction = "ASC" if int(sort_by_value) == 1 else "DESC"
        else:
            sort_field = "order_date"
            sort_direction = "DESC"

        # --------------------------
        # Fetch Orders
        # --------------------------

        query = f"""
        SELECT
            id,
            purchase_order_id,
            order_date,
            order_status,
            order_total,
            currency,
            marketplace_name,
            items_order_quantity
        FROM order_list
        WHERE {where_clause}
        ORDER BY {sort_field} {sort_direction}
        LIMIT {limit}
        OFFSET {skip}
        """

        rows = client.query(query).result_rows

        orders = []

        for row in rows:

            order_date = row[2]

            if order_date:

                if order_date.tzinfo is None:
                    order_date = pytz.utc.localize(order_date)

                order_date = order_date.astimezone(pacific_tz)

            if order_date and order_date <= current_time_pacific:

                orders.append(
                    {
                        "id": row[0],
                        "purchase_order_id": row[1],
                        "order_date": order_date,
                        "order_status": row[3],
                        "order_total": float(row[4]),
                        "currency": row[5],
                        "marketplace_name": row[6],
                        "items_order_quantity": row[7],
                    }
                )

        data["orders"] = orders
        data["total_count"] = total_count
        data["status"] = ""

    # -------------------------------------------------------
    # MARKETPLACE LIST (UNCHANGED)
    # -------------------------------------------------------

    marketplace_pipeline = [
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "name": 1,
                "image_url": 1,
            }
        }
    ]

    data["marketplace_list"] = list(
        Marketplace.objects.aggregate(*marketplace_pipeline)
    )

    return data
