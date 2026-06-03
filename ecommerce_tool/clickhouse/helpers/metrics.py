from datetime import timedelta, datetime
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from clickhouse.config import client
from omnisight.operations.helium_utils import get_date_range


@csrf_exempt
def create_fact_order_items_table(request):
    if request.method != "POST":
        return {"error": "Only POST allowed"}

    try:
        query = """
        CREATE TABLE IF NOT EXISTS fact_order_items
        (
            order_id String,
            order_item_id String,
            purchase_order_id String,

            order_date DateTime,
            order_date_day Date,

            marketplace_id String,
            marketplace_name String,
            fulfillment_channel String,
            brand_id String,
            manufacturer_name String,
            product_id String,
            sku String,
            category String,

            country String,
            channel String,

            order_total Float64,
            shipping_price Float64,
            merchant_shipment_cost Float64,
            order_status LowCardinality(String),

            item_price Float64,
            item_tax Float64,
            quantity UInt32,

            promotion_discount Float64,
            ship_promotion_discount Float64,

            product_cost Float64,
            cogs Float64,
            referral_fee Float64,
            vendor_funding Float64,
            vendor_discount Float64,

            gross_revenue Float64,
            net_item_revenue Float64,

            currency LowCardinality(String)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(order_date)
        ORDER BY (order_date_day, marketplace_id, product_id, order_id)
        """

        client.command(query)

        return {
            "success": True,
            "message": "fact_order_items table created successfully",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@csrf_exempt
def truncate_fact_order_items_table(request):
    if request.method != "POST":
        return {"error": "Only POST allowed"}

    try:
        client.command("TRUNCATE TABLE IF EXISTS fact_order_items")

        return {
            "success": True,
            "message": "fact_order_items table truncated successfully",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@csrf_exempt
def drop_fact_order_items_table(request):
    if request.method != "POST":
        return {"error": "Only POST allowed"}

    try:
        client.command("DROP TABLE IF EXISTS fact_order_items")

        return {
            "success": True,
            "message": "fact_order_items table dropped successfully",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@csrf_exempt
def get_metrics_by_date_range_clickhouse(request):

    json_request = JSONParser().parse(request)

    country = json_request.get("country", "US")
    brand_ids = json_request.get("brand_id", [])
    preset = json_request.get("preset")

    start_date_str = json_request.get("start_date")
    end_date_str = json_request.get("end_date")

    if start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, "%d/%m/%Y").date()
        end_date = datetime.strptime(end_date_str, "%d/%m/%Y").date()
    else:
        start_date, end_date = get_date_range(preset)

    # -------------------------
    # ClickHouse SAFE DATE FORMAT
    # -------------------------
    def to_clickhouse_date(d):
        return d.strftime("%Y-%m-%d")

    # -------------------------
    # WHERE BUILDER (SAFE)
    # -------------------------
    where_clauses = ["order_date_day BETWEEN {start:Date} AND {end:Date}"]

    params = {
        "start": to_clickhouse_date(start_date),
        "end": to_clickhouse_date(end_date),
    }

    # IMPORTANT: only add if value exists and is meaningful
    if country:
        where_clauses.append("country = {country:String}")
        params["country"] = country

    if brand_ids:
        where_clauses.append("brand_id IN {brand_ids:Array(String)}")
        params["brand_ids"] = brand_ids

    where_sql = " AND ".join(where_clauses)

    # -------------------------
    # GRAPH QUERY
    # -------------------------
    graph_query = f"""
        SELECT
            order_date_day,
            sum(gross_revenue) AS gross_revenue_with_tax
        FROM fact_order_items
        WHERE {where_sql}
        GROUP BY order_date_day
        ORDER BY order_date_day
    """

    graph_rows = client.query(graph_query, parameters=params).result_rows

    graph_data = {
        r[0].strftime("%B %d, %Y").lower(): {
            "gross_revenue_with_tax": round(r[1], 2)
        }
        for r in graph_rows
    }

    # -------------------------
    # METRICS QUERY
    # -------------------------
    metrics_query = f"""
        SELECT
            sum(gross_revenue) AS gross_revenue_with_tax,
            sum(cogs) AS total_cogs,
            sum(referral_fee) AS total_referral_fee,
            sum(quantity) AS total_units,
            sum(item_tax) AS total_tax,
            sum(product_cost * quantity) AS product_cost,
            uniq(order_id) AS total_orders,

            sum(merchant_shipment_cost) AS shipping_cost,
            sum(shipping_price) AS shipping_price,
            sum(vendor_funding) AS vendor_funding,
            sum(vendor_discount) AS vendor_discount

        FROM fact_order_items
        WHERE {where_sql}
    """

    target = client.query(metrics_query, parameters=params).result_rows[0]

    # -------------------------
    # PREVIOUS PERIOD (SAFE)
    # -------------------------
    previous_start = start_date - timedelta(days=1)
    previous_end = end_date - timedelta(days=1)

    previous_params = {
        "start": to_clickhouse_date(previous_start),
        "end": to_clickhouse_date(previous_end),
    }

    # IMPORTANT: keep same filters consistent
    if country:
        previous_params["country"] = country

    if brand_ids:
        previous_params["brand_ids"] = brand_ids

    previous = client.query(metrics_query, parameters=previous_params).result_rows[0]

    # -------------------------
    # RESPONSE BUILDER
    # -------------------------
    def build_metrics(row):

        gross = row[0] or 0
        cogs = row[1] or 0
        referral_fee = row[2] or 0
        units = row[3] or 0
        tax = row[4] or 0
        product_cost = row[5] or 0
        orders = row[6] or 0

        shipping_cost = row[7] or 0
        shipping_price = row[8] or 0
        vendor_funding = row[9] or 0
        vendor_discount = row[10] or 0

        expense = (
            cogs
            + referral_fee
            + shipping_cost
            + shipping_price
            - vendor_funding
            - vendor_discount
        )

        net_profit = gross - expense

        return {
            "gross_revenue_with_tax": gross,
            "total_cogs": cogs,
            "referral_fee": referral_fee,
            "total_units": units,
            "total_tax": tax,
            "product_cost": product_cost,
            "total_orders": orders,
            "total_expense": expense,
            "net_profit": net_profit,
            "margin": round((net_profit / gross) * 100, 2) if gross else 0,
        }

    metrics = {
        "graph_data": graph_data,
        "targeted": build_metrics(target),
        "previous": build_metrics(previous),
    }

    metrics["difference"] = {
        "gross_revenue_with_tax": metrics["targeted"]["gross_revenue_with_tax"]
        - metrics["previous"]["gross_revenue_with_tax"],
        "total_cogs": metrics["targeted"]["total_cogs"]
        - metrics["previous"]["total_cogs"],
        "total_tax": metrics["targeted"]["total_tax"]
        - metrics["previous"]["total_tax"],
        "total_orders": metrics["targeted"]["total_orders"]
        - metrics["previous"]["total_orders"],
        "net_profit": metrics["targeted"]["net_profit"]
        - metrics["previous"]["net_profit"],
    }

    return metrics