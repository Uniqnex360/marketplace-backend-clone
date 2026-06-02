from datetime import timedelta, datetime
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from clickhouse.config import client


@csrf_exempt
def get_metrics_by_date_range_clickhouse(request):

    json_request = JSONParser().parse(request)

    marketplace_id = json_request.get("marketplace_id")
    start_date = datetime.strptime(json_request.get("start_date"), "%d/%m/%Y")
    end_date = datetime.strptime(json_request.get("end_date"), "%d/%m/%Y")
    timezone = "US/Pacific"

    # 1. GRAPH DATA (no threads needed)
    graph_query = """
        SELECT
            order_date_day,
            sum(gross_revenue) as gross_revenue_with_tax
        FROM fact_order_items
        WHERE order_date BETWEEN {start:DateTime} AND {end:DateTime}
        GROUP BY order_date_day
        ORDER BY order_date_day
    """

    graph_rows = client.query(
        graph_query, parameters={"start": start_date, "end": end_date}
    ).result_rows

    graph_data = {
        r[0].strftime("%B %d, %Y").lower(): {"gross_revenue_with_tax": round(r[1], 2)}
        for r in graph_rows
    }

    # 2. METRICS (target + previous)
    metrics_query = """
        SELECT
            sum(gross_revenue) as gross_revenue_with_tax,
            sum(cogs) as total_cogs,
            sum(referral_fee) as referral_fee,
            sum(quantity) as total_units,
            sum(item_tax) as total_tax,
            sum(product_cost * quantity) as product_cost,
            countDistinct(order_id) as total_orders
        FROM fact_order_items
        WHERE order_date BETWEEN {start:DateTime} AND {end:DateTime}
    """

    target = client.query(
        metrics_query, {"start": start_date, "end": end_date}
    ).result_rows[0]

    previous = client.query(
        metrics_query,
        {"start": start_date - timedelta(days=1), "end": end_date - timedelta(days=1)},
    ).result_rows[0]

    def build_metrics(row):
        return {
            "gross_revenue_with_tax": row[0],
            "total_cogs": row[1],
            "referral_fee": row[2],
            "total_units": row[3],
            "total_tax": row[4],
            "product_cost": row[5],
            "total_orders": row[6],
            "margin": 0,
            "net_profit": 0,
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
    }

    return metrics


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
            fulfillment_channel String,
            brand_id String,
            manufacturer_name String,
            product_id String,
            sku String,
            category String,

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
