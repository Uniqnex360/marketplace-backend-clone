import time
import pytz
from pytz import timezone
from datetime import datetime

from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from clickhouse.config import client
from omnisight.operations.helium_utils import get_date_range


def convertdateTotimezone(start_date, end_date, timezone_str):
    local_tz = pytz.timezone(timezone_str)
    naive_from_date = datetime.strptime(start_date, "%Y-%m-%d")
    naive_to_date = datetime.strptime(end_date, "%Y-%m-%d")
    naive_to_date = naive_to_date.replace(hour=23, minute=59, second=59)
    start_date = local_tz.localize(naive_from_date)
    end_date = local_tz.localize(naive_to_date)
    return start_date, end_date


import logging
import time
from datetime import timezone

logger = logging.getLogger("clickhouse_debug")
logger.setLevel(logging.INFO)


# @csrf_exempt
# def all_market_place_data_clickhouse(request):
#     overall_start = time.time()

#     json_request = JSONParser().parse(request)

#     marketplace_id = json_request.get("marketplace_id", None)
#     brand_id = json_request.get("brand_id", [])
#     product_id = json_request.get("product_id", [])
#     manufacturer_name = json_request.get("manufacturer_name", [])
#     fulfillment_channel = json_request.get("fulfillment_channel", None)
#     preset = json_request.get("preset")

#     start_date = json_request.get("start_date", None)
#     end_date = json_request.get("end_date", None)

#     logger.info("===== REQUEST RECEIVED =====")
#     logger.info(f"marketplace_id: {marketplace_id}")
#     logger.info(f"brand_id: {brand_id}")
#     logger.info(f"product_id: {product_id}")
#     logger.info(f"manufacturer_name: {manufacturer_name}")
#     logger.info(f"fulfillment_channel: {fulfillment_channel}")
#     logger.info(f"preset: {preset}")

#     # =========================================================
#     # DATE LOGIC (SAME AS YOUR STANDARD DASHBOARD LOGIC)
#     # =========================================================
#     def parse_date(d):
#         return datetime.strptime(d, "%d/%m/%Y").date()

#     if start_date and end_date:
#         from_date = parse_date(start_date)
#         to_date = parse_date(end_date)
#     else:
#         from_date, to_date = get_date_range(preset)

#     logger.info(f"from_date: {from_date}")
#     logger.info(f"to_date: {to_date}")

#     # Convert to ClickHouse-safe format (DATE ONLY)
#     from_date = from_date.strftime("%Y-%m-%d")
#     to_date = to_date.strftime("%Y-%m-%d")

#     # =========================================================
#     # FILTER BUILDER (CLICKHOUSE CLEAN VERSION)
#     # =========================================================
#     def build_filters():
#         filters = ["order_date_day BETWEEN {start:Date} AND {end:Date}"]

#         params = {
#             "start": from_date,
#             "end": to_date,
#         }

#         if marketplace_id and marketplace_id != "all":
#             filters.append("toString(marketplace_id) = {marketplace:String}")
#             params["marketplace"] = marketplace_id

#         if brand_id:
#             filters.append("brand_id IN {brand_ids:Array(String)}")
#             params["brand_ids"] = brand_id

#         if product_id:
#             filters.append("product_id IN {product_ids:Array(String)}")
#             params["product_ids"] = product_id

#         if manufacturer_name:
#             filters.append("manufacturer_name IN {manufacturers:Array(String)}")
#             params["manufacturers"] = manufacturer_name

#         if fulfillment_channel:
#             filters.append("fulfillment_channel = {channel:String}")
#             params["channel"] = fulfillment_channel

#         return " AND ".join(filters), params

#     where_clause, params = build_filters()

#     logger.info("===== WHERE CLAUSE =====")
#     logger.info(where_clause)

#     # =========================================================
#     # CURRENT TOTAL QUERY
#     # =========================================================
#     current_query = f"""
#     SELECT
#         sum(order_total) AS grossRevenue,
#         sum(cogs + referral_fee) AS expenses,
#         sum(quantity) AS unitsSold,
#         uniq(sku) AS skuCount,
#         sum(item_tax) AS tax_price,
#         sum(product_cost * quantity) AS total_cogs,
#         sum(shipping_price) AS shipping_cost,
#         sum(
#             item_price +
#             shipping_price +
#             promotion_discount +
#             vendor_funding -
#             (vendor_discount + ship_promotion_discount)
#         ) AS netProfit
#     FROM fact_order_items
#     WHERE {where_clause}
#     """

#     # =========================================================
#     # BREAKDOWN QUERY
#     # =========================================================
#     breakdown_query = f"""
#     SELECT
#         marketplace_id,
#         marketplace_name,
#         sum(order_total) AS grossRevenue,
#         sum(cogs + referral_fee) AS expenses,
#         sum(quantity) AS unitsSold,
#         sum(product_cost * quantity) AS total_cogs,

#         sum(
#             item_price +
#             shipping_price +
#             promotion_discount +
#             vendor_funding -
#             (vendor_discount + ship_promotion_discount)
#         ) AS netProfit,

#         round(
#             if(sum(order_total) = 0, 0,
#                 (
#                     sum(
#                         item_price +
#                         shipping_price +
#                         promotion_discount +
#                         vendor_funding -
#                         (vendor_discount + ship_promotion_discount)
#                     ) / sum(order_total)
#                 ) * 100
#             ),
#         2) AS margin

#     FROM fact_order_items
#     WHERE {where_clause}
#     GROUP BY marketplace_id, marketplace_name
#     ORDER BY grossRevenue DESC
#     """

#     logger.info("===== CURRENT QUERY =====")
#     logger.info(current_query)

#     logger.info("===== BREAKDOWN QUERY =====")
#     logger.info(breakdown_query)

#     # =========================================================
#     # EXECUTION
#     # =========================================================
#     t1 = time.time()
#     current_result = client.query(current_query, parameters=params)
#     logger.info(f"Current query time: {time.time() - t1:.2f}s")

#     t2 = time.time()
#     breakdown_result = client.query(breakdown_query, parameters=params)
#     logger.info(f"Breakdown query time: {time.time() - t2:.2f}s")

#     # =========================================================
#     # PARSE CURRENT
#     # =========================================================
#     def safe_first(result):
#         if result.result_rows:
#             return dict(zip(result.column_names, result.result_rows[0]))
#         return {}

#     all_marketplace = safe_first(current_result)

#     # =========================================================
#     # MARKETPLACE LIST
#     # =========================================================
#     marketplace_list = []

#     for row in breakdown_result.result_rows:
#         data = dict(zip(breakdown_result.column_names, row))

#         marketplace_list.append(
#             {
#                 "marketplace_id": data.get("marketplace_id"),
#                 "marketplace": data.get("marketplace_name", ""),
#                 "currency_list": [
#                     {
#                         "grossRevenue": data.get("grossRevenue", 0),
#                         "expenses": data.get("expenses", 0),
#                         "unitsSold": data.get("unitsSold", 0),
#                         "total_cogs": data.get("total_cogs", 0),
#                         "netProfit": data.get("netProfit", 0),
#                         "margin": data.get("margin", 0),
#                         "roi": data.get("roi", 0),
#                         "refunds": data.get("refunds", 0),
#                     }
#                 ],
#             }
#         )

#     # =========================================================
#     # RESPONSE (UNCHANGED SHAPE)
#     # =========================================================
#     response_data = {
#         "custom": {
#             "all_marketplace": all_marketplace,
#             "marketplace_list": marketplace_list,
#         },
#         "from_date": from_date,
#         "to_date": to_date,
#         "status": True,
#         "message": "success",
#     }

#     logger.info(f"TOTAL TIME: {time.time() - overall_start:.2f}s")

#     return response_data


@csrf_exempt
def all_market_place_data_clickhouse(request):
    overall_start = time.time()

    json_request = JSONParser().parse(request)

    marketplace_id = json_request.get("marketplace_id", None)
    brand_id = json_request.get("brand_id", [])
    product_id = json_request.get("product_id", [])
    manufacturer_name = json_request.get("manufacturer_name", [])
    fulfillment_channel = json_request.get("fulfillment_channel", None)
    preset = json_request.get("preset")
    country = json_request.get("country")

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)

    def parse_date(d):
        return datetime.strptime(d, "%d/%m/%Y").date()

    if start_date and end_date:
        from_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        from_date, to_date = get_date_range(preset)

    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    # =========================================================
    # FILTERS
    # =========================================================
    def build_filters():
        filters = [
            "order_status NOT IN ('Canceled', 'Cancelled')",
            "order_date_day BETWEEN {start:Date} AND {end:Date}",
        ]

        params = {
            "start": from_date,
            "end": to_date,
        }

        if marketplace_id and marketplace_id != "all":
            filters.append("toString(marketplace_id) = {marketplace:String}")
            params["marketplace"] = marketplace_id

        if brand_id:
            filters.append("brand_id IN {brand_ids:Array(String)}")
            params["brand_ids"] = brand_id

        if product_id:
            filters.append("product_id IN {product_ids:Array(String)}")
            params["product_ids"] = product_id

        if manufacturer_name:
            filters.append("manufacturer_name IN {manufacturers:Array(String)}")
            params["manufacturers"] = manufacturer_name

        if fulfillment_channel:
            filters.append("fulfillment_channel = {channel:String}")
            params["channel"] = fulfillment_channel

        if country:
            filters.append("country = {country:String}")
            params["country"] = country



        return " AND ".join(filters), params

    where_clause, params = build_filters()

    # =========================================================
    # SHARED NET PROFIT LOGIC (SAME AS GET_METRICS)
    # =========================================================
    def compute_finance(row):

        gross = row.get("grossRevenue", 0) or 0

        cogs = row.get("cogs", 0) or 0
        channel_fees = row.get("channel_fees", 0) or 0

        item_price = row.get("item_price", 0) or 0
        shipping_price = row.get("shipping_price", 0) or 0
        promotion_discount = row.get("promotion_discount", 0) or 0
        vendor_funding = row.get("vendor_funding", 0) or 0

        vendor_discount = row.get("vendor_discount", 0) or 0
        ship_promo = row.get("ship_promotion_discount", 0) or 0

        net_profit = (
            item_price + shipping_price + promotion_discount + vendor_funding
        ) - (channel_fees + cogs + vendor_discount + ship_promo)

        expenses = cogs + channel_fees

        return gross, cogs, channel_fees, net_profit, expenses

    # =========================================================
    # CURRENT QUERY (FIXED REVENUE SOURCE)
    # =========================================================
    current_query = f"""
    SELECT
        sum(gross_revenue) AS grossRevenue,

        sum(product_cost * quantity + merchant_shipment_cost) AS cogs,
        sum(referral_fee) AS channel_fees,

        sum(quantity) AS unitsSold,
        uniq(sku) AS skuCount,
        sum(item_tax) AS tax_price,

        sum(item_price) AS item_price,
        sum(shipping_price) AS shipping_price,
        sum(promotion_discount) AS promotion_discount,
        sum(vendor_funding) AS vendor_funding,
        sum(vendor_discount) AS vendor_discount,
        sum(ship_promotion_discount) AS ship_promotion_discount

    FROM fact_order_items
    WHERE {where_clause}
    """

    # =========================================================
    # BREAKDOWN QUERY (FIXED REVENUE SOURCE)
    # =========================================================
    breakdown_query = f"""
    SELECT
        marketplace_id,
        marketplace_name,

        sum(gross_revenue) AS grossRevenue,

        sum(product_cost * quantity + merchant_shipment_cost) AS cogs,
        sum(referral_fee) AS channel_fees,

        sum(quantity) AS unitsSold,

        sum(item_price) AS item_price,
        sum(shipping_price) AS shipping_price,
        sum(promotion_discount) AS promotion_discount,
        sum(vendor_funding) AS vendor_funding,
        sum(vendor_discount) AS vendor_discount,
        sum(ship_promotion_discount) AS ship_promotion_discount

    FROM fact_order_items
    WHERE {where_clause}
    GROUP BY marketplace_id, marketplace_name
    ORDER BY grossRevenue DESC
    """

    current_result = client.query(current_query, parameters=params)
    breakdown_result = client.query(breakdown_query, parameters=params)

    def safe_first(result):
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return {}

    # =========================================================
    # ALL MARKETPLACE (NOW MATCHES GET_METRICS)
    # =========================================================
    all_marketplace_raw = safe_first(current_result)

    gross, cogs, channel_fees, net_profit, expenses = compute_finance(
        all_marketplace_raw
    )

    all_marketplace = {
        **all_marketplace_raw,
        "expenses": expenses,
        "netProfit": net_profit,
        "margin": round((net_profit / gross) * 100, 2) if gross else 0,
        "roi": round((net_profit / expenses) * 100, 2) if expenses else 0,
    }

    # =========================================================
    # MARKETPLACE LIST
    # =========================================================
    marketplace_list = []

    for row in breakdown_result.result_rows:
        data = dict(zip(breakdown_result.column_names, row))

        gross, cogs, channel_fees, net_profit, expenses = compute_finance(data)

        marketplace_list.append(
            {
                "marketplace_id": data.get("marketplace_id"),
                "marketplace": data.get("marketplace_name", ""),
                "currency_list": [
                    {
                        "grossRevenue": gross,
                        "expenses": expenses,
                        "unitsSold": data.get("unitsSold", 0),
                        "total_cogs": cogs,
                        "netProfit": net_profit,
                        "margin": round((net_profit / gross) * 100, 2) if gross else 0,
                        "roi": (
                            round((net_profit / expenses) * 100, 2) if expenses else 0
                        ),
                    }
                ],
            }
        )

    # =========================================================
    # RESPONSE (UNCHANGED)
    # =========================================================
    response_data = {
        "custom": {
            "all_marketplace": all_marketplace,
            "marketplace_list": marketplace_list,
        },
        "from_date": from_date,
        "to_date": to_date,
        "status": True,
        "message": "success",
    }

    return response_data

