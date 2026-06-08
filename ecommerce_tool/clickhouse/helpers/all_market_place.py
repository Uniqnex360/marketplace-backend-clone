import time
import pytz
from pytz import timezone

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

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)

    logger.info("===== REQUEST RECEIVED =====")
    logger.info(f"marketplace_id: {marketplace_id}")
    logger.info(f"brand_id: {brand_id}")
    logger.info(f"product_id: {product_id}")
    logger.info(f"manufacturer_name: {manufacturer_name}")
    logger.info(f"fulfillment_channel: {fulfillment_channel}")
    logger.info(f"preset: {preset}")
    logger.info(f"start_date: {start_date}")
    logger.info(f"end_date: {end_date}")

    # -------------------------
    # TIMEZONE CONVERSION
    # -------------------------
    def to_clickhouse_dt(dt):
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if start_date:
        from_date, to_date = convertdateTotimezone(start_date, end_date, "US/Pacific")
    else:
        from_date, to_date = get_date_range(preset, "US/Pacific")

    logger.info(f"Converted from_date: {from_date}")
    logger.info(f"Converted to_date: {to_date}")

    # -------------------------
    # FILTER BUILDER
    # -------------------------
    def build_filters(from_d, to_d):
        filters = [
            f"order_date >= toDateTime('{to_clickhouse_dt(from_d)}')",
            f"order_date <= toDateTime('{to_clickhouse_dt(to_d)}')",
        ]

        if marketplace_id and marketplace_id != "all":
            filters.append(f"toString(marketplace_id) = '{marketplace_id}'")

        if brand_id:
            if len(brand_id) > 1:
                filters.append(f"brand_id IN {tuple(brand_id)}")
            else:
                filters.append(f"brand_id = '{brand_id[0]}'")

        if product_id:
            if len(product_id) > 1:
                filters.append(f"product_id IN {tuple(product_id)}")
            else:
                filters.append(f"product_id = '{product_id[0]}'")

        if manufacturer_name:
            if len(manufacturer_name) > 1:
                filters.append(f"manufacturer_name IN {tuple(manufacturer_name)}")
            else:
                filters.append(f"manufacturer_name = '{manufacturer_name[0]}'")

        if fulfillment_channel:
            filters.append(f"fulfillment_channel = '{fulfillment_channel}'")

        return " AND ".join(filters)

    where_clause = build_filters(from_date, to_date)

    logger.info("===== WHERE CLAUSE =====")
    logger.info(where_clause)

    # -------------------------
    # CURRENT QUERY (TOTAL)
    # -------------------------
    current_query = f"""
    SELECT
        sum(order_total) AS grossRevenue,
        sum(cogs + referral_fee) AS expenses,
        sum(quantity) AS unitsSold,
        uniq(sku) AS skuCount,
        sum(item_tax) AS tax_price,
        sum(product_cost * quantity) AS total_cogs,
        sum(shipping_price) AS shipping_cost,
        sum(
            item_price +
            shipping_price +
            promotion_discount +
            vendor_funding -
            (vendor_discount + ship_promotion_discount)
        ) AS netProfit
    FROM fact_order_items
    WHERE {where_clause}
    """

    # -------------------------
    # BREAKDOWN QUERY (FIXED)
    # -------------------------
    breakdown_query  = f"""
        SELECT
        marketplace_id,
        marketplace_name,
        sum(order_total) AS grossRevenue,
        sum(cogs + referral_fee) AS expenses,
        sum(quantity) AS unitsSold,
        sum(product_cost * quantity) AS total_cogs,

        sum(
            item_price +
            shipping_price +
            promotion_discount +
            vendor_funding -
            (vendor_discount + ship_promotion_discount)
        ) AS netProfit,

        round(
            if(sum(order_total) = 0, 0,
                (
                    sum(
                        item_price +
                        shipping_price +
                        promotion_discount +
                        vendor_funding -
                        (vendor_discount + ship_promotion_discount)
                    ) / sum(order_total)
                ) * 100
            ),
        2) AS margin

    FROM fact_order_items
    WHERE {where_clause}
    GROUP BY marketplace_id, marketplace_name
    ORDER BY grossRevenue DESC
    """
    # breakdown_query = f"""
    # SELECT
    #     marketplace_id,
    #     marketplace_name,
    #     sum(order_total) AS grossRevenue,
    #     sum(cogs + referral_fee) AS expenses,
    #     sum(quantity) AS unitsSold,
    #     sum(product_cost * quantity) AS total_cogs,
    #     sum(
    #         item_price +
    #         shipping_price +
    #         promotion_discount +
    #         vendor_funding -
    #         (vendor_discount + ship_promotion_discount)
    #     ) AS netProfit
    # FROM fact_order_items
    # WHERE {where_clause}
    # GROUP BY marketplace_id, marketplace_name
    # ORDER BY grossRevenue DESC
    # """

    logger.info("===== CURRENT QUERY =====")
    logger.info(current_query)

    logger.info("===== BREAKDOWN QUERY =====")
    logger.info(breakdown_query)

    # -------------------------
    # EXECUTION
    # -------------------------
    t1 = time.time()
    current_result = client.query(current_query)
    logger.info(f"Current query time: {time.time() - t1:.2f}s")

    t2 = time.time()
    breakdown_result = client.query(breakdown_query)
    logger.info(f"Breakdown query time: {time.time() - t2:.2f}s")

    # -------------------------
    # PARSE CURRENT
    # -------------------------
    def safe_first(result):
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return {}

    all_marketplace = safe_first(current_result)

    logger.info("===== CURRENT RESULT =====")
    logger.info(all_marketplace)

    # -------------------------
    # MARKETPLACE LIST
    # -------------------------
    marketplace_list = []
    for row in breakdown_result.result_rows:
        data = dict(zip(breakdown_result.column_names, row))

        marketplace_list.append(
            {
                "marketplace_id": data.get("marketplace_id"),
                "marketplace_name": data.get("marketplace_name", ""),
                "currency_list": [
                    {
                        "grossRevenue": data.get("grossRevenue", 0),
                        "expenses": data.get("expenses", 0),
                        "unitsSold": data.get("unitsSold", 0),
                        "total_cogs": data.get("total_cogs", 0),
                        "netProfit": data.get("netProfit", 0),
                        "margin": data.get("margin", 0),
                        "roi": data.get("roi", 0),
                        "refunds": data.get("refunds", 0),
                    }
                ],
            }
        )

    logger.info(f"Marketplace rows count: {len(marketplace_list)}")

    if not marketplace_list:
        logger.warning("⚠️ NO MARKETPLACE DATA RETURNED FROM CLICKHOUSE")

    # -------------------------
    # RESPONSE (UNCHANGED SHAPE)
    # -------------------------
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

    logger.info(f"TOTAL TIME: {time.time() - overall_start:.2f}s")

    return response_data
