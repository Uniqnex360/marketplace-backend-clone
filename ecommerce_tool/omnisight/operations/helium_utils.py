from __future__ import annotations
from omnisight.operations.core_calculator import EcommerceCalculator
from omnisight.models import Product, Order,pageview_session_count,OrderItems,Marketplace
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from ecommerce_tool.crud import DatabaseModel
import threading
import math
import pytz
from ecommerce_tool.util.shipping_price import get_full_order_and_shipping_details,get_orders_by_customer_and_date
import logging
logger = logging.getLogger(__name__)
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
def convertdateTotimezone(start_date,end_date,timezone_str):
    local_tz = pytz.timezone(timezone_str)
    naive_from_date = datetime.strptime(start_date, '%Y-%m-%d')
    naive_to_date = datetime.strptime(end_date, '%Y-%m-%d')
    naive_to_date = naive_to_date.replace(hour=23, minute=59, second=59)
    start_date = local_tz.localize(naive_from_date)
    end_date = local_tz.localize(naive_to_date)
    return start_date, end_date
def convertLocalTimeToUTC(start_date, end_date, timezone_str):
    local_tz = pytz.timezone(timezone_str)
    if start_date.tzinfo is None:
        start_date = local_tz.localize(start_date)
    if end_date.tzinfo is None:
        end_date = local_tz.localize(end_date)
    start_date = start_date.astimezone(pytz.UTC)
    end_date = end_date.astimezone(pytz.UTC)
    return start_date, end_date
def getOrdersListBasedonProductId(productIds,start_date=None, end_date=None):
    """
    Fetches the list of orders based on the provided product ID using a pipeline aggregation.
    Args:
        productId (str): The ID of the product for which to fetch orders.
    Returns:
        list: A list of dictionaries containing order details.
    """
    pipeline = []
    if start_date and end_date:
        pipeline.append({
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
            }
        })
    pipeline.extend([
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "order_items"
            }
        },
        {"$unwind": "$order_items"},
        {
            "$match": {
                "order_items.ProductDetails.product_id": {"$in": productIds}
            }
        },
        {
            "$group" : {
                "_id" : None,
                "orderIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "orderIds": 1
            }
        }
    ])
    orders = list(Order.objects.aggregate(*pipeline))
    if orders != []:
        orders = orders[0]['orderIds']
    return orders
def getproductIdListBasedonbrand(brandIds,start_date=None, end_date=None):
    """
    Fetches the list of product IDs based on the provided brand ID using a pipeline aggregation.
    Args:
        productId (str): The ID of the brand for which to fetch product IDs.
    Returns:
        list: A list of dictionaries containing product details.
    """
    orders = []
    pipeline = [
        {
            "$match": {
                "brand_id": {"$in": [ObjectId(bid) for bid in brandIds]}
            }
        },
        {
            "$group" : {
                "_id" : None,
                "productIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "productIds": 1
            }
        }
    ]
    products = list(Product.objects.aggregate(*pipeline))
    if products != []:
        orders = getOrdersListBasedonProductId(products[0]['productIds'],start_date, end_date)
    return orders
def getproductIdListBasedonManufacture(manufactureName = [],start_date=None, end_date=None):
    """
    Fetches the list of product IDs based on the provided brand ID using a pipeline aggregation.
    Args:
        productId (str): The ID of the brand for which to fetch product IDs.
    Returns:
        list: A list of dictionaries containing product details.
    """
    orders = []
    pipeline = [
        {
            "$match": {
                "manufacturer_name": {"$in":manufactureName }
            }
        },
        {
            "$group" : {
                "_id" : None,
                "productIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "productIds": 1
            }
        }
    ]
    products = list(Product.objects.aggregate(*pipeline))
    if products != []:
        orders = getOrdersListBasedonProductId(products[0]['productIds'],start_date, end_date)
    return orders
def     get_date_range(preset, time_zone_str="UTC"):
    tz = timezone(time_zone_str)
    now = tz.localize(datetime(year=2025, month=9, day=1, hour=0, minute=0, second=0))
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if preset == "Today":
        start = today
        return start, start.replace(hour=23, minute=59, second=59)
    elif preset == "Yesterday":
        start = today - timedelta(days=1)
        return start, start.replace(hour=23, minute=59, second=59)
    elif preset == "This Week":
        start = today - timedelta(days=today.weekday())
        return start, today.replace(hour=23, minute=59, second=59)
    elif preset == "This Month":
        start = today.replace(day=1)
        return start, today.replace(hour=23, minute=59, second=59)
    elif preset == "This Year":
        return today.replace(month=1, day=1), today.replace(hour=23, minute=59, second=59)
    elif preset == "Last Week":
        start = today - timedelta(days=today.weekday() + 7)
        end = start + timedelta(days=6)
        return start.replace(hour=0, minute=0, second=0, microsecond=0), end.replace(hour=23, minute=59, second=59)
    elif preset == "Last 7 days":
        return today - timedelta(days=7), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 14 days":
        return today - timedelta(days=14), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 30 days":
        return today - timedelta(days=30), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 60 days":
        return today - timedelta(days=60), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 90 days":
        return today - timedelta(days=90), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last Month":
        start = (today.replace(day=1) - relativedelta(months=1))
        return start, (today.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "This Quarter":
        quarter = (today.month - 1) // 3
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, (start + relativedelta(months=3) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last Quarter":
        quarter = ((today.month - 1) // 3) - 1
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, (start + relativedelta(months=3) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last Year":
        return today.replace(year=today.year - 1, month=1, day=1), today.replace(year=today.year - 1, month=12, day=31, hour=23, minute=59, second=59)
    return today, (today + timedelta(days=1)).replace(hour=23, minute=59, second=59)

def grossRevenue(start_date, end_date, filtered_marketplace_id=[], brand_id=None,
                 product_id=None, manufacuture_name=[], fulfillment_channel=None,
                 timezone='UTC',country=None):

    if timezone != 'UTC':
        start_date, end_date = convertLocalTimeToUTC(start_date, end_date, timezone)

    # Normalize inputs
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)

    marketplace_pipeline = [{"$project": {"_id": 1, "name": 1, "image_url": 1}}]
    marketplace_list = list(Marketplace.objects.aggregate(*marketplace_pipeline))
    marketplace_names = {str(m['_id']): m['name'] for m in marketplace_list}

    match = {
        'order_date': {"$gte": start_date, "$lte": end_date},
        'order_status': {"$nin": ["Canceled", "Cancelled"]},
        'order_total': {"$gt": 0}
    }
    if country:
        match['geo']=country.upper()

    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel

    if filtered_marketplace_id and isinstance(filtered_marketplace_id,list) and len(filtered_marketplace_id)>0:
        match['marketplace_id'] = {"$in":filtered_marketplace_id}

    # Handle filtering via product/brand/manufacturer
    if manufacuture_name:
        ids = getproductIdListBasedonManufacture(manufacuture_name, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif product_id:
        pids = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(pids, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif brand_id:
        bids = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(bids, start_date, end_date)
        match["_id"] = {"$in": ids}

    # Full aggregation pipeline
    pipeline = [
        {"$match": match},
        {"$lookup": {
            "from": "orderitems",
            "localField": "order_items",
            "foreignField": "_id",
            "as": "items"
        }},
        {"$addFields": {
            "item_details": {
                "$map": {
                    "input": "$items",
                    "as": "item",
                    "in": {
                        "price": {
                            "$cond": {
                                "if": {"$and": [
                                    {"$ifNull": ["$$item.Pricing.ItemPrice.Amount", None]},
                                    {"$ne": ["$$item.Pricing.ItemPrice.Amount", ""]}
                                ]},
                                "then": {"$toDouble": "$$item.Pricing.ItemPrice.Amount"},
                                "else": 0.0
                            }
                        },
                        "tax": {
                            "$cond": {
                                "if": {"$and": [
                                    {"$ifNull": ["$$item.Pricing.ItemTax.Amount", None]},
                                    {"$ne": ["$$item.Pricing.ItemTax.Amount", ""]}
                                ]},
                                "then": {"$toDouble": "$$item.Pricing.ItemTax.Amount"},
                                "else": 0.0
                            }
                        }
                    }
                }
            }
        }},
        {"$addFields": {
            "calculated_item_price": {"$sum": "$item_details.price"},
            "calculated_tax_sum": {"$sum": "$item_details.tax"},
            "original_order_total": "$order_total",
            "order_total": {
                "$subtract": [
                    "$order_total",
                    {"$sum": "$item_details.tax"}
                ]
            }
        }},
        {"$project": {
            "_id": 1,
            "order_date": 1,
            "purchase_order_id": 1,
            "order_items": 1,
            "order_total": 1,
            "original_order_total": 1,
            "order_status": 1,
            "fulfillment_channel": 1,
            "merchant_order_id": 1,
            "seller_order_id": 1,
            "customer_email_id": 1,
            "marketplace_id": 1,
            "currency": 1,
            "shipping_address": 1,
            "shipping_information": 1,
            "shipping_price": {"$ifNull": ["$shipping_price", 0.0]},
            "merchant_shipment_cost": {"$ifNull": ["$merchant_shipment_cost", 0.0]},
            "items_order_quantity": {"$size": {"$ifNull": ["$order_items", []]}},
            "marketplace_name": {
                "$cond": {
                    "if": {"$in": [{"$toString": "$marketplace_id"}, list(marketplace_names.keys())]},
                    "then": {
                        "$arrayElemAt": [
                            list(marketplace_names.values()),
                            {"$indexOfArray": [list(marketplace_names.keys()), {"$toString": "$marketplace_id"}]}
                        ]
                    },
                    "else": None
                }
            }
        }}
    ]

    try:
        results = list(Order.objects.aggregate(*pipeline))
        # Round values
        for r in results:
            r['original_order_total'] = round(r.get('original_order_total', 0.0), 2)
            r['order_total'] = round(r.get('order_total', 0.0), 2)
        return results
    except Exception as e:
        print(f"Aggregation error: {e}")
        return []
def get_previous_periods(current_start, current_end):
    period_duration = current_end - current_start
    if period_duration.days > 1:
        period_duration += timedelta(days=1)
    previous_period = {
        'start': (current_start - period_duration).strftime('%b %d, %Y'),
        'end': (current_start - timedelta(days=1)).strftime('%b %d, %Y')
    }
    previous_week = {
        'start': (current_start - timedelta(weeks=1)).strftime('%b %d, %Y'),
        'end': (current_end - timedelta(weeks=1)).strftime('%b %d, %Y')
    }
    previous_month = {
        'start': (current_start - relativedelta(months=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(months=1)).strftime('%b %d, %Y')
    }
    previous_year = {
        'start': (current_start - relativedelta(years=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(years=1)).strftime('%b %d, %Y')
    }
    response_data = {
        'previous_period': previous_period,
        'previous_week': previous_week,
        'previous_month': previous_month,
        'previous_year': previous_year,
        'current_period': {
            'start': current_start.strftime('%b %d, %Y'),
            'end': current_end.strftime('%b %d, %Y')
        }
    }
    return response_data
def refundOrder(start_date, end_date, filtered_marketplace_id=[],brand_id=None,product_id=None,manufacuture_name=[],fulfillment_channel=None,timezone='UTC'):    
    if timezone != 'UTC':
        start_date,end_date = convertLocalTimeToUTC(start_date, end_date, timezone)
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)
    match=dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = "Refunded"
    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    if filtered_marketplace_id and isinstance(filtered_marketplace_id,list) and len(filtered_marketplace_id)>0:
        match['marketplace_id'] = {"$in":filtered_marketplace_id}
    if manufacuture_name != None and manufacuture_name != "" and manufacuture_name != []:
        ids = getproductIdListBasedonManufacture(manufacuture_name,start_date, end_date)
        match["_id"] = {"$in": ids}
    elif product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id,start_date, end_date)
        match["_id"] = {"$in": ids}
    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id,start_date, end_date)
        match["_id"] = {"$in": ids}
    pipeline = [
            {
            "$match": match
            },
            {
            "$project": {
                "_id": 0,
                "order_items": 1,
                "order_total" :1,
                "order_date" : 1
            }
            },
            {
                "$sort" : {
                    "_id" : -1
                }
            }
        ]
    result = list(Order.objects.aggregate(*pipeline))
    return result
def AnnualizedRevenueAPIView(target_date):
    start_date = target_date - timedelta(days=365)
    monthly_revenues = []
    total_gross_revenue = 0
    for i in range(12):
        month_start = start_date + timedelta(days=30*i)
        month_end = month_start + timedelta(days=30)
        monthly_result = grossRevenue(month_start, month_end)
        monthly_gross = sum(ins['order_total'] for ins in monthly_result) if monthly_result else 0
        monthly_revenues.append(monthly_gross)
        total_gross_revenue += monthly_gross
    average_monthly = total_gross_revenue / 12 if 12 > 0 else 0
    annualized_revenue = average_monthly * 12
    annualized_revenue = round(annualized_revenue, 2)
    return annualized_revenue
def getdaywiseproductssold(start_date, end_date, product_id, is_hourly=False):
    date_format = "%Y-%m-%d %H:00" if is_hourly else "%Y-%m-%d"
    pipeline = [
        {
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
            }
        },
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "order_items_ins"
            }
        },
        {"$unwind": "$order_items_ins"},
        {
            "$match": {
                "order_items_ins.ProductDetails.product_id": ObjectId(product_id)
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$order_date"
                    }
                },
                "total_quantity": {"$sum": "$order_items_ins.ProductDetails.QuantityOrdered"},
                "total_price": {"$sum": "$order_items_ins.Pricing.ItemPrice.Amount"},
                "total_cost": {"$sum": {"$multiply": ["$order_items_ins.ProductDetails.product_cost", "$order_items_ins.ProductDetails.QuantityOrdered"]}},
                "total_merchant_shipment_cost": {"$sum": {"$divide": ["$merchant_shipment_cost", {"$size": "$order_items"}]}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "total_quantity": 1,
                "total_price": {"$round": ["$total_price", 2]},
                "total_cost": {"$round": ["$total_cost", 2]},
                "total_merchant_shipment_cost": {"$round": ["$total_merchant_shipment_cost", 2]}
            }
        },
        {"$sort": {"date": 1}}
    ]
    orders = list(Order.objects.aggregate(*pipeline))
    return orders
def pageViewsandSessionCount(start_date,end_date,product_id):
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "product_id": ObjectId(product_id)
            }
        },
        {
            "$project": {
                "_id": 0,
                "date" : 1,
                "page_views": 1,
                "session_count": "$sessions"
            }
        }
    ]
    views_list = list(pageview_session_count.objects.aggregate(*pipeline))
    return views_list
def create_empty_bucket_data(time_key):
    """Return a valid empty data structure for a time bucket"""
    return {
        "gross_revenue": 0.0,
        "net_profit": 0.0,
        "profit_margin": 0.0,
        "orders": 0,
        "units_sold": 0,
        "refund_amount": 0.0,
        "refund_quantity": 0,
        "current_date": time_key
    }
from concurrent.futures import ThreadPoolExecutor
def get_graph_data(start_date, end_date, preset, filtered_marketplace_id, brand_id=None, product_id=None, 
                  manufacturer_name=None, fulfillment_channel=None, timezone="UTC",country=None):
    user_timezone = pytz.timezone(timezone) if timezone != 'UTC' else pytz.UTC
    original_start_date = start_date
    original_end_date = end_date
    if timezone != 'UTC':
        start_date_utc, end_date_utc = convertLocalTimeToUTC(start_date, end_date, timezone)
    else:
        start_date_utc = start_date
        end_date_utc = end_date
    start_date_utc = start_date_utc.replace(tzinfo=None)
    end_date_utc = end_date_utc.replace(tzinfo=None)
    bucket_to_local_date_map = {}
    is_single_day = (preset in ['Today', "Yesterday"]) or (start_date.date() == end_date.date())

    # Time buckets setup
    if is_single_day:
        time_buckets = [(start_date_utc + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) 
                      for i in range(24)]
        time_format = "%Y-%m-%d %H:00:00"
    else:
        time_buckets = []
        time_format = "%Y-%m-%d 00:00:00"
        current_date = original_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_midnight = original_end_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        while current_date < end_date_midnight:
            utc_bucket = current_date.astimezone(pytz.UTC).replace(tzinfo=None)
            time_buckets.append(utc_bucket)
            utc_key = utc_bucket.strftime(time_format)
            local_date_key = current_date.strftime(time_format)
            bucket_to_local_date_map[utc_key] = local_date_key
            current_date += timedelta(days=1)

    graph_data = {}
    for dt in time_buckets:
        time_key = dt.strftime(time_format)
        graph_data[time_key] = {
            "gross_revenue": 0, "net_profit": 0, "profit_margin": 0, "orders": 0,
            "units_sold": 0, "refund_amount": 0, "refund_quantity": 0
        }

    if not is_single_day and timezone != 'UTC':
        query_end_date = end_date_utc + timedelta(days=1)
    else:
        query_end_date = end_date_utc

    # Build aggregation pipeline
    pipeline = [
        {
            "$match": {
                "order_status": {"$in": ["Shipped", "Delivered", "Acknowledged", "Pending", "Unshipped", "PartiallyShipped"]},
                "order_date": {"$gte": start_date_utc, "$lt": query_end_date}
            }
        }
    ]
    if country:
        pipeline[0]["$match"]["geo"] = country.upper()
        

    # Add marketplace filter
    if filtered_marketplace_id and isinstance(filtered_marketplace_id, list) and len(filtered_marketplace_id) > 0:
        pipeline[0]["$match"]["marketplace_id"] = {"$in": filtered_marketplace_id}

    # Add fulfillment channel
    if fulfillment_channel:
        pipeline[0]["$match"]["fulfillment_channel"] = fulfillment_channel

    # Add brand/product/manufacturer filters via $in
    if manufacturer_name not in [None, "", []]:
        ids = getproductIdListBasedonManufacture(manufacturer_name, start_date_utc, end_date_utc)
        if ids:
            pipeline[0]["$match"]["_id"] = {"$in": ids}
    elif product_id not in [None, "", []]:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id, start_date_utc, end_date_utc)
        if ids:
            pipeline[0]["$match"]["_id"] = {"$in": ids}
    elif brand_id not in [None, "", []]:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id, start_date_utc, end_date_utc)
        if ids:
            pipeline[0]["$match"]["_id"] = {"$in": ids}

    # Execute aggregation
    orders_cursor = Order.objects.aggregate(*pipeline)
    orders_dict_list = list(orders_cursor)
    orders_list = []
    for doc in orders_dict_list:
        try:
            order = Order.objects.get(id=doc['_id'])
            orders_list.append(order)
        except Exception as e:
            continue  

    # Group orders by time bucket
    orders_by_bucket = {}
    for dt in time_buckets:
        bucket_start = dt
        bucket_end = dt + timedelta(hours=1) if is_single_day else dt + timedelta(days=1)
        orders_by_bucket[dt.strftime(time_format)] = [
            order for order in orders_list 
            if bucket_start <= order.order_date < bucket_end
        ]

    # Process each time bucket
    def process_time_bucket(time_key):
        nonlocal graph_data, orders_by_bucket
        bucket_orders = orders_by_bucket.get(time_key, [])
        total_cogs = 0
        refund_amount = 0
        refund_quantity = 0
        total_units = 0
        temp_other_price = 0
        vendor_funding = 0
        vendor_discount = 0
        shipping_price = 0
        promotion_discount = 0
        ship_promotion_discount = 0
        channel_fee = 0

        bucket_start = datetime.strptime(time_key, time_format).replace(tzinfo=pytz.UTC)
        if is_single_day:
            bucket_end = bucket_start + timedelta(hours=1)
        else:
            bucket_end = bucket_start + timedelta(days=1)

        # Get refunds in this bucket
        refund_ins = refundOrder(bucket_start, bucket_end, filtered_marketplace_id, brand_id, product_id)
        if refund_ins:
            for ins in refund_ins:
                if bucket_start <= ins['order_date'] < bucket_end:
                    refund_amount += ins['order_total']
                    refund_quantity += len(ins['order_items'])

        all_item_ids = []
        unique_order_ids = set()
        for order in bucket_orders:
            shipping_price += getattr(order, 'shipping_price', 0) or 0
            po_id = getattr(order, 'purchase_order_id', None)
            if po_id:
                unique_order_ids.add(po_id)
            for item in order.order_items:
                all_item_ids.append(item.id)

        # Fetch item details via aggregation
        if all_item_ids:
            item_pipeline = [
                {"$match": {"_id": {"$in": all_item_ids}}},
                {"$lookup": {
                    "from": "product",
                    "localField": "ProductDetails.product_id",
                    "foreignField": "_id",
                    "as": "product_ins"
                }},
                {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                {"$project": {
                    "_id": 1,
                    "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                    "product_cost": {"$round": [{"$ifNull": ["$product_ins.product_cost", 0]}, 2]},
                    "promotion_discount": {"$ifNull": ["$Pricing.PromotionDiscount.Amount", 0]},
                    "ship_promotion_discount": {"$ifNull": ["$Pricing.ShipPromotionDiscount.Amount", 0]},
                    "vendor_discount": {"$ifNull": ["$product_ins.vendor_discount", 0]},
                    "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    "referral_fee": {"$round": [{"$ifNull": ["$product_ins.referral_fee", 0]}, 2]},
                    "walmart_fee": {"$ifNull": ["$product_ins.walmart_fee", 0]},
                    "QuantityOrdered": {"$ifNull": ["$ProductDetails.QuantityOrdered", 1]}
                }}
            ]
            item_results = list(OrderItems.objects.aggregate(*item_pipeline))
            item_map = {str(item["_id"]): item for item in item_results}

            for order in bucket_orders:
                for item in order.order_items:
                    item_data = item_map.get(str(item.id))
                    if not item_data:
                        continue
                    price = float(item_data.get('price', 0) or 0)
                    quantity = int(item_data.get('QuantityOrdered', 1) or 1)
                    product_cost = float(item_data.get('product_cost', 0) or 0)
                    temp_other_price += price
                    total_units += quantity
                    total_cogs += product_cost * quantity
                    vendor_funding += float(item_data.get('vendor_funding', 0) or 0) * quantity
                    promotion_discount += float(item_data.get('promotion_discount', 0) or 0)
                    ship_promotion_discount += float(item_data.get('ship_promotion_discount', 0) or 0)
                    vendor_discount += float(item_data.get('vendor_discount', 0) or 0)
                    channel_fee += float(item_data.get('referral_fee', 0)) * quantity

                fulfillment_channel = getattr(order, 'fulfillment_channel', "")
                merchant_shipment_cost = getattr(order, 'merchant_shipment_cost', 0) or 0
                if merchant_shipment_cost is None:
                    if fulfillment_channel == 'AFN':
                        merchant_shipment_cost = getattr(order, 'shipping_price', 0) or 0
                total_cogs += merchant_shipment_cost

        gross_revenue_with_tax = temp_other_price
        net_profit = (temp_other_price + shipping_price + promotion_discount + vendor_funding - 
                     (channel_fee + total_cogs + vendor_discount + ship_promotion_discount))
        profit_margin = round((net_profit / gross_revenue_with_tax) * 100, 2) if gross_revenue_with_tax else 0

        graph_data[time_key] = {
            "gross_revenue_with_tax": round(gross_revenue_with_tax, 2),
            "net_profit": round(net_profit, 2),
            "profit_margin": profit_margin,
            "orders": len(unique_order_ids),
            "units_sold": total_units,
            "refund_amount": round(refund_amount, 2),
            "refund_quantity": refund_quantity
        }

    # Process all buckets in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_time_bucket, time_key): time_key for time_key in graph_data}
        for future in futures:
            future.result()

    # Convert UTC timestamps to local timezone
    converted_graph_data = {}
    start_date_only = original_start_date.date()
    end_date_only = original_end_date.date()

    if is_single_day:
        for utc_time_key, data in graph_data.items():
            utc_dt = datetime.strptime(utc_time_key, time_format).replace(tzinfo=pytz.UTC)
            local_dt = utc_dt.astimezone(user_timezone)
            local_time_key = local_dt.strftime(time_format)
            converted_graph_data[local_time_key] = data
            converted_graph_data[local_time_key]["current_date"] = local_time_key
    else:
        for utc_time_key, data in graph_data.items():
            local_time_key = bucket_to_local_date_map.get(utc_time_key)
            if local_time_key:
                local_date = datetime.strptime(local_time_key, time_format).date()
                if start_date_only <= local_date <= end_date_only:
                    converted_graph_data[local_time_key] = data
                    converted_graph_data[local_time_key]["current_date"] = local_time_key

    return converted_graph_data


def totalRevenueCalculation(start_date, end_date, filtered_marketplace_id=[], brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None, timezone_str="UTC",country=None):
    total = dict()
    gross_revenue_with_tax = 0
    refund = 0
    net_profit = 0
    total_units = 0
    total_orders = 0
    temp_other_price = 0
    vendor_funding = 0
    vendor_discount = 0
    total_price = 0
    total_cogs = 0
    refund_quantity_ins = 0
    promotion_discount=0   
    ship_promotion_discount=0 
    shipping_price = 0
    channel_fee = 0
    orders = grossRevenue(start_date, end_date, filtered_marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel, timezone_str,country)
    refund_ins = refundOrder(start_date, end_date, filtered_marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel, timezone_str)
    if refund_ins:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])
    all_item_ids = []
    item_marketplace_map = {}
    unique_order_ids = set()
    for order in orders:
        gross_revenue_with_tax += order.get('original_order_total')
        shipping_price += order.get('shipping_price', 0) or 0
        po_id=order.get('purchase_order_id')
        if po_id:
            unique_order_ids.add(po_id)
        for item_id in order['order_items']:
            all_item_ids.append(item_id)
            item_marketplace_map[str(item_id)] = order['marketplace_name']
    total_orders=len(unique_order_ids)
    item_details_map=EcommerceCalculator.get_item_details_bulk(all_item_ids )
    metrics=EcommerceCalculator.calculate_order_metrics(orders=orders,item_details_map=item_details_map,include_breakdown=False)
    # pipeline = [
    #     {"$match": {"_id": {"$in": all_item_ids}}},
    #     {
    #         "$lookup": {
    #             "from": "product",
    #             "localField": "ProductDetails.product_id",
    #             "foreignField": "_id",
    #             "as": "product_ins"
    #         }
    #     },
    #     {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
    #     {
    #         "$project": {
    #             "_id": 1,
    #             "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
    #             "product_cost": {"$round":[{"$ifNull": ["$product_ins.product_cost", 0]},2]},
    #             "promotion_discount": {"$ifNull": ["$Pricing.PromotionDiscount.Amount", 0]},
    #             "ship_promotion_discount": {"$ifNull": ["$Pricing.ShipPromotionDiscount.Amount", 0]},
    #             "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
    #             "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
    #             "referral_fee": {"$round":[{"$ifNull": ["$product_ins.referral_fee", 0]},2]},
    #             "walmart_fee": {"$ifNull": ["$product_ins.walmart_fee", 0]},
    #             "vendor_discount": {"$ifNull": ["$product_ins.vendor_discount", 0]},
    #             "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
    #             "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
    #             "QuantityOrdered": {"$ifNull": ["$ProductDetails.QuantityOrdered", 1]},
    #         }
    #     }
    # ]
    # item_results = list(OrderItems.objects.aggregate(*pipeline))
    # item_map = {str(item["_id"]): item for item in item_results}
    # for order in orders:
    #     for item_id in order['order_items']:
    #         item = item_map.get(str(item_id))
    #         if not item:
    #             continue
    #         product_cost = float(item.get('product_cost', 0) or 0)
    #         quantity = int(item.get('QuantityOrdered', 1) or 1)
    #         total_cogs += product_cost * quantity
    #         vendor_discount += float(item.get("vendor_discount", 0) or 0)
    #         vendor_funding += float(item.get("vendor_funding", 0) or 0)*quantity
    #         promotion_discount+=float(item.get('promotion_discount',0) or 0)
    #         ship_promotion_discount+=float(item.get('ship_promotion_discount',0) or 0)
    #         price=item.get('price',0) or 0
    #         if price==0 and 'charges' in item:
    #             price=sum(float(charge.get('chargeAmount',0)) for charge in item['charges'])
    #         temp_other_price += price
    #         total_units+=quantity
    #         channel_fee += float(item.get("referral_fee", 0) or 0)*quantity
    #     fulfillment_channel = order.get('fulfillment_channel', "")
    #     merchant_shipment_cost = order.get('merchant_shipment_cost',0)
    #     if merchant_shipment_cost is None:
    #         if fulfillment_channel == 'AFN':
    #             merchant_shipment_cost = order.get('shipping_price', 0) or 0
    #         elif fulfillment_channel == "MFN":
    #             merchant_shipment_cost = order.get('merchant_shipment_cost',0)
    #         elif fulfillment_channel == 'SellerFulfilled':
    #             merchant_shipment_cost = order.get('merchant_shipment_cost',0)
    #     total_cogs += merchant_shipment_cost
    # net_profit = (temp_other_price + shipping_price + promotion_discount+vendor_funding - (channel_fee + total_cogs + vendor_discount+ship_promotion_discount))
    total = {
        "gross_revenue_with_tax": metrics['gross_revenue_with_tax'],
        "net_profit": metrics['net_profit'],
        "profit_margin": metrics['margin'],
        "orders": metrics['total_orders'],
        "units_sold": metrics['total_units'],
        "refund_amount": round(refund, 2),
        "refund_quantity": refund_quantity_ins
    }
    return total

def chunked_aggregate(pipeline_base, id_list, chunk_size=5000):
    results = {}
    for i in range(0, len(id_list), chunk_size):
        chunk = id_list[i:i + chunk_size]
        pipeline = pipeline_base.copy()
        pipeline[0] = {"$match": {"_id": {"$in": chunk}}}
        for item in OrderItems.objects.aggregate(*pipeline):
            results[str(item['_id'])] = item
    return results
    
def calculate_metricss(
    from_date,
    to_date,
    filtered_marketplace_id,
    brand_id,
    product_id,
    manufacturer_name,
    fulfillment_channel,
    country=None,
    timezone='UTC',
    include_extra_fields=False,
    use_threads=True
):
    def safe_float(value, default=0.0):
        if value is None or math.isnan(value):
            return default
        return float(value)  
    gross_revenue = 0
    total_cogs = 0
    channel_fee = 0  
    net_profit = 0
    total_units = 0
    vendor_funding = 0
    vendor_discount=0
    temp_price = 0
    refund = 0  
    tax_price = 0
    sessions = 0
    page_views = 0
    shipping_cost = 0
    promotion_discount=0
    ship_promotion_discount=0
    unitSessionPercentage = 0
    sku_set = set()
    p_id = set()
    unique_order_id=set()
    product_categories = {}  
    product_completeness = {"complete": 0, "incomplete": 0}  
    total_product_cost = 0  
    result = grossRevenue(from_date, to_date, filtered_marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel, timezone,country)
    all_item_ids = [ObjectId(item_id) for order in result for item_id in order['order_items']]
    for order in result:
        po_id=order.get('purchase_order_id')
        if po_id:
            unique_order_id.add(po_id)
    if timezone != 'UTC':
        from_date, to_date = convertLocalTimeToUTC(from_date, to_date, timezone)
    from_date = from_date.replace(tzinfo=None)
    to_date = to_date.replace(tzinfo=None)
    item_pipeline = [
        { "$match": { "_id": { "$in": all_item_ids } } },
        {
            "$lookup": {
                "from": "product",
                "localField": "ProductDetails.product_id",
                "foreignField": "_id",
                "as": "product_ins"
            }
        },
        { "$unwind": { "path": "$product_ins", "preserveNullAndEmptyArrays": True } },
        {
            "$project": {
                "p_id" : "$product_ins._id",
                "price": "$Pricing.ItemPrice.Amount",
                "tax_price": "$Pricing.ItemTax.Amount",
                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                "sku": "$product_ins.sku",
                "category": "$product_ins.category",  
                "promotion_discount": {"$ifNull": ["$Pricing.PromotionDiscount.Amount", 0]},
                "ship_promotion_discount": {"$ifNull": ["$Pricing.ShipPromotionDiscount.Amount", 0]},
                "total_cogs": { "$ifNull": ["$product_ins.total_cogs", 0] },
                "w_total_cogs": { "$ifNull": ["$product_ins.w_total_cogs", 0] },
                "vendor_funding": { "$ifNull": ["$product_ins.vendor_funding", 0] },
                "vendor_discount": { "$ifNull": ["$product_ins.vendor_discount", 0] },
                "a_shipping_cost" : {"$ifNull":["$product_ins.a_shipping_cost",0]},
                "w_shiping_cost" : {"$ifNull":["$product_ins.w_shiping_cost",0]},
                "referral_fee": {"$round":[{"$ifNull": ["$product_ins.referral_fee", 0]},2]},
                "product_cost": {"$round":[{"$ifNull": ["$product_ins.product_cost", 0]},2]},
                "QuantityOrdered": {"$ifNull": ["$ProductDetails.QuantityOrdered", 1]},
            }
        }
    ]
    item_details_map = chunked_aggregate(item_pipeline, all_item_ids)
    def process_order(order):
        nonlocal gross_revenue, temp_price, tax_price, total_cogs, vendor_funding,vendor_discount, total_units, sku_set, page_views, sessions, shipping_cost, p_id,promotion_discount,ship_promotion_discount, channel_fee, total_product_cost, product_categories, product_completeness
        gross_revenue += safe_float(order.get('original_order_total'))
        shipping_cost += safe_float(order.get('shipping_price'))
        for item_id in order['order_items']:
            item_data = item_details_map.get(str(item_id))
            if item_data:
                price=safe_float(item_data.get('price',0))
                if price==0 and 'charges' in item_data:
                    price=sum(safe_float(charge.get('chargeAmount',0))for charge in item_data['charges'])
                temp_price += price 
                tax_price += safe_float(item_data.get('tax_price', 0))
                product_cost = safe_float(item_data.get('product_cost', 0))
                quantity = int(item_data.get('QuantityOrdered', 1) or 1)
                total_units+=quantity
                total_cogs+=product_cost*quantity
                channel_fee += safe_float(item_data.get('referral_fee', 0)) * quantity  
                promotion_discount += safe_float(item_data.get('promotion_discount',0))
                ship_promotion_discount += safe_float(item_data.get('ship_promotion_discount',0))
                vendor_funding += safe_float(item_data.get('vendor_funding', 0))*quantity
                vendor_discount += safe_float(item_data.get('vendor_discount',0))
                total_product_cost += product_cost * quantity  
                if item_data.get('sku'):
                    sku_set.add(item_data['sku'])
                category = item_data.get('category', 'Unknown')  
                product_categories[category] = product_categories.get(category, 0) + 1
                if item_data.get('price') and item_data.get('product_cost') and item_data.get('sku'):
                    product_completeness["complete"] += 1
                else:
                    product_completeness["incomplete"] += 1
                try:
                    p_id.add(item_data['p_id'])
                except:
                    pass
        fulfillment_channel = order.get('fulfillment_channel', "")
        merchant_shipment_cost = safe_float(order.get('merchant_shipment_cost'))  
        if merchant_shipment_cost is None:  
            if fulfillment_channel == "AFN":
                merchant_shipment_cost = safe_float(order.get('shipping_price'))  
            elif fulfillment_channel == "MFN":
                merchant_shipment_cost = safe_float(order.get('merchant_shipment_cost',0))
            elif fulfillment_channel=='SellerFulfilled':
                merchant_shipment_cost = safe_float(order.get('merchant_shipment_cost',0))
        total_cogs += merchant_shipment_cost
    if use_threads:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_order, order) for order in result]
            for future in as_completed(futures):
                future.result()
    else:
        for order in result:
            process_order(order)
    pipeline = [
        {
            "$match": {
                "product_id": {"$in": list(p_id)},
                "date": {"$gte": from_date, "$lte": to_date}
            }
        },
        {
            "$group": {
                "_id": None,
                "page_views": {"$sum": "$page_views"},
                "sessions": {"$sum": "$sessions"}
            }
        }
    ]
    p_result = list(pageview_session_count.objects.aggregate(*pipeline))
    for P_ins in p_result:
        page_views += P_ins.get('page_views', 0)
        sessions += P_ins.get('sessions', 0)
    gross_revenue = safe_float(gross_revenue)
    temp_price = safe_float(temp_price)
    shipping_cost = safe_float(shipping_cost)
    promotion_discount = safe_float(promotion_discount)
    ship_promotion_discount = safe_float(ship_promotion_discount)
    vendor_funding = safe_float(vendor_funding)
    channel_fee = safe_float(channel_fee)
    total_cogs = safe_float(total_cogs)
    vendor_discount = safe_float(vendor_discount)
    expenses = total_cogs + channel_fee
    net_profit = (
    temp_price + shipping_cost + promotion_discount + vendor_funding
    - (channel_fee + total_cogs + vendor_discount + ship_promotion_discount + refund)
)   
    margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 and not math.isnan(gross_revenue) else 0
    unitSessionPercentage = (total_units / sessions) * 100 if sessions else 0
    base_result = {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round(expenses, 2),
        "referral_fee": round(channel_fee, 2),  
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / expenses) * 100, 2) if expenses > 0 and not math.isnan(expenses) else 0,
        "unitsSold": total_units,
        "refunds": refund,
        "skuCount": len(sku_set),
        "sessions": sessions,
        "pageViews": page_views,
        "unitSessionPercentage": round(unitSessionPercentage, 2),
        "margin": round(margin, 2),
        "orders": len(unique_order_id),
        "productCategories": product_categories,  
        "productCompleteness": product_completeness  
    }
    if include_extra_fields:
        base_result.update({
            "seller": "",
            "tax_price": round(tax_price, 2),
            "total_cogs": round(total_cogs, 2),
            "product_cost": round(total_product_cost, 2),  
            "shipping_cost": round(shipping_cost, 2),
        })
    return base_result
def totalRevenueCalculationForProduct(start_date, end_date, marketplace_id=None, brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None,timezone_str="UTC"):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    net_profit = 0
    total_units = 0
    temp_other_price = 0
    vendor_funding = 0
    result = grossRevenue(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone_str)
    def process_order(order):
        nonlocal gross_revenue, total_cogs, total_units, temp_other_price, vendor_funding
        tax_price = 0
        gross_revenue += order['order_total']
        total_units += order['items_order_quantity']
        for j in order['order_items']:
            pipeline = [
                {
                    "$match": {
                        "_id": j
                    }
                },
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {
                    "$unwind": {
                        "path": "$product_ins",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
                        "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
                        "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
                        "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    }
                }
            ]
            result = list(OrderItems.objects.aggregate(*pipeline))
            if result:
                tax_price += result[0]['tax_price']
                temp_other_price += result[0]['price']
                if order['marketplace_name'] == "Amazon":
                    total_cogs += result[0]['total_cogs']
                else:
                    total_cogs += result[0]['w_total_cogs']
                vendor_funding += result[0]['vendor_funding']
    threads = []
    for order in result:
        thread = threading.Thread(target=process_order, args=(order,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    net_profit = (temp_other_price - total_cogs) + vendor_funding
    total = {
        "gross_revenue": round(gross_revenue, 2),
        "net_profit": round(net_profit, 2),
        "units_sold": round(total_units, 2)
    }
    return total
def get_top_movers(yesterday_data, previous_day_data):
    prev_data_map = {item['sku']: item for item in previous_day_data}
    changes = []
    for item in yesterday_data:
        sku = item['sku']
        yesterday_units = item['unitsSold']
        prev_units = prev_data_map.get(sku, {}).get('unitsSold', 0)
        change = yesterday_units - prev_units  
        changes.append({
            'sku': sku,
            "id": item.get("id") or item.get("_id", ""),
            "asin" : item['asin'],
            "fulfillmentChannel" : item['fulfillmentChannel'],
            'product_name': item['product_name'],
            'images': item['images'],
            "unitsSold" : yesterday_units,
            "grossRevenue" : item['grossRevenue'],
            "netProfit" : item['netProfit'],
            "totalCogs" : round(item['totalCogs'],2),
            "netProfit" : item['netProfit'],
            "m_name": item.get("m_name", ""),
            'yesterday_units': yesterday_units,
            'previous_units': prev_units,
            'change_in_units': change,
        })
    top_increasing = sorted(
        [c for c in changes if c['change_in_units'] > 0],
        key=lambda x: x['change_in_units'],
        reverse=True
    )[:3]
    top_decreasing = sorted(
        [c for c in changes if c['change_in_units'] < 0],
        key=lambda x: x['change_in_units']
    )[:3]
    return {
        'top_3_products': top_increasing,
        'least_3_products': top_decreasing
    }