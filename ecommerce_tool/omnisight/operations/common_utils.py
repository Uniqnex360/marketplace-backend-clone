import re
import threading
from omnisight.models import Marketplace, OrderItems, pageview_session_count
from omnisight.operations.helium_utils import convertLocalTimeToUTC, convertdateTotimezone, get_date_range, grossRevenue, refundOrder
import emoji



def calculate_listing_score(product):
    total_rules = 13
    score_per_rule = 10 / total_rules
    passed_rules = 0
    def check_title_strange_symbols(p):
        # Check if the product title contains any emoji
        title = p.get("product_title", "")
        contains_emoji = any(char in emoji.EMOJI_DATA for char in title)
        return not contains_emoji

    def check_title_length(p):
        return len(p.get("product_title", "")) >= 150

    def check_qty_bullets(p):
        return len(p.get("features", [])) >= 5

    def check_length_bullets(p):
        return all(len(bullet) >= 150 for bullet in p.get("features", []))

    def check_capitalized_bullets(p):
        return all(bullet[:1].isupper() for bullet in p.get("features", []) if bullet)

    def check_all_caps_bullets(p):
        return all(not bullet.isupper() for bullet in p.get("features", []))

    def check_ebc_description(p):
        return len(p.get("product_description", "")) >= 1000

    def check_image_resolution(p):
        # Placeholder - Assume pass if image_url is present
        return bool(p.get("image_url"))

    def check_image_background(p):
        # Placeholder - Assume pass if image_url is present
        return bool(p.get("image_url"))

    def check_images_qty(p):
        return len(p.get("image_urls", [])) >= 7

    def check_videos_qty(p):
        # Placeholder - No video data available
        return len(p.get("videos", [])) >= 7

    def check_review_qty(p):
        # Placeholder - No review data available
        return False

    def check_review_rating(p):
        # Placeholder - No review rating available
        return False
    final_checks = []
    checks = [
        check_title_strange_symbols,
        check_title_length,
        check_qty_bullets,
        check_length_bullets,
        check_capitalized_bullets,
        check_all_caps_bullets,
        check_ebc_description,
        check_image_resolution,
        check_image_background,
        check_images_qty,
        check_videos_qty,
        check_review_qty,
        check_review_rating
    ]

    for check in checks:
        c_rule = check(product)
        final_checks.append(c_rule)
        if c_rule:
            passed_rules += 1
    data = {
    "final_score" : round(passed_rules * score_per_rule, 2),
    "rules_checks" : final_checks
    }


    return data

# Example usage:
def assign_listing_score_to_product(product_doc):
    product_dict = product_doc.to_mongo().to_dict()
    score = calculate_listing_score(product_dict)
    product_doc.listing_quality_score = score['final_score']
    product_doc.save()
    return score


# def process_products_in_batches(start_index, end_index, product_list):
#     for i in range(start_index, end_index):
#         if i >= len(product_list):
#             break
#         product = product_list[i]
#         score = assign_listing_score_to_product(product)
#         print(f"Product count {i+1} Product ID: {product.id}, Listing Quality Score: {score}")

# product_list = DatabaseModel.list_documents(Product.objects)
# batch_size = 100
# threads = []

# for start_index in range(0, len(product_list), batch_size):
#     end_index = start_index + batch_size
#     thread = threading.Thread(target=process_products_in_batches, args=(start_index, end_index, product_list))
#     threads.append(thread)
#     thread.start()

# # Wait for all threads to complete
# for thread in threads:
#     thread.join()



# shared_revenue_utilities.py

from datetime import datetime, timedelta
from bson import ObjectId
import pytz
from concurrent.futures import ThreadPoolExecutor
from rest_framework.parsers import JSONParser
from collections import defaultdict
from django.http import JsonResponse


def parse_common_request_parameters(request):
    """Extract and validate common request parameters across all APIs"""
    json_request = JSONParser().parse(request)
    
    return {
        'preset': json_request.get("preset", "Today"),
        'marketplace_id': json_request.get("marketplace_id", None),
        'brand_id': json_request.get("brand_id", []),
        'product_id': json_request.get("product_id", []),
        'manufacturer_name': json_request.get("manufacturer_name", []),
        'fulfillment_channel': json_request.get("fulfillment_channel", None),
        'timezone_str': "US/Pacific",
        'start_date': json_request.get("start_date", None),
        'end_date': json_request.get("end_date", None),
        'compare_startdate': json_request.get("compare_startdate"),
        'compare_enddate': json_request.get("compare_enddate")
    }


def process_date_range(params):
    """Process start/end dates and comparison dates"""
    start_date = params['start_date']
    end_date = params['end_date']
    timezone_str = params['timezone_str']
    preset = params['preset']
    
    # Process main date range
    if start_date not in [None, ""]:
        start_date, end_date = convertdateTotimezone(start_date, end_date, timezone_str)
    else:
        start_date, end_date = get_date_range(preset, timezone_str)
    
    # Process comparison dates
    compare_enabled = params['compare_startdate'] not in [None, ""]
    compare_startdate = compare_enddate = None
    
    if compare_enabled:
        compare_startdate = datetime.strptime(params['compare_startdate'], "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        compare_enddate = datetime.strptime(params['compare_enddate'], "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, microsecond=0
        )
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'compare_enabled': compare_enabled,
        'compare_startdate': compare_startdate,
        'compare_enddate': compare_enddate
    }


def build_order_match_conditions(start_date, end_date, marketplace_id=None, brand_id=None, 
                                product_id=None, manufacturer_name=None, fulfillment_channel=None, 
                                order_status_filter="active"):
    """Build common MongoDB match conditions for order queries"""
    if timezone != 'UTC':
        start_date, end_date = convertLocalTimeToUTC(start_date, end_date, timezone)
    
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)
    
    match = {
        'order_date': {"$gte": start_date, "$lte": end_date},
        'order_total': {"$gt": 0}
    }
    
    # Handle different order status filters
    if order_status_filter == "active":
        match['order_status'] = {"$nin": ["Canceled", "Cancelled"]}
    elif order_status_filter == "refunded":
        match['order_status'] = "Refunded"
    
    # Add filters
    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    
    if marketplace_id not in [None, "", "all", "custom"]:
        match['marketplace_id'] = ObjectId(marketplace_id)
    
    # Handle product/brand/manufacturer filters
    if manufacturer_name not in [None, "", []]:
        ids = getproductIdListBasedonManufacture(manufacturer_name, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif product_id not in [None, "", []]:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif brand_id not in [None, "", []]:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id, start_date, end_date)
        match["_id"] = {"$in": ids}
    
    return match


def build_item_details_pipeline(item_ids, include_category=False):
    """Build common aggregation pipeline for fetching item details"""
    project_stage = {
        "_id": 1,
        "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
        "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
        "promotion_discount": {"$ifNull": ["$Pricing.PromotionDiscount.Amount", 0]},
        "sku": "$product_ins.sku",
        "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
        "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
        "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
        "vendor_discount": {"$ifNull": ["$product_ins.vendor_discount", 0]},
        "referral_fee": {"$ifNull": ["$product_ins.referral_fee", 0]},
        "walmart_fee": {"$ifNull": ["$product_ins.walmart_fee", 0]},
        "product_cost": {"$ifNull": ["$product_ins.product_cost", 0]},
        "w_product_cost": {"$ifNull": ["$product_ins.w_product_cost", 0]},
        "a_shipping_cost": {"$ifNull": ["$product_ins.a_shipping_cost", 0]},
        "w_shiping_cost": {"$ifNull": ["$product_ins.w_shiping_cost", 0]},
        "QuantityOrdered": {"$ifNull": ["$ProductDetails.QuantityOrdered", 1]},
        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
    }
    
    if include_category:
        project_stage["category"] = "$product_ins.category"
        project_stage["p_id"] = "$product_ins._id"
    
    return [
        {"$match": {"_id": {"$in": item_ids}}},
        {
            "$lookup": {
                "from": "product",
                "localField": "ProductDetails.product_id",
                "foreignField": "_id",
                "as": "product_ins"
            }
        },
        {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
        {"$project": project_stage}
    ]


def chunked_aggregate_items(pipeline_base, id_list, chunk_size=5000):
    """Process large item lists in chunks to avoid memory issues"""
    results = {}
    for i in range(0, len(id_list), chunk_size):
        chunk = id_list[i:i + chunk_size]
        pipeline = pipeline_base.copy()
        pipeline[0] = {"$match": {"_id": {"$in": chunk}}}
        for item in OrderItems.objects.aggregate(*pipeline):
            results[str(item['_id'])] = item
    return results


def calculate_merchant_shipping_cost(order, fulfillment_channel):
    """Calculate merchant shipping cost based on fulfillment channel"""
    merchant_shipment_cost = order.get('merchant_shipment_cost', 0)
    
    if merchant_shipment_cost is None:
        if fulfillment_channel == 'AFN':
            merchant_shipment_cost = order.get('shipping_price', 0) or 0
        elif fulfillment_channel in ['MFN', 'SellerFulfilled']:
            merchant_shipment_cost = order.get('merchant_shipment_cost', 0) or 0
        else:
            merchant_shipment_cost = 0
    
    return float(merchant_shipment_cost or 0)


def process_item_pricing(item_data):
    """Extract and process pricing information from item data"""
    price = item_data.get('price', 0) or 0
    
    # Handle alternative pricing structure
    if price == 0 and 'charges' in item_data:
        price = sum(float(charge.get('chargeAmount', 0)) for charge in item_data['charges'])
    
    return {
        'price': float(price),
        'tax_price': float(item_data.get('tax_price', 0) or 0),
        'promotion_discount': float(item_data.get('promotion_discount', 0) or 0),
        'quantity': int(item_data.get('QuantityOrdered', 1) or 1),
        'product_cost': float(item_data.get('product_cost', 0) or 0),
        'referral_fee': float(item_data.get('referral_fee', 0) or 0),
        'vendor_funding': float(item_data.get('vendor_funding', 0) or 0),
        'vendor_discount': float(item_data.get('vendor_discount', 0) or 0),
        'sku': item_data.get('sku')
    }


def calculate_core_metrics(orders, item_details_map, include_sessions=False, timezone_str='UTC'):
    """Calculate core revenue metrics from orders and item details"""
    metrics = {
        'gross_revenue': 0,
        'temp_price': 0,
        'tax_price': 0,
        'total_cogs': 0,
        'total_units': 0,
        'shipping_cost': 0,
        'referral_fee_total': 0,
        'vendor_funding': 0,
        'vendor_discount': 0,
        'promotion_discount': 0,
        'sku_set': set(),
        'unique_order_ids': set(),
        'product_ids': set()
    }
    
    for order in orders:
        metrics['gross_revenue'] += order.get('original_order_total', 0)
        metrics['shipping_cost'] += order.get('shipping_price', 0) or 0
        
        # Track unique order IDs
        po_id = order.get('purchase_order_id')
        if po_id:
            metrics['unique_order_ids'].add(po_id)
        
        # Process items in order
        for item_id in order['order_items']:
            item_data = item_details_map.get(str(item_id))
            if not item_data:
                continue
            
            pricing = process_item_pricing(item_data)
            
            metrics['temp_price'] += pricing['price']
            metrics['tax_price'] += pricing['tax_price']
            metrics['promotion_discount'] += pricing['promotion_discount']
            metrics['total_units'] += pricing['quantity']
            metrics['total_cogs'] += pricing['product_cost'] * pricing['quantity']
            metrics['referral_fee_total'] += pricing['referral_fee'] * pricing['quantity']
            metrics['vendor_funding'] += pricing['vendor_funding'] * pricing['quantity']
            metrics['vendor_discount'] += pricing['vendor_discount']
            
            if pricing['sku']:
                metrics['sku_set'].add(pricing['sku'])
            
            if include_sessions and 'p_id' in item_data:
                metrics['product_ids'].add(item_data['p_id'])
        
        # Add merchant shipping costs
        fulfillment_channel = order.get('fulfillment_channel', "")
        merchant_cost = calculate_merchant_shipping_cost(order, fulfillment_channel)
        metrics['total_cogs'] += merchant_cost
    
    # Calculate derived metrics
    expenses = metrics['total_cogs'] + metrics['referral_fee_total']
    net_profit = (metrics['temp_price'] + metrics['shipping_cost'] + 
                  metrics['promotion_discount'] + metrics['vendor_funding'] - 
                  (metrics['referral_fee_total'] + metrics['total_cogs'] + metrics['vendor_discount']))
    
    return {
        "grossRevenue": round(metrics['gross_revenue'], 2),
        "gross_revenue_with_tax": round(metrics['gross_revenue'], 2),  # For backward compatibility
        "expenses": round(expenses, 2),
        "netProfit": round(net_profit, 2),
        "net_profit": round(net_profit, 2),  # For backward compatibility
        "roi": round((net_profit / expenses) * 100, 2) if expenses > 0 else 0,
        "unitsSold": metrics['total_units'],
        "units_sold": metrics['total_units'],  # For backward compatibility
        "orders": len(metrics['unique_order_ids']),
        "skuCount": len(metrics['sku_set']),
        "margin": round((net_profit / metrics['gross_revenue']) * 100, 2) if metrics['gross_revenue'] > 0 else 0,
        "profit_margin": round((net_profit / metrics['gross_revenue']) * 100, 2) if metrics['gross_revenue'] > 0 else 0,
        "sessions": 0,  # Will be updated if sessions are requested
        "pageViews": 0,  # Will be updated if page views are requested
        "unitSessionPercentage": 0,  # Will be updated if sessions are available
        "seller": "",
        "tax_price": round(metrics['tax_price'], 2),
        "total_cogs": round(metrics['total_cogs'], 2),
        "product_cost": round(metrics['temp_price'], 2),
        "shipping_cost": round(metrics['shipping_cost'], 2),
        "channel_fee": round(metrics['referral_fee_total'], 2),
        "refund_amount": 0,  # Will be updated by refund calculation
        "refund_quantity": 0,  # Will be updated by refund calculation
        "refunds": 0  # Will be updated by refund calculation
    }


def fetch_sessions_and_pageviews(product_ids, start_date, end_date, timezone_str='UTC'):
    """Fetch sessions and page views for given products and date range"""
    if timezone_str != 'UTC':
        start_date, end_date = convertLocalTimeToUTC(start_date, end_date, timezone_str)
    
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)
    
    pipeline = [
        {
            "$match": {
                "product_id": {"$in": list(product_ids)},
                "date": {"$gte": start_date, "$lte": end_date}
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
    
    result = list(pageview_session_count.objects.aggregate(*pipeline))
    if result:
        return result[0].get('page_views', 0), result[0].get('sessions', 0)
    return 0, 0


def calculate_percentage_change(current, previous):
    """Calculate percentage change between current and previous values"""
    if previous == 0:
        return 0
    return round(((current - previous) / previous * 100), 2)


def create_delta_response(current_metrics, previous_metrics, metrics_list):
    """Create response with delta calculations for specified metrics"""
    return {
        metric: {
            "current": current_metrics.get(metric, 0),
            "previous": previous_metrics.get(metric, 0),
            "delta": round(current_metrics.get(metric, 0) - previous_metrics.get(metric, 0), 2)
        } for metric in metrics_list
    }


def execute_parallel_calculations(calculation_tasks, max_workers=4):
    """Execute multiple calculation tasks in parallel using ThreadPoolExecutor"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            key: executor.submit(task['function'], *task['args'], **task.get('kwargs', {}))
            for key, task in calculation_tasks.items()
        }
        
        # Collect results
        results = {}
        for key, future in futures.items():
            results[key] = future.result()
        
        return results


def get_marketplace_info():
    """Get marketplace information dictionary"""
    pipeline = [{"$project": {"_id": 1, "name": 1, "image_url": 1}}]
    marketplace_list = list(Marketplace.objects.aggregate(*pipeline))
    
    marketplace_dict = {str(mp['_id']): mp for mp in marketplace_list}
    
    # Add default images for known marketplaces
    for mp_id, mp in marketplace_dict.items():
        if mp['name'] == "Amazon":
            mp['default_image'] = "https://i.pinimg.com/originals/01/ca/da/01cada77a0a7d326d85b7969fe26a728.jpg"
        elif mp['name'] == "Walmart":
            mp['default_image'] = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRzjtf8dzq48TtkzeRYx2-_li3gTCkstX2juA&s"
        else:
            mp['default_image'] = ""
    
    return marketplace_dict


def sanitize_data(value):
    """Sanitize numeric data to handle None/invalid values"""
    try:
        return float(value) if value is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def to_utc_format(dt):
    """Convert datetime to UTC format string"""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_local_date_string(dt, tz_str):
    """Convert datetime to local timezone date string"""
    local_tz = pytz.timezone(tz_str)
    return dt.astimezone(local_tz).strftime("%Y-%m-%d")


# Refactored main calculation function that can be used by all APIs
def unified_revenue_calculation(start_date, end_date, marketplace_id=None, brand_id=None, 
                               product_id=None, manufacturer_name=None, fulfillment_channel=None, 
                               timezone_str="UTC", include_sessions=False, include_refunds=True,
                               include_category=False):
    """
    Unified function to calculate revenue metrics that can be used by all APIs
    
    Args:
        start_date, end_date: Date range
        marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel: Filters
        timezone_str: Timezone for date conversion
        include_sessions: Whether to fetch session/pageview data
        include_refunds: Whether to include refund calculations
        include_category: Whether to include category information
    
    Returns:
        Dictionary with calculated metrics
    """
    
    # Get orders data
    orders = grossRevenue(start_date, end_date, marketplace_id, brand_id, 
                         product_id, manufacturer_name, fulfillment_channel, timezone_str)
    
    # Get all item IDs
    all_item_ids = [item_id for order in orders for item_id in order['order_items']]
    
    if not all_item_ids:
        return get_empty_metrics()
    
    # Build and execute item details pipeline
    pipeline = build_item_details_pipeline(all_item_ids, include_category)
    item_details_map = chunked_aggregate_items(pipeline, all_item_ids)
    
    # Calculate core metrics
    metrics = calculate_core_metrics(orders, item_details_map, include_sessions, timezone_str)
    
    # Add session and pageview data if requested
    if include_sessions and metrics.get('product_ids'):
        page_views, sessions = fetch_sessions_and_pageviews(
            metrics['product_ids'], start_date, end_date, timezone_str
        )
        metrics.update({
            "sessions": sessions,
            "pageViews": page_views,
            "unitSessionPercentage": round((metrics['unitsSold'] / sessions) * 100, 2) if sessions else 0
        })
    
    # Add refund data if requested
    if include_refunds:
        refund_data = refundOrder(start_date, end_date, marketplace_id, brand_id, 
                                product_id, manufacturer_name, fulfillment_channel, timezone_str)
        refund_amount = sum(order.get('order_total', 0) for order in refund_data)
        refund_quantity = sum(len(order.get('order_items', [])) for order in refund_data)
        
        metrics.update({
            "refund_amount": round(refund_amount, 2),
            "refund_quantity": refund_quantity,
            "refunds": len(refund_data)
        })
    
    return metrics


def get_empty_metrics():
    """Return empty metrics structure"""
    return {
        "grossRevenue": 0, "gross_revenue_with_tax": 0, "expenses": 0, "netProfit": 0, "net_profit": 0,
        "roi": 0, "unitsSold": 0, "units_sold": 0, "orders": 0, "skuCount": 0, "margin": 0, 
        "profit_margin": 0, "sessions": 0, "pageViews": 0, "unitSessionPercentage": 0,
        "seller": "", "tax_price": 0, "total_cogs": 0, "product_cost": 0, "shipping_cost": 0,
        "channel_fee": 0, "refund_amount": 0, "refund_quantity": 0, "refunds": 0
    }
def get_common_calc_kwargs(params, include_sessions=False):
    """Get common calculation kwargs for API calls"""
    return {
        'marketplace_id': params['marketplace_id'],
        'brand_id': params['brand_id'],
        'product_id': params['product_id'],
        'manufacturer_name': params['manufacturer_name'],
        'fulfillment_channel': params['fulfillment_channel'],
        'timezone_str': params['timezone_str'],
        'include_sessions': include_sessions,
        'include_refunds': True
    }
def create_period_response_v2(label, cur_from, cur_to, prev_from, prev_to, current_metrics, previous_metrics, timezone_str):
    """Create period response with date ranges and formatted metrics"""
    date_ranges = {
        "current": {
            "from": to_utc_format(cur_from),
            "to": to_utc_format(cur_to),
            "from_local": to_local_date_string(cur_from, timezone_str),
            "to_local": to_local_date_string(cur_to, timezone_str)
        },
        "previous": {
            "from": to_utc_format(prev_from),
            "to": to_utc_format(prev_to),
            "from_local": to_local_date_string(prev_from, timezone_str),
            "to_local": to_local_date_string(prev_to, timezone_str)
        }
    }
    
    # Define metrics to include with delta
    summary_metrics = [
        "grossRevenue", "netProfit", "expenses", "unitsSold", "refunds", "skuCount",
        "sessions", "pageViews", "unitSessionPercentage", "margin", "roi", "orders"
    ]
    
    summary = create_delta_response(current_metrics, previous_metrics, summary_metrics)
    
    # Create net profit calculation breakdown
    netProfitCalculation = {
        "current": create_net_profit_breakdown(current_metrics),
        "previous": create_net_profit_breakdown(previous_metrics)
    }
    
    return {
        "dateRanges": date_ranges,
        "summary": summary,
        "netProfitCalculation": netProfitCalculation
    }
def create_net_profit_breakdown(metrics):
    """Create net profit calculation breakdown"""
    return {
        "gross": sanitize_data(metrics.get("grossRevenue", 0)),
        "totalCosts": sanitize_data(metrics.get("expenses", 0)),
        "productRefunds": sanitize_data(metrics.get("refunds", 0)),
        "totalTax": sanitize_data(metrics.get("tax_price", 0)),
        "totalTaxWithheld": 0,
        "ppcProductCost": 0,
        "ppcBrandsCost": 0,
        "ppcDisplayCost": 0,
        "ppcStCost": 0,
        "cogs": sanitize_data(metrics.get("total_cogs", 0)),
        "product_cost": sanitize_data(metrics.get("product_cost", 0)),
        "shipping_cost": sanitize_data(metrics.get("shipping_cost", 0))
    }
