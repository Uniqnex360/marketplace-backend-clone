from __future__ import annotations
from celery import shared_task
from omnisight.operations.walmart_operations import syncRecentWalmartOrders, syncWalmartPrice
from omnisight.operations.amazon_operations import syncRecentAmazonOrders,sync_inventory, syncPageviews,FetchProductsDetails
from omnisight.operations.general_functions import backfill_missing_merchant_shipment_cost
from ecommerce_tool.util.redis_lock import redis_lock
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import *
from sp_api.api import Orders
from collections import defaultdict
from sp_api.base import Marketplaces, SellingApiException
from mongoengine.queryset.visitor import Q
from ecommerce_tool.settings import  Role_ARN, Acccess_Key, Secret_Access_Key,AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN
import logging
import random
logger = logging.getLogger(__name__)
import time
@shared_task
def backfill_missing_shipping_cost(batch_size=200):
    with redis_lock("backfill_shipping_cost_lock", timeout=1800) as acquired:
        if not acquired:
            print("backfill_missing_shipping_cost is already running. Skipping.")
            return "Skipped - already running"
        print("Starting Merchant Shipment Cost Backfill...")
        return backfill_missing_merchant_shipment_cost(batch_size=batch_size)
@shared_task
def sync_orders():
    with redis_lock("sync_orders_lock", timeout=3600) as acquired:
        if not acquired:
            print("sync_orders is already running. Skipping.")
            return "Skipped - already running"
        print("Amazon Orders Sync starting........................")
        amazon_orders = syncRecentAmazonOrders()
        print("Amazon Orders Sync completed........................")
        return f"Synced {amazon_orders} Amazon orders"
@shared_task
def fetch_item_full_pricing_from_amazon(batch_size=80):
    """
    Fetch OrderItems with missing/zero ItemTax, PromotionDiscount, or ShipPromotionDiscount
    and update all pricing fields from Amazon.
    """
    lock_name = "fetch_item_full_pricing_from_amazon"
    with redis_lock(lock_name, timeout=3600) as acquired:
        if not acquired:
            logger.warning(f"{lock_name} is already running. Skipping...")
            return {"updated": 0, "skipped": 0, "errors": 0, "scanned": 0}
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "order",
                        "localField": "OrderId", 
                        "foreignField": "purchase_order_id",
                        "as": "order_info"
                    }
                },
                {
                    "$match": {
                        "order_info.fulfillment_channel": {"$in": ["MFN", "AFN"]},
                        "order_info.order_status": {"$in": ["Shipped", "Delivered"]},
                        "pricing_checked": {"$ne": True},
                        "$or": [
                            {"Pricing.PromotionDiscount.Amount": {"$eq": 0.0}},
                            {"Pricing.ShipPromotionDiscount.Amount": {"$eq": 0.0}},
                            {"Pricing.PromotionDiscount.Amount": {"$exists": False}},
                            {"Pricing.ShipPromotionDiscount.Amount": {"$exists": False}},
                        ],
                    }
                },
                {"$sort": {"order_info.order_date": -1}},
                {"$limit": batch_size},
                {
                    "$project": {
                        "OrderId": 1,
                        "ProductDetails": 1,
                        "Pricing": 1
                    }
                }
            ]

            db_items = list(OrderItems.objects.aggregate(pipeline))
            if not db_items:
                logger.info("No OrderItems with missing pricing info found.")
                return {"updated": 0, "skipped": 0, "errors": 0, "scanned": 0}

            items_by_order = defaultdict(list)
            for item in db_items:
                items_by_order[item["OrderId"]].append(item)

            # Initialize Amazon client
            try:
                credentials = {
                    'lwa_app_id': AMAZON_API_KEY,
                    'lwa_client_secret': AMAZON_SECRET_KEY,
                    'refresh_token': REFRESH_TOKEN,
                    'aws_access_key': Acccess_Key,
                    'aws_secret_key': Secret_Access_Key,
                    'role_arn': Role_ARN,
                }
                client = Orders(credentials=credentials, marketplace=Marketplaces.US)
            except Exception as e:
                logger.error(f"Failed to initialize Amazon API client: {e}")
                return {"updated": 0, "skipped": 0, "errors": 1, "scanned": 0}

            updated, skipped, errors = 0, 0, 0

            for order_id, items in items_by_order.items():
                logger.info(f"🔍 Processing order {order_id} ({len(items)} items)")
                retry_count, max_retries, base_delay = 0, 5, 2

                while retry_count <= max_retries:
                    try:
                        response = client.get_order_items(order_id)
                        if not response or not hasattr(response, 'payload'):
                            logger.warning(f"Invalid response for order {order_id}")
                            errors += 1
                            break

                        order_items_data = response.payload.get("OrderItems", [])
                        if not order_items_data:
                            logger.info(f"No items found in Amazon response for {order_id}")
                            skipped += len(items)
                            break

                        amz_items = {item.get("SellerSKU"): item for item in order_items_data if item.get("SellerSKU")}

                        for item_data in items:
                            try:
                                sku = item_data.get("ProductDetails", {}).get("SKU")
                                if not sku:
                                    logger.warning(f"Missing SKU in order {order_id}")
                                    errors += 1
                                    continue

                                amz_item = amz_items.get(sku)
                                if not amz_item:
                                    logger.debug(f"SKU {sku} not found in Amazon response for order {order_id}")
                                    skipped += 1
                                    continue

                                # Extract all pricing data
                                promotion_data = amz_item.get("PromotionDiscount", {}) or {}
                                ship_discount_data = amz_item.get("ShippingDiscount", {}) or {}

                                new_promotion = round(float(promotion_data.get("Amount", 0.0) or 0.0), 2)
                                new_ship_discount = round(float(ship_discount_data.get("Amount", 0.0) or 0.0), 2)

                                currency_promo = promotion_data.get("CurrencyCode", "USD")
                                currency_ship = ship_discount_data.get("CurrencyCode", "USD")

                                update_result = DatabaseModel.update_documents(
                                    OrderItems.objects,
                                    {"OrderId": order_id, "ProductDetails__SKU": sku},
                                    {
                                        "set__Pricing__PromotionDiscount__Amount": new_promotion,
                                        "set__Pricing__PromotionDiscount__CurrencyCode": currency_promo,
                                        "set__Pricing__ShipPromotionDiscount__Amount": new_ship_discount,
                                        "set__Pricing__ShipPromotionDiscount__CurrencyCode": currency_ship,
                                        "set__pricing_checked": True
                                    }
                                )

                                if update_result:
                                    logger.info(f"Updated full pricing for {order_id} - {sku}")
                                    updated += 1
                                else:
                                    logger.warning(f"Failed to update full pricing for {order_id} - {sku}")
                                    errors += 1

                            except Exception as item_error:
                                logger.exception(f"Failed processing item in order {order_id}: {item_error}")
                                errors += 1

                        time.sleep(0.5)
                        break

                    except SellingApiException as api_error:
                        error_message = str(api_error)
                        if "QuotaExceeded" in error_message or "RequestThrottled" in error_message:
                            retry_count += 1
                            if retry_count > max_retries:
                                logger.error(f"Max retries exceeded for order {order_id}")
                                errors += 1
                                break
                            delay = min(300, (base_delay ** retry_count) + random.uniform(0, 2))
                            logger.warning(f"Rate limit hit for order {order_id}, retry {retry_count} in {delay:.1f}s")
                            time.sleep(delay)
                        else:
                            logger.error(f"SP-API error for order {order_id}: {api_error}")
                            errors += 1
                            break
                    except Exception as general_error:
                        logger.exception(f"Unexpected error for order {order_id}: {general_error}")
                        errors += 1
                        break

            summary = {"updated": updated, "skipped": skipped, "errors": errors, "scanned": len(db_items)}
            logger.info(f"Amazon full pricing sync summary: {summary}")
            return summary

        except Exception as task_error:
            logger.exception(f"Fatal error in fetch_item_full_pricing_from_amazon task: {task_error}")
            return {"updated": 0, "skipped": 0, "errors": 1, "scanned": 0}

@shared_task
def fetch_item_tax_from_amazon(batch_size=80):
    lock_name = "fetch_item_tax_from_amazon"
    with redis_lock(lock_name, timeout=3600) as acquired:
        if not acquired:
            logger.warning(f"{lock_name} is already running. Skipping...")
            return {"updated": 0, "skipped": 0, "errors": 0, "scanned": 0}
        try:
            pipeline = [
                {
                    "$lookup": {
                        "from": "order",
                        "localField": "OrderId", 
                        "foreignField": "purchase_order_id",
                        "as": "order_info"
                    }
                },
                {
                    "$match": {
                        "order_info.fulfillment_channel": {"$in": ["MFN", "AFN"]},
                        "order_info.order_status": {"$in": ["Shipped", "Delivered"]},
                        "tax_checked": {"$ne": True},
                        "$or": [
                            {"Pricing.ItemTax.Amount": {"$eq": 0.0}},
                            {"Pricing.ItemTax.Amount": {"$exists": False}},
                            {"Pricing.ItemTax": {"$exists": False}}
                        ],
                    }
                },
                {"$sort": {"order_info.order_date": -1}},
                {"$limit": batch_size},
                {
                    "$project": {
                        "OrderId": 1,
                        "ProductDetails": 1,
                        "Pricing": 1
                    }
                }
            ]
            db_items = list(OrderItems.objects.aggregate(pipeline))
            if not db_items:
                logger.info("No OrderItems with missing/zero ItemTax found.")
                return {"updated": 0, "skipped": 0, "errors": 0, "scanned": 0}
            items_by_order = defaultdict(list)
            for item in db_items:
                items_by_order[item["OrderId"]].append(item)
            try:
                credentials = {
                    'lwa_app_id': AMAZON_API_KEY,
                    'lwa_client_secret': AMAZON_SECRET_KEY,
                    'refresh_token': REFRESH_TOKEN,
                    'aws_access_key': Acccess_Key,
                    'aws_secret_key': Secret_Access_Key,
                    'role_arn': Role_ARN,
                }
                client = Orders(credentials=credentials, marketplace=Marketplaces.US)
            except Exception as e:
                logger.error(f"Failed to initialize Amazon API client: {e}")
                return {"updated": 0, "skipped": 0, "errors": 1, "scanned": 0}
            updated, skipped, errors = 0, 0, 0
            for order_id, items in items_by_order.items():
                logger.info(f"🔍 Starting processing for purchase_order_id={order_id} with {len(items)} items")
                retry_count = 0
                max_retries = 5
                base_delay = 2
                while retry_count <= max_retries:
                    try:
                        response = client.get_order_items(order_id)
                        if not response or not hasattr(response, 'payload'):
                            logger.warning(f"Invalid response structure for order {order_id}")
                            errors += 1
                            break
                        order_items_data = response.payload.get("OrderItems", [])
                        if not order_items_data:
                            logger.info(f"No order items found in response for {order_id}")
                            skipped += len(items)
                            break
                        amz_items = {}
                        for item in order_items_data:
                            seller_sku = item.get("SellerSKU")
                            if seller_sku:
                                amz_items[seller_sku] = item
                        for item_data in items:
                            try:
                                product_details = item_data.get("ProductDetails", {})
                                sku = product_details.get("SKU")
                                if not sku:
                                    logger.warning(f"Missing SKU in order {order_id}")
                                    errors += 1
                                    continue
                                amz_item = amz_items.get(sku)
                                if not amz_item:
                                    logger.debug(f"SKU {sku} not found in Amazon response for order {order_id}")
                                    skipped += 1
                                    continue
                                item_tax_data = amz_item.get("ItemTax", {})
                                if not isinstance(item_tax_data, dict):
                                    logger.warning(f"Invalid ItemTax structure for {sku} in order {order_id}")
                                    skipped += 1
                                    continue
                                new_tax_amount = item_tax_data.get("Amount", 0.0)
                                currency = item_tax_data.get("CurrencyCode", "USD")
                                try:
                                    new_tax = round(float(new_tax_amount or 0.0), 2)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid tax amount '{new_tax_amount}' for {sku} in order {order_id}")
                                    errors += 1
                                    continue
                                current_tax = 0.0
                                pricing = item_data.get("Pricing", {})
                                if pricing and isinstance(pricing, dict):
                                    item_tax = pricing.get("ItemTax", {})
                                    if item_tax and isinstance(item_tax, dict):
                                        current_amount = item_tax.get("Amount", 0.0)
                                        try:
                                            current_tax = round(float(current_amount or 0.0), 2)
                                        except (ValueError, TypeError):
                                            current_tax = 0.0
                                should_update = False
                                if current_tax == 0.0 and new_tax > 0.0:
                                    should_update = True
                                elif current_tax > 0.0 and new_tax > 0.0 and abs(current_tax - new_tax) > 0.01:
                                    should_update = True
                                if should_update:
                                    update_result = DatabaseModel.update_documents(
                                        OrderItems.objects,
                                        {"OrderId": order_id, "ProductDetails__SKU": sku},
                                        {
                                            "set__Pricing__ItemTax__Amount": new_tax,
                                            "set__Pricing__ItemTax__CurrencyCode": currency,
                                            "set__tax_checked":True
                                        }
                                    )
                                    if update_result:
                                        logger.info(f"Updated tax for {order_id} - {sku}: {current_tax} → {new_tax}")
                                        updated += 1
                                    else:
                                        logger.warning(f"Failed to update tax for {order_id} - {sku}")
                                        errors += 1
                                else:
                                    DatabaseModel.update_documents(
                                        OrderItems.objects,
                                        {"OrderId": order_id, "ProductDetails__SKU": sku},
                                        {
                                            "set__tax_checked":True
                                        }
                                    )
                                    skipped += 1
                            except Exception as item_error:
                                logger.exception(f"Failed processing item in order {order_id}: {item_error}")
                                errors += 1
                        time.sleep(0.5)
                        break  
                    except SellingApiException as api_error:
                        error_message = str(api_error)
                        if "QuotaExceeded" in error_message or "RequestThrottled" in error_message:
                            retry_count += 1
                            if retry_count > max_retries:
                                logger.error(f"Max retries exceeded for order {order_id} due to rate limiting")
                                errors += 1
                                break
                            delay = min(300, (base_delay ** retry_count) + random.uniform(0, 2))
                            logger.warning(f"Rate limit hit for order {order_id}, retry {retry_count}/{max_retries} in {delay:.1f}s")
                            time.sleep(delay)
                        elif "InvalidInput" in error_message or "AccessDenied" in error_message:
                            logger.error(f"Permanent API error for order {order_id}: {api_error}")
                            errors += 1
                            break
                        else:
                            logger.warning(f"SP-API error for order {order_id}: {api_error}")
                            errors += 1
                            break
                    except Exception as general_error:
                        logger.exception(f"Unexpected error processing order {order_id}: {general_error}")
                        errors += 1
                        break
            summary = {
                "updated": updated, 
                "skipped": skipped, 
                "errors": errors, 
                "scanned": len(db_items)
            }
            logger.info(f"Amazon ItemTax sync summary: {summary}")
            return summary
        except Exception as task_error:
            logger.exception(f"Fatal error in fetch_item_tax_from_amazon task: {task_error}")
            return {"updated": 0, "skipped": 0, "errors": 1, "scanned": 0}
@shared_task
def sync_walmart_orders():
    with redis_lock("sync_walmart_orders_lock", timeout=1800) as acquired:
        if not acquired:
            print("sync_walmart_orders is already running. Skipping.")
            return "Skipped - already running"
        print("Walmart Orders Sync starting........................")
        walmart_orders = syncRecentWalmartOrders()
        print("Walmart Orders Sync completed........................")
        return f"Synced {len(walmart_orders)} orders"
@shared_task
def sync_products():
    with redis_lock("sync_products_lock", timeout=3600) as acquired:
        if not acquired:
            print("sync_products is already running. Skipping.")
            return "Skipped - already running"
        print("PageViews Sync starting........................")
        syncPageviews()
        return True
@shared_task
def sync_inventry():
    with redis_lock("sync_inventory_lock", timeout=3600) as acquired:
        if not acquired:
            print("sync_inventry is already running. Skipping.")
            return "Skipped - already running"
        print("Inventory Sync starting........................")
        sync_inventory()
        print("Inventory Sync completed........................")
        return True
@shared_task
def sync_price():
    with redis_lock("sync_price_lock", timeout=3600) as acquired:
        if not acquired:
            print("sync_price is already running. Skipping.")
            return "Skipped - already running"
        print("Amazon Price Sync starting........................")
        FetchProductsDetails()
        print("Amazon Price Sync completed........................")
        return True
@shared_task
def sync_WalmartPrice():
    with redis_lock("sync_walmart_price_lock", timeout=3600) as acquired:
        if not acquired:
            print("sync_WalmartPrice is already running. Skipping.")
            return "Skipped - already running"
        print("Walmart Price Sync starting........................")
        syncWalmartPrice()
        print("Walmart Price Sync completed........................")
        return True