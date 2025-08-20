# from celery import shared_task
# from omnisight.operations.walmart_operations import syncRecentWalmartOrders, syncWalmartPrice
# from omnisight.operations.amazon_operations import syncRecentAmazonOrders,sync_inventory, syncPageviews,FetchProductsDetails
# from omnisight.operations.general_functions import backfill_missing_merchant_shipment_cost
# from ecommerce_tool.util.redis_lock import redis_lock

# @shared_task
# def backfill_missing_shipping_cost(batch_size=200):
#     with redis_lock("backfill_shipping_cost_lock", timeout=1800) as acquired:
#         if not acquired:
#             print("backfill_missing_shipping_cost is already running. Skipping.")
#             return "Skipped - already running"

#         print("Starting Merchant Shipment Cost Backfill...")
#         return backfill_missing_merchant_shipment_cost(batch_size=batch_size)
    
# @shared_task
# def sync_orders():
#     with redis_lock("sync_orders_lock", timeout=3600) as acquired:
#         if not acquired:
#             print("sync_orders is already running. Skipping.")
#             return "Skipped - already running"

#         print("Amazon Orders Sync starting........................")
#         amazon_orders = syncRecentAmazonOrders()
#         print("Amazon Orders Sync completed........................")
#         return f"Synced {amazon_orders} Amazon orders"
    
    
# @shared_task
# def sync_walmart_orders():
#     with redis_lock("sync_walmart_orders_lock", timeout=1800) as acquired:
#         if not acquired:
#             print("sync_walmart_orders is already running. Skipping.")
#             return "Skipped - already running"

#         print("Walmart Orders Sync starting........................")
#         walmart_orders = syncRecentWalmartOrders()
#         print("Walmart Orders Sync completed........................")
#         return f"Synced {len(walmart_orders)} orders"
    
# @shared_task
# def sync_products():
#     with redis_lock("sync_products_lock", timeout=3600) as acquired:
#         if not acquired:
#             print("sync_products is already running. Skipping.")
#             return "Skipped - already running"

#         print("PageViews Sync starting........................")
#         syncPageviews()
#         return True
    


# @shared_task
# def sync_inventry():
#     with redis_lock("sync_inventory_lock", timeout=3600) as acquired:
#         if not acquired:
#             print("sync_inventry is already running. Skipping.")
#             return "Skipped - already running"

#         print("Inventory Sync starting........................")
#         sync_inventory()
#         print("Inventory Sync completed........................")
#         return True

# @shared_task
# def sync_price():
#     with redis_lock("sync_price_lock", timeout=3600) as acquired:
#         if not acquired:
#             print("sync_price is already running. Skipping.")
#             return "Skipped - already running"

#         print("Amazon Price Sync starting........................")
#         FetchProductsDetails()
#         print("Amazon Price Sync completed........................")
#         return True
    
# @shared_task
# def sync_WalmartPrice():
#     with redis_lock("sync_walmart_price_lock", timeout=3600) as acquired:
#         if not acquired:
#             print("sync_WalmartPrice is already running. Skipping.")
#             return "Skipped - already running"

#         print("Walmart Price Sync starting........................")
#         syncWalmartPrice()
#         print("Walmart Price Sync completed........................")
#         return True