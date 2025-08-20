from datetime import datetime, timedelta
from pytz import timezone
from mongoengine.queryset.visitor import Q
from mongoengine import connect
import os

connect(
    db="ecommerce_db",
    host="mongodb://plmp_admin:admin%401234@54.86.75.104:27017/"
)

# ✅ Import your models and utility functions
from omnisight.models import Order, CachedMetrics
from omnisight.operations.helium_utils import grossRevenue, refundOrder

def refresh_today_metrics():
    """
    Refreshes global CachedMetrics for today by checking if any Order was updated today.
    """
    
    # Step 1: Setup timezone (Pacific → UTC)
    tz = timezone("US/Pacific")
    now = datetime.now(tz)
    
    # Define today's range in local time
    today_start = tz.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    today_end = tz.localize(datetime(now.year, now.month, now.day, 23, 59, 59))
    
    # Convert to UTC for querying MongoDB
    start_utc = today_start.astimezone(timezone("UTC"))
    end_utc = today_end.astimezone(timezone("UTC"))
    
    print(f"[INFO] Refreshing CachedMetrics for date: {start_utc.date()}...")
    
    # Step 2: Check if any orders were updated today
    order_updates = Order.objects(
        Q(last_update_date__gte=start_utc) &
        Q(last_update_date__lte=end_utc)
    )
    
    if not order_updates:
        print("[SKIP] No order updates for today. No changes made.")
        return
    
    print(f"[PROCESS] Found {order_updates.count()} updated orders. Calculating metrics...")
    
    # Step 3: Run aggregation using grossRevenue()
    try:
        aggregation_result = grossRevenue(
            start_utc,
            end_utc,
            marketplace_id=None,
            brand_id=None,
            product_id=None,
            fulfillment_channel=None,
        )
        
        # Filter out cancelled orders and orders with total <= 0
        aggregation_result = [
            r for r in aggregation_result
            if r.get('order_status') not in ['Cancelled', 'Canceled'] and r.get('order_total', 0) > 0
        ]
        
        refund_data = refundOrder(
            start_utc,
            end_utc,
            marketplace_id=None,
            brand_id=None,
            product_id=None,
            fulfillment_channel=None
        )
    except Exception as e:
        print(f"[ERROR] Failed to fetch data: {e}")
        return
    
    # Step 4: Get all order item IDs for lookup
    all_order_item_ids = set()
    for order in aggregation_result:
        if 'order_items' in order and order['order_items']:
            all_order_item_ids.update(order['order_items'])
    
    print(f"[INFO] Found {len(all_order_item_ids)} order items to process")
    
    # Step 5: Bulk lookup order items with product data
    order_items_lookup = {}
    if all_order_item_ids:
        try:
            # Import OrderItems if not already imported
            from omnisight.models import OrderItems
            
            bulk_pipeline = [
                {"$match": {"_id": {"$in": list(all_order_item_ids)}}},
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
                    "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                    "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
                    "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
                    "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
                    "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                }}
            ]
            bulk_results = list(OrderItems.objects.aggregate(*bulk_pipeline))
            
            for item in bulk_results:
                order_items_lookup[item['_id']] = item
            
            print(f"[INFO] Successfully looked up {len(order_items_lookup)} order items")
            
        except Exception as e:
            print(f"[ERROR] Failed to lookup order items: {e}")
            # Continue with zero values if lookup fails
    
    # Step 6: Calculate all metrics
    try:
        gross_revenue_total = 0
        total_units = 0
        total_cogs = 0
        total_tax = 0
        temp_price = 0
        vendor_funding = 0
        
        # Calculate gross revenue and units
        for order in aggregation_result:
            gross_revenue_total += order.get('order_total', 0)
            total_units += order.get('items_order_quantity', 0)
            
            # Process each order item for detailed calculations
            for item_id in order.get('order_items', []):
                item_result = order_items_lookup.get(item_id)
                if item_result:
                    total_tax += item_result.get('tax_price', 0)
                    temp_price += item_result.get('price', 0)
                    
                    # Use different COGS based on marketplace
                    marketplace = order.get('marketplace_name', '')
                    if marketplace == "Amazon":
                        total_cogs += item_result.get('total_cogs', 0)
                    else:
                        total_cogs += item_result.get('w_total_cogs', 0)
                    
                    vendor_funding += item_result.get('vendor_funding', 0)
        
        # Calculate unique orders
        unique_order_ids = set()
        for order in aggregation_result:
            order_id = order.get('purchase_order_id') or str(order.get('_id', ''))
            if order_id:
                unique_order_ids.add(order_id)
        total_orders = len(unique_order_ids)
        
        # Calculate refunds
        refund_qty = 0
        if refund_data:
            for r in refund_data:
                order_items = r.get('order_items', [])
                if isinstance(order_items, list):
                    refund_qty += len(order_items)
        
        # Calculate net profit and margin
        net_profit = (temp_price - total_cogs) + vendor_funding
        margin = (net_profit / gross_revenue_total * 100) if gross_revenue_total > 0 else 0
        
        print(f"[METRICS] Gross Revenue: ${gross_revenue_total:.2f}")
        print(f"[METRICS] Total Orders: {total_orders}")
        print(f"[METRICS] Total Units: {total_units}")
        print(f"[METRICS] Total Tax: ${total_tax:.2f}")
        print(f"[METRICS] Total COGS: ${total_cogs:.2f}")
        print(f"[METRICS] Net Profit: ${net_profit:.2f}")
        print(f"[METRICS] Margin: {margin:.2f}%")
        print(f"[METRICS] Refunds: {refund_qty}")
        
    except Exception as e:
        print(f"[ERROR] Failed to calculate metrics: {e}")
        return
    
    # Step 7: Save (or update) to CachedMetrics
    try:
        # Use date without timezone for consistency
        cache_date = start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        result = CachedMetrics.objects(
            brand_id=None,
            marketplace_id=None,
            date=cache_date
        ).update_one(
            set__gross_revenue=round(gross_revenue_total, 2),
            set__net_profit=round(net_profit, 2),
            set__total_orders=total_orders,
            set__total_units=total_units,
            set__total_tax=round(total_tax, 2),
            set__refund=refund_qty,
            set__margin=round(margin, 2),
            set__total_cogs=round(total_cogs, 2),
            set__last_updated=datetime.utcnow(),
            upsert=True
        )
        
        print(f"[✔ SAVED] CachedMetrics for {start_utc.date()} successfully.")
        
    except Exception as e:
        print(f"[ERROR] Failed to save to CachedMetrics: {e}")
        return

# Run directly
if __name__ == "__main__":
    refresh_today_metrics()
