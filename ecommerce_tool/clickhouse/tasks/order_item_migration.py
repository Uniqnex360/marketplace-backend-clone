from celery import shared_task
import time
from datetime import datetime
from bson import ObjectId, DBRef
from omnisight.models import Order, OrderItems, Marketplace, Product
from clickhouse.config import client


@shared_task
def migrate_mongo_order_item_to_clickhouse_task():
    t0 = time.time()

    ORDER_BATCH = 250
    INSERT_BATCH = 100

    print("[START] Migration started...")

    def chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    # -----------------------------
    # Marketplace cache
    # -----------------------------
    marketplace_map = {
        str(m["_id"]): m.get("name", "")
        for m in Marketplace._get_collection().find({}, {"_id": 1, "name": 1})
    }

    print(f"[INFO] Marketplace cache loaded: {len(marketplace_map)}")

    def flush(buffer, batch_no):
        if not buffer:
            print(
                f"[FLUSH] Buffer empty for batch={batch_no}. Skipping ClickHouse execution."
            )
            return

        try:
            client.insert(
                "fact_order_items",
                buffer,
                column_names=[
                    "order_id",
                    "order_item_id",
                    "purchase_order_id",
                    "order_date",
                    "order_date_day",
                    "order_status",
                    "order_total",
                    "currency",
                    "marketplace_id",
                    "marketplace_name",
                    "country",
                    "channel",
                    "fulfillment_channel",
                    "brand_id",
                    "item_price",
                    "item_tax",
                    "quantity",
                    "sku",
                    "category",
                    "product_cost",
                    "cogs",
                    "referral_fee",
                    "gross_revenue",
                ],
                settings={
                    "max_memory_usage": 300_000_000,
                    "max_block_size": 500,
                    "async_insert": 1,
                    "wait_for_async_insert": 1,
                },
            )
            print(f"[FLUSH] batch={batch_no} successfully sent rows={len(buffer)}")

        except Exception as e:
            print(f"[ERROR] ClickHouse Driver rejected batch={batch_no}. Error: {e}")

    # -----------------------------
    # FIX: DBRef safe handler
    # -----------------------------
    def normalize_product_id(product_id):
        if not product_id:
            return None

        if isinstance(product_id, DBRef):
            return product_id.id

        if isinstance(product_id, ObjectId):
            return product_id

        if isinstance(product_id, str):
            try:
                return ObjectId(product_id)
            except Exception:
                return None

        return None

    def get_product_by_sku(sku):
        if not sku:
            return {}

        return (
            Product._get_collection().find_one(
                {"sku": sku},
                {
                    "_id": 1,
                    "product_cost": 1,
                    "referral_fee": 1,
                    "brand_id": 1,
                    "sku": 1,
                    "category": 1,
                },
            )
            or {}
        )

    def get_item(item_id):
        if isinstance(item_id, str):
            item_id = ObjectId(item_id)

        return OrderItems._get_collection().find_one(
            {"_id": item_id},
            {"_id": 1, "Pricing": 1, "ProductDetails": 1},
        )

    # -----------------------------
    # Date Filtering
    # -----------------------------
    start_date = "2026-06-01 00:00:00"
    end_date = "2026-06-13 00:00:00"

    print(
        f"[INFO] Querying MongoDB Order collection where order_date between '{start_date}' and '{end_date}'..."
    )

    order_cursor = (
        Order._get_collection()
        .find(
            {"migrate_date": {"$exists": True}},
            {
                "_id": 1,
                "order_items": 1,
                "order_date": 1,
                "purchase_order_id": 1,
                "marketplace_id": 1,
                "geo": 1,
                "channel": 1,
                "fulfillment_channel": 1,
                "order_status": 1,
                "order_total": 1,
                "currency": 1,
            },
        )
        .batch_size(ORDER_BATCH)
    )

    insert_buffer = []
    batch_no = 1
    rows = 0
    orders_count = 0

    for order in order_cursor:
        orders_count += 1
        order_id = str(order["_id"])

        print(f"\n[PROCESSING] Order #{orders_count} | {order_id}")

        purchase_order_id = str(order.get("purchase_order_id") or "")
        marketplace_id = str(order.get("marketplace_id") or "")
        marketplace_name = marketplace_map.get(marketplace_id, "")

        country = order.get("geo") or ""
        channel = order.get("channel") or ""
        fulfillment_channel = order.get("fulfillment_channel") or ""
        order_status = order.get("order_status") or ""
        order_total = order.get("order_total")
        currency = order.get("currency")

        # ---------------- FIX: SAFE DATE HANDLING ----------------
        order_date_raw = order.get("order_date")
        order_date_day = None

        if isinstance(order_date_raw, str):
            try:
                order_date_raw = datetime.fromisoformat(order_date_raw)
            except Exception:
                order_date_raw = None

        if isinstance(order_date_raw, datetime):
            order_date_day = order_date_raw.date()
        # ---------------------------------------------------------

        order_items_list = order.get("order_items", [])

        if not order_items_list:
            print(f"  [SKIP] No order items for {order_id}")
            continue

        print(f"  [INFO] Items found: {len(order_items_list)}")

        for iid in order_items_list:
            item = get_item(iid)

            if not item:
                print(f"    [SKIP] Item not found: {iid}")
                continue

            pricing = item.get("Pricing", {})

            item_price = float(pricing.get("ItemPrice", {}).get("Amount", 0) or 0)
            item_tax = float(pricing.get("ItemTax", {}).get("Amount", 0) or 0)

            quantity = int(
                item.get("ProductDetails", {}).get("QuantityOrdered", 1) or 1
            )

            sku = item.get("ProductDetails", {}).get("SKU", "")

            product = get_product_by_sku(sku)

            brand_id = str(product.get("brand_id") or "")
            product_cost = float(product.get("product_cost", 0) or 0)
            referral_fee = float(product.get("referral_fee", 0) or 0)
            category = product.get("category", "")

            record_tuple = (
                order_id,
                str(iid),
                purchase_order_id,
                order_date_raw,  # ✅ FIXED HERE
                order_date_day,
                order_status,
                order_total,
                currency,
                marketplace_id,
                marketplace_name,
                country,
                channel,
                fulfillment_channel,
                brand_id,
                item_price,
                item_tax,
                quantity,
                sku,
                category,
                product_cost,
                product_cost * quantity,
                referral_fee,
                item_price * quantity,
            )

            insert_buffer.append(record_tuple)
            rows += 1

            if len(insert_buffer) >= INSERT_BATCH:
                print(f"\n[FLUSH TRIGGER] Batch {batch_no}")
                flush(insert_buffer, batch_no)
                insert_buffer.clear()
                batch_no += 1

        if orders_count % ORDER_BATCH == 0:
            print(f"\n--- Progress: {orders_count} orders processed ---")

    # Final flush
    if insert_buffer:
        print(f"\n[FINAL FLUSH] {len(insert_buffer)} rows")
        flush(insert_buffer, batch_no)

    print("\n[DONE] Migration finished")
    print(f"Orders: {orders_count}, Rows: {rows}")

    return {
        "status": "success",
        "orders": orders_count,
        "rows": rows,
        "time_sec": round(time.time() - t0, 2),
    }
