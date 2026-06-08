from celery import shared_task
import time
from datetime import datetime
from bson import ObjectId
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
            yield lst[i:i + size]

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
            return

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

        print(f"[FLUSH] batch={batch_no} rows={len(buffer)}")

    def get_product(product_id):
        if not product_id:
            return {}

        if isinstance(product_id, str):
            product_id = ObjectId(product_id)

        return Product._get_collection().find_one(
            {"_id": product_id},
            {
                "_id": 1,
                "product_cost": 1,
                "referral_fee": 1,
                "brand_id": 1,
                "sku": 1,
                "category": 1,
            },
        ) or {}

    def get_item(item_id):
        if isinstance(item_id, str):
            item_id = ObjectId(item_id)

        return OrderItems._get_collection().find_one(
            {"_id": item_id},
            {"_id": 1, "Pricing": 1, "ProductDetails": 1},
        )

    # order_cursor = (
    #     Order._get_collection()
    #     .find(
    #         {},
    #         {
    #             "_id": 1,
    #             "order_items": 1,
    #             "order_date": 1,
    #             "purchase_order_id": 1,
    #             "marketplace_id": 1,
    #             "geo": 1,
    #             "channel": 1,
    #             "fulfillment_channel": 1,
    #             "order_status": 1,
    #             "order_total": 1,
    #             "currency": 1,
    #         },
    #     )
    #     .batch_size(ORDER_BATCH)
    # )

    from datetime import datetime

    # start_date = datetime(2026, 6, 1)
    # end_date = datetime(2026, 6, 13)  # exclusive
    start_date = "2026-06-01 00:00:00"
    end_date = "2026-06-13 00:00:00"

    order_cursor = (
        Order._get_collection()
        .find(
            {
                "order_date": {
                    "$gte": start_date,
                    "$lt": end_date,
                }
            },
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
        print("order id", order.get("_id"))
        orders_count += 1

        order_id = str(order["_id"])
        purchase_order_id = str(order.get("purchase_order_id") or "")
        marketplace_id = str(order.get("marketplace_id") or "")
        marketplace_name = marketplace_map.get(marketplace_id, "")

        country = order.get("geo") or ""
        channel = order.get("channel") or ""
        fulfillment_channel = order.get("fulfillment_channel") or ""
        order_status = order.get("order_status") or ""
        order_total = order.get("order_total")
        currency = order.get("currency")

        order_date = order.get("order_date")
        order_date_day = None

        if order_date:
            if isinstance(order_date, str):
                order_date = datetime.fromisoformat(order_date)
            order_date_day = order_date.date()

        for iid in order.get("order_items", []):

            item = get_item(iid)
            if not item:
                continue

            pricing = item.get("Pricing", {})

            item_price = float(pricing.get("ItemPrice", {}).get("Amount", 0) or 0)
            item_tax = float(pricing.get("ItemTax", {}).get("Amount", 0) or 0)

            quantity = int(
                item.get("ProductDetails", {}).get("QuantityOrdered", 1) or 1
            )

            product_id = item.get("ProductDetails", {}).get("product_id")
            product = get_product(product_id)

            brand_id = str(product.get("brand_id") or "")
            product_cost = float(product.get("product_cost", 0) or 0)
            referral_fee = float(product.get("referral_fee", 0) or 0)
            sku = product.get("sku", "")
            category = product.get("category", "")

            insert_buffer.append(
                (
                    order_id,
                    str(iid),
                    purchase_order_id,
                    order.get("order_date"),
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
            )

            rows += 1

            if len(insert_buffer) >= INSERT_BATCH:
                flush(insert_buffer, batch_no)
                insert_buffer.clear()
                batch_no += 1

        if orders_count % ORDER_BATCH == 0:
            print(f"[PROGRESS] orders={orders_count} rows={rows}")

    flush(insert_buffer, batch_no)

    print("[DONE] MIGRATION FINISHED")
    print(f"Orders: {orders_count}")
    print(f"Rows: {rows}")
    print(f"Time: {round(time.time() - t0, 2)}s")

    return {
        "status": "success",
        "orders": orders_count,
        "rows": rows,
        "time_sec": round(time.time() - t0, 2),
    }