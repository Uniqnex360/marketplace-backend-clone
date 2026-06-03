from bson import ObjectId
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from clickhouse.config import client
from omnisight.models import Order, OrderItems, Product
from datetime import datetime
import time


@csrf_exempt
def migrate_mongo_order_item_to_clickhouse(request):

    t0 = time.time()

    ORDER_BATCH = 500
    ITEM_BATCH = 1000
    INSERT_BATCH = 500

    print("[START] Migration started...")

    def chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

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
        )

        print(f"[FLUSH] batch={batch_no} rows={len(buffer)}")

    def process_items(item_ids):
        print(f"[STEP] Fetching OrderItems: {len(item_ids)}")

        item_map = {}
        product_ids = set()

        for chunk in chunked(list(item_ids), ITEM_BATCH):
            cursor = OrderItems._get_collection().find(
                {"_id": {"$in": chunk}},
                {
                    "_id": 1,
                    "Pricing": 1,
                    "ProductDetails": 1,
                },
            )

            for item in cursor:
                item_id = str(item["_id"])
                item_map[item_id] = item

                pid = item.get("ProductDetails", {}).get("product_id")
                if pid:
                    product_ids.add(pid if isinstance(pid, ObjectId) else ObjectId(pid))

        print(f"[INFO] OrderItems loaded={len(item_map)} products={len(product_ids)}")

        product_map = {}

        for chunk in chunked(list(product_ids), ITEM_BATCH):
            cursor = Product._get_collection().find(
                {"_id": {"$in": chunk}},
                {
                    "_id": 1,
                    "product_cost": 1,
                    "referral_fee": 1,
                    "brand_id": 1,
                    "sku": 1,
                    "category": 1,
                },
            )

            for p in cursor:
                product_map[str(p["_id"])] = p

        print(f"[INFO] Products loaded={len(product_map)}")

        return item_map, product_map

    # IMPORTANT: raw mongo (fast + no mongoengine lazy loading issues)
    order_cursor = (
        Order._get_collection()
        .find(
            {},
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
    item_ids = set()

    batch_no = 1
    rows = 0
    orders_count = 0

    order_buffer = []

    for order in order_cursor:

        order_buffer.append(order)
        orders_count += 1

        # collect item ids
        for iid in order.get("order_items", []):
            item_ids.add(iid if isinstance(iid, ObjectId) else ObjectId(iid))

        # process batch
        if len(order_buffer) >= ORDER_BATCH:

            print(f"[BATCH] orders={len(order_buffer)} items={len(item_ids)}")

            item_map, product_map = process_items(item_ids)

            item_ids = set()

            for o in order_buffer:

                order_id = str(o["_id"])
                purchase_order_id = str(o["purchase_order_id"] or "")
                marketplace_id = str(o.get("marketplace_id") or "")
                country = o.get("geo") or ""
                channel = o.get("channel") or ""
                fulfillment_channel = o.get("fulfillment_channel") or ""
                order_status = o.get("order_status") or ""
                order_total = o.get("order_total")
                currency = o.get("currency")

                for iid in o.get("order_items", []):

                    iid = str(iid)
                    item = item_map.get(iid)

                    if not item:
                        continue

                    pricing = item.get("Pricing", {})

                    item_price = float(
                        pricing.get("ItemPrice", {}).get("Amount", 0) or 0
                    )
                    item_tax = float(pricing.get("ItemTax", {}).get("Amount", 0) or 0)

                    quantity = int(
                        item.get("ProductDetails", {}).get("QuantityOrdered", 1) or 1
                    )

                    product_id = item.get("ProductDetails", {}).get("product_id")
                    product = product_map.get(str(product_id if product_id else ""), {})
                    brand_id = str(product.get("brand_id") or "")

                    product_cost = float(product.get("product_cost", 0) or 0)
                    referral_fee = float(product.get("referral_fee", 0) or 0)
                    sku = product.get("sku", "")
                    category = product.get("category", "")

                    order_date = o.get("order_date")

                    order_date_day = None
                    if order_date:
                        if isinstance(order_date, str):
                            order_date = datetime.fromisoformat(order_date)
                        order_date_day = order_date.date()

                    insert_buffer.append(
                        (
                            order_id,
                            iid,
                            purchase_order_id,
                            o.get("order_date"),
                            order_date_day,
                            order_status,
                            order_total,
                            currency,
                            marketplace_id,
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
                        insert_buffer = []
                        batch_no += 1

            print(f"[PROGRESS] orders={orders_count} rows={rows}")

            order_buffer = []

    # final flush
    flush(insert_buffer, batch_no)

    print("===================================")
    print("[DONE] MIGRATION FINISHED")
    print(f"Orders: {orders_count}")
    print(f"Rows: {rows}")
    print(f"Time: {round(time.time() - t0, 2)}s")
    print("===================================")

    return JsonResponse(
        {
            "status": "success",
            "orders": orders_count,
            "rows": rows,
            "time_sec": round(time.time() - t0, 2),
        }
    )
