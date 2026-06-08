import datetime
import json
from django.views.decorators.csrf import csrf_exempt
from pymongo import MongoClient
from django.conf import settings

client = MongoClient(settings.DATABASE_HOST)
db = client["ecommerce_db"]

temp_orders = db["tempOrder"]
temp_items = db["tempOrderItem"]

orders = db["order"]
orderitems = db["orderitems"]
products = db["product"]
marketplaces = db["marketplace"]


# -----------------------------
# HELPERS
# -----------------------------

def normalize(name):
    if not name:
        return "Unknown"
    name = name.lower()
    if "temu" in name:
        return "Temu"
    if "amazon" in name:
        return "Amazon"
    if "target" in name:
        return "Target"
    if "ebay" in name:
        return "eBay"
    return name.title()


def get_marketplace(name, geo="US"):
    name = normalize(name)

    mp = marketplaces.find_one({"name": name, "country": geo})
    if mp:
        return mp["_id"]

    return marketplaces.insert_one({
        "name": name,
        "image_url": "",
        "country": [geo]
    }).inserted_id


def get_or_create_product(item):
    sku = item.get("product_details_SKU")
    asin = item.get("product_details_ASIN")

    query = {}
    if sku:
        query["sku"] = sku
    elif asin:
        query["asin"] = asin
    else:
        query["title"] = item.get("product_title")

    product = products.find_one(query)

    if product:
        return product["_id"]

    return products.insert_one({
        "product_title": item.get("product_title"),
        "sku": sku,
        "asin": asin,
        "price": item.get("pricing_ItemPrice_Amount"),
        "currency": item.get("pricing_ItemPrice_CurrencyCode"),
        "brand": item.get("product_brand"),
        "manufacturer": item.get("product_manufacturer"),
        "created_from": "temp_migration",
        "created_at": datetime.datetime.utcnow(),
    }).inserted_id


# -----------------------------
# HARD DELETE (FIXED)
# -----------------------------

def delete_existing_order_and_items(order_doc):
    """
    HARD RESET:
    delete order + ALL related orderitems
    """

    order_id = order_doc["_id"]
    purchase_id = order_doc.get("purchase_order_id")

    print(f"🗑 Deleting existing order: {purchase_id}")

    # ✅ DELETE ALL ORDER ITEMS (FIXED LOGIC)
    orderitems.delete_many({"OrderId": purchase_id})

    # delete order
    orders.delete_one({"_id": order_id})


# -----------------------------
# MAIN API
# -----------------------------

@csrf_exempt
def migrate_temp_orders_to_orders(request):

    if request.method != "POST":
        return {"error": "POST method required"}

    try:
        body = json.loads(request.body or "{}")
        limit = body.get("limit")

        cursor = temp_orders.find()
        if limit:
            cursor = cursor.limit(int(limit))

        migrated = 0
        replaced = 0
        missing_items_report = []

        print("\n🚀 MIGRATION START\n")

        for t in cursor:

            order_key = t.get("order_id")
            purchase_id = t.get("purchase_order_id")
            merchant_id = t.get("merchant_order_id")

            print(f"\n➡ Processing: {order_key}")

            # -----------------------------
            # CHECK EXISTING ORDER
            # -----------------------------
            existing_order = orders.find_one({
                "purchase_order_id": purchase_id
            })

            if existing_order:
                # 🔥 THIS NOW DELETES BOTH ORDER + ORDERITEMS
                delete_existing_order_and_items(existing_order)
                replaced += 1

            # -----------------------------
            # MARKETPLACE
            # -----------------------------
            marketplace_id = get_marketplace(
                t.get("marketplace_name") or t.get("Type"),
                t.get("Geo", "US")
            )

            # -----------------------------
            # GET TEMP ITEMS
            # -----------------------------
            items = list(temp_items.find({
                "order_id": order_key
            }))

            order_item_ids = []
            missing_items = []

            # -----------------------------
            # CREATE ORDER ITEMS
            # -----------------------------
            for it in items:
                try:
                    product_id = get_or_create_product(it)

                    inserted = orderitems.insert_one({
                        "OrderId": purchase_id,
                        "product_id": product_id,
                        "ProductDetails": {
                            "Title": it.get("product_title"),
                            "SKU": it.get("product_details_SKU"),
                            "ASIN": it.get("product_details_ASIN"),
                            "QuantityOrdered": it.get("product_details_QuantityOrdered"),
                            "QuantityShipped": it.get("product_details_QuantityShipped"),
                        },
                        "Pricing": {
                            "ItemPrice": {
                                "CurrencyCode": it.get("pricing_ItemPrice_CurrencyCode"),
                                "Amount": it.get("pricing_ItemPrice_Amount"),
                            }
                        },
                        "marketplace_id": marketplace_id,
                        "created_date": it.get("created_date"),
                        "updated_at": it.get("updated_at"),
                    })

                    order_item_ids.append(inserted.inserted_id)

                except Exception as e:
                    missing_items.append({
                        "temp_item_id": str(it.get("_id")),
                        "error": str(e)
                    })

            # -----------------------------
            # CREATE ORDER
            # -----------------------------
            orders.insert_one({
                "purchase_order_id": purchase_id,
                "merchant_order_id": merchant_id,
                "order_key": order_key,

                "geo": t.get("Geo"),
                "channel": normalize(t.get("Type") or t.get("marketplace_name")),

                "order_date": t.get("order_date"),
                "last_update_date": t.get("last_update_date"),
                "order_status": t.get("order_status"),

                "shipping_info_City": t.get("shipping_info_City"),
                "shipping_info_StateOrRegion": t.get("shipping_info_StateOrRegion"),
                "shipping_info_PostalCode": t.get("shipping_info_PostalCode"),
                "shipping_info_CountryCode": t.get("shipping_info_CountryCode"),

                "shipping_cost": t.get("shipping_cost", 0),
                "order_total": t.get("order_total", 0),
                "currency": t.get("currency"),
                "items_order_quantity": t.get("items_order_quantity", 0),

                "marketplace_id": marketplace_id,
                "fulfillment_channel": t.get("fulfillment_channel"),

                "order_items": order_item_ids,
                "created_at": datetime.datetime.utcnow(),
            })

            migrated += 1

            print(f"✅ Done: {order_key}")

        return {
            "status": "success",
            "migrated": migrated,
            "replaced": replaced,
            "missing_items": missing_items_report
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }