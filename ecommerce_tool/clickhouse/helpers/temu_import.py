import datetime

from pymongo import MongoClient
from bson import ObjectId
from django.conf import settings
import re

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
    if "ebay" in name:
        return "eBay"
    return name.title()


def get_marketplace(name, geo="US"):
    name = normalize(name)

    mp = marketplaces.find_one({"name": name, "country": geo})
    if mp:
        return mp["_id"]

    return marketplaces.insert_one(
        {"name": name, "image_url": "", "country": [geo]}
    ).inserted_id


def extract_objectid(val):
    if not val:
        return None

    if isinstance(val, ObjectId):
        return val

    val = str(val)
    match = re.search(r"ObjectId\('([a-f0-9]{24})'\)", val)
    if match:
        return ObjectId(match.group(1))

    if len(val) == 24:
        try:
            return ObjectId(val)
        except:
            return None

    return None


# -----------------------------
# PRODUCT UPSERT
# -----------------------------


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

    product_doc = {
        "product_title": item.get("product_title"),
        "sku": sku,
        "asin": asin,
        "price": item.get("pricing_ItemPrice_Amount"),
        "currency": item.get("pricing_ItemPrice_CurrencyCode"),
        "brand": item.get("product_brand"),
        "manufacturer": item.get("product_manufacturer"),
        "created_from": "temp_migration",
    }

    return products.insert_one(product_doc).inserted_id


# -----------------------------
# ORDER CHECK
# -----------------------------
def find_order(t):
    purchase_id = t.get("purchase_order_id")
    merchant_id = t.get("merchant_order_id")

    query = None

    if purchase_id:
        query = {"purchase_order_id": purchase_id}

    if not query and merchant_id:
        query = {"merchant_order_id": merchant_id}

    if not query:
        return None

    print("QUERY:", query)
    data = orders.find_one(query)
    print("FOUND:", data)

    return data


# -----------------------------
# MIGRATION
# -----------------------------


def migrate():
    print("\n🚀 FULL ETL MIGRATION START\n")

    for t_order in temp_orders.find():

        purchase_id = t_order.get("purchase_order_id")
        merchant_id = t_order.get("merchant_order_id")

        print(f"\n➡ Processing Order: {purchase_id}")

        if find_order(t_order):
            print("⚠ Order exists → skipping")
            continue

        # -------------------------
        # MARKETPLACE
        # -------------------------
        marketplace_id = get_marketplace(
            t_order.get("marketplace_name"), t_order.get("Geo", "US")
        )

        # -------------------------
        # ORDER ITEMS SOURCE
        # -------------------------
        items = list(temp_items.find({"order_id": purchase_id}))

        order_item_ids = []

        # -------------------------
        # PROCESS ITEMS
        # -------------------------
        for it in items:

            # PRODUCT CREATE / GET
            product_id = get_or_create_product(it)

            # ORDER ITEM DUP CHECK
            existing_item = orderitems.find_one(
                {
                    "OrderId": purchase_id,
                    "ProductDetails.SKU": it.get("product_details_SKU"),
                }
            )

            if existing_item:
                order_item_ids.append(existing_item["_id"])
                continue

            order_item_doc = {
                "OrderId": purchase_id,
                "Platform": it.get("platform"),
                "product_id": product_id,
                "ProductDetails": {
                    "product_id": product_id,
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
            }

            inserted = orderitems.insert_one(order_item_doc)
            order_item_ids.append(inserted.inserted_id)

        # -------------------------
        # ORDER CREATE
        # -------------------------
        order_doc = {
            "purchase_order_id": purchase_id,
            "merchant_order_id": merchant_id,
            "geo": t_order.get("Geo"),
            "channel": t_order.get("channel"),
            "order_date": t_order.get("order_date"),
            "order_status": t_order.get("order_status"),
            "shipping_cost": t_order.get("shipping_cost", 0),
            "order_total": t_order.get("order_total", 0),
            "currency": t_order.get("currency"),
            "marketplace_id": marketplace_id,
            "order_items": order_item_ids,
            "migrate_date": datetime.datetime.utcnow(),
        }

        orders.insert_one(order_doc)

        print(f"✅ Migrated Order: {purchase_id}")

    print("\n🎉 DONE - FULL MIGRATION COMPLETE\n")
