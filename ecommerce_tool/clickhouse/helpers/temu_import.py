import datetime
import re
from bson import ObjectId
from pymongo import MongoClient
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings

client = MongoClient(settings.DATABASE_HOST)
db = client["ecommerce_db"]

temp_orders = db["tempOrder"]
temp_items = db["tempOrderItem"]

orders = db["order"]
orderitems = db["order_items"]
products = db["product"]
marketplaces = db["marketplace"]  # if exists


# -----------------------------
# NORMALIZE PLATFORM / CHANNEL
# -----------------------------
def normalize_channel(val):
    if not val:
        return None

    val = str(val).strip().lower()

    if "temu" in val:
        return "Temu"
    if "target" in val:
        return "Target"
    if "ebay" in val:
        return "eBay"
    if "amazon" in val:
        return "Amazon"

    return val.capitalize()


# -----------------------------
# GEO CLEAN
# -----------------------------
def normalize_geo(val):
    if not val:
        return None

    val = str(val).strip().upper()

    if val in ["UNKNOWN", "NULL", "NONE", ""]:
        return None

    return val


# -----------------------------
# MARKETPLACE RESOLUTION
# -----------------------------
def get_marketplace_id(channel):
    if not channel:
        return None

    m = marketplaces.find_one({"name": channel})
    if m:
        return m["_id"]

    return None


# -----------------------------
# PRODUCT HANDLER (CREATE IF NOT EXISTS)
# -----------------------------
def get_or_create_product(it):
    raw = it.get("product_details_product_id")

    product_id = None
    if isinstance(raw, ObjectId):
        product_id = raw
    elif isinstance(raw, str):
        try:
            product_id = ObjectId(raw)
        except:
            product_id = None

    if product_id:
        p = products.find_one({"_id": product_id})
        if p:
            return p["_id"]

    new_product = {
        "title": it.get("product_title"),
        "brand": it.get("product_brand"),
        "manufacturer": it.get("product_manufacturer"),
        "sku": it.get("product_details_SKU"),
        "price": it.get("product_price", 0),
        "currency": it.get("product_currency"),
        "created_at": datetime.datetime.utcnow()
    }

    return products.insert_one(new_product).inserted_id


# -----------------------------
# DELETE OLD ORDER
# -----------------------------
def delete_existing(purchase_order_id):
    existing = orders.find_one({"purchase_order_id": purchase_order_id})

    if not existing:
        return

    orderitems.delete_many({"OrderId": purchase_order_id})
    orders.delete_one({"_id": existing["_id"]})


# -----------------------------
# MAIN MIGRATION
# -----------------------------
@csrf_exempt
def migrate_temp_orders_to_orders(request):

    if request.method != "POST":
        return {"error": "POST method required"}

    cursor = temp_orders.find()

    migrated = 0
    failed = []
    migrate_date = datetime.datetime.utcnow()

    for t in cursor:
        try:
            purchase_id = t.get("purchase_order_id")
            print("purchase id", purchase_id)
            delete_existing(purchase_id)

            items = list(temp_items.find({"order_id": purchase_id}))
            order_item_ids = []

            # -----------------------------
            # ORDER FIELD MAPPING (EXACT)
            # -----------------------------
            geo = normalize_geo(t.get("geo", "US"))
            channel = normalize_channel(t.get("channel") or t.get("Type"))

            marketplace_id = get_marketplace_id(channel)

            order_doc = {
                "order_id": t.get("order_id") or str(purchase_id),
                "purchase_order_id": purchase_id,
                "merchant_order_id": t.get("merchant_order_id"),

                # FIXED (no null garbage)
                "geo": geo,
                "channel": channel,
                "marketplace_id": marketplace_id,

                "shipstation_synced": t.get("shipstation_synced", False),
                "shipping_rates_fetched": t.get("shipping_rates_fetched", False),
                "shipping_cost": t.get("shipping_cost", 0),
                "merchant_shipment_cost": t.get("merchant_shipment_cost", 0),

                "order_date": t.get("order_date"),
                "pacific_date": t.get("pacific_date"),
                "last_update_date": t.get("last_update_date"),

                "shipping_information": t.get("shipping_information", {}),

                "ship_service_level": t.get("ship_service_level"),
                "order_status": t.get("order_status"),

                "number_of_items_shipped": t.get("number_of_items_shipped", 0),
                "number_of_items_unshipped": t.get("number_of_items_unshipped", 0),

                "fulfillment_channel": t.get("fulfillment_channel"),
                "sales_channel": t.get("sales_channel"),

                "order_total": t.get("order_total", 0),
                "currency": t.get("currency"),

                "items_order_quantity": t.get("items_order_quantity", 0),
                "shipping_price": t.get("shipping_price", 0),

                "order_items": [],
                "order_items_count": 0,

                "migrate_date": migrate_date
            }

            orders.insert_one(order_doc)

            # -----------------------------
            # ORDER ITEMS
            # -----------------------------
            for it in items:

                product_id = get_or_create_product(it)

                item_doc = {
                    "OrderId": purchase_id,
                    "order_item_id": it.get("order_item_id"),

                    "product_id": product_id,

                    "ProductDetails": {
                        "product_id": str(product_id),
                        "Title": it.get("product_title"),
                        "SKU": it.get("product_details_SKU"),
                        "ASIN": it.get("product_details_ASIN"),
                        "QuantityOrdered": it.get("product_details_QuantityOrdered", 1),
                        "QuantityShipped": it.get("product_details_QuantityShipped", 1),
                    },

                    "Pricing": {
                        "ItemPrice": {
                            "CurrencyCode": it.get("pricing_ItemPrice_CurrencyCode"),
                            "Amount": it.get("pricing_ItemPrice_Amount", 0),
                        },
                        "ItemTax": {
                            "CurrencyCode": it.get("pricing_ItemTax_CurrencyCode"),
                            "Amount": it.get("pricing_ItemTax_Amount", 0),
                        },
                        "PromotionDiscount": {
                            "CurrencyCode": it.get("pricing_PromotionDiscount_CurrencyCode"),
                            "Amount": it.get("pricing_PromotionDiscount_Amount", 0),
                        },
                    },

                    "product_title": it.get("product_title"),
                    "product_brand": it.get("product_brand"),
                    "product_manufacturer": it.get("product_manufacturer"),
                    "product_price": it.get("product_price"),
                    "product_currency": it.get("product_currency"),

                    "tax_checked": it.get("tax_checked", False),
                    "pricing_checked": it.get("pricing_checked", False),

                    "created_date": it.get("created_date"),
                    "updated_at": it.get("updated_at"),

                    "migrate_date": migrate_date,
                }

                inserted = orderitems.insert_one(item_doc)
                order_item_ids.append(inserted.inserted_id)

            # update order with items
            orders.update_one(
                {"purchase_order_id": purchase_id},
                {"$set": {
                    "order_items": order_item_ids,
                    "order_items_count": len(order_item_ids)
                }}
            )

            migrated += 1

        except Exception as e:
            failed.append({
                "purchase_order_id": t.get("purchase_order_id"),
                "error": str(e)
            })

    return {
        "status": "success",
        "migrated": migrated,
        "failed": failed
    }