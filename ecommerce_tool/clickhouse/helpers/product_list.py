import time
import pytz
import math

from celery import shared_task
from datetime import datetime

from omnisight.models import Product, Marketplace
from clickhouse.config import client


def safe_str(value):
    if value is None:
        return ""

    if isinstance(value, float):
        if math.isnan(value):
            return ""

    return str(value)


@shared_task
def migrate_product_list_to_clickhouse_task():

    t0 = time.time()

    PRODUCT_BATCH = 500
    INSERT_BATCH = 500

    print("[START] Product Migration Started")

    mongo_count = Product._get_collection().count_documents({})

    print("Mongo Count:", mongo_count)

    def normalize_datetime(value):

        if value is None:
            return None

        if isinstance(value, datetime):

            if value.tzinfo is None:
                return pytz.UTC.localize(value)

            return value.astimezone(pytz.UTC)

        return None

    marketplace_map = {
        str(m["_id"]): m.get("name", "")
        for m in Marketplace._get_collection().find(
            {},
            {
                "_id": 1,
                "name": 1,
            },
        )
    }

    print(f"[INFO] Marketplace Cache Loaded: " f"{len(marketplace_map)}")

    mongo_cursor = (
        Product._get_collection()
        .find(
            {},
            {
                "_id": 1,
                "product_id": 1,
                "product_title": 1,
                "sku": 1,
                "master_sku": 1,
                "parent_sku": 1,
                "asin": 1,
                "category": 1,
                "product_type": 1,
                "brand_id": 1,
                "brand_name": 1,
                "manufacturer_name": 1,
                "price": 1,
                "quantity": 1,
                "image_url": 1,
                "marketplace_ids": 1,
                "created_at": 1,
                "updated_at": 1,
            },
        )
        .batch_size(PRODUCT_BATCH)
    )

    batch = []
    total_inserted = 0
    processed = 0

    for product in mongo_cursor:

        processed += 1

        marketplace_ids = [str(mid) for mid in product.get("marketplace_ids", [])]

        marketplace_names = [marketplace_map.get(mid, "") for mid in marketplace_ids]

        row = [
            str(product["_id"]),  # Mongo _id
            safe_str(product.get("product_id")),
            safe_str(product.get("product_title", "")),
            safe_str(product.get("sku", "")),
            safe_str(product.get("master_sku", "")),
            safe_str(product.get("parent_sku", "")),
            safe_str(product.get("asin", "")),
            product.get("category", ""),
            product.get("product_type", ""),
            str(product.get("brand_id", "")),
            product.get("brand_name", ""),
            product.get("manufacturer_name", ""),
            float(product.get("price") or 0),
            int(product.get("quantity") or 0),
            product.get("image_url", ""),
            marketplace_ids,
            marketplace_names,
            normalize_datetime(product.get("created_at")),
            normalize_datetime(product.get("updated_at")),
        ]

        batch.append(row)

        if len(batch) >= INSERT_BATCH:

            client.insert(
                "product_list",
                batch,
                column_names=[
                    "id",
                    "product_id",
                    "product_title",
                    "sku",
                    "master_sku",
                    "parent_sku",
                    "asin",
                    "category",
                    "product_type",
                    "brand_id",
                    "brand_name",
                    "manufacturer_name",
                    "price",
                    "quantity",
                    "image_url",
                    "marketplace_ids",
                    "marketplace_names",
                    "created_at",
                    "updated_at",
                ],
            )

            total_inserted += len(batch)

            print(f"[INFO] Inserted " f"{total_inserted} products")

            batch = []

    if batch:

        client.insert(
            "product_list",
            batch,
            column_names=[
                "id",
                "product_id",
                "product_title",
                "sku",
                "master_sku",
                "parent_sku",
                "asin",
                "category",
                "product_type",
                "brand_id",
                "brand_name",
                "manufacturer_name",
                "price",
                "quantity",
                "image_url",
                "marketplace_ids",
                "marketplace_names",
                "created_at",
                "updated_at",
            ],
        )

        total_inserted += len(batch)
        print("current data", total_inserted)

    print("Mongo processed:", processed)

    print(
        f"[DONE] Migrated "
        f"{total_inserted} products "
        f"in {round(time.time() - t0, 2)} sec"
    )

    return {
        "success": True,
        "inserted": total_inserted,
        "processed": processed,
    }
