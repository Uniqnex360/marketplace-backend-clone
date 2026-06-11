import time
import pytz

from celery import shared_task
from datetime import datetime

from omnisight.models import Order, Marketplace
from clickhouse.config import client


@shared_task
def migrate_order_list_to_clickhouse_task():
    t0 = time.time()

    ORDER_BATCH = 500
    INSERT_BATCH = 500

    print("[START] Order List Migration Started")

    print("Mongo Count:", Order._get_collection().count_documents({}))

    def normalize_datetime(value):
        """
        Convert any Mongo date/string/date-like field into UTC datetime.
        """

        if value is None:
            return None

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return pytz.UTC.localize(value)
            return value.astimezone(pytz.UTC)

        if isinstance(value, str):
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return pytz.UTC.localize(dt)
                except Exception:
                    pass

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

    print(f"[INFO] Marketplace Cache Loaded: {len(marketplace_map)}")

    mongo_cursor = (
        Order._get_collection()
        .find(
            {},
            {
                "_id": 1,
                "purchase_order_id": 1,
                "order_date": 1,
                "order_status": 1,
                "order_total": 1,
                "currency": 1,
                "items_order_quantity": 1,
                "marketplace_id": 1,
                "geo": 1,
            },
        )
        .batch_size(ORDER_BATCH)
    )

    batch = []
    total_inserted = 0
    processed = 0

    for order in mongo_cursor:
        processed += 1
        marketplace_id = str(order.get("marketplace_id", ""))

        order_date = normalize_datetime(order.get("order_date"))

        row = [
            str(order["_id"]),
            order.get("purchase_order_id", ""),
            order_date,
            order.get("order_status", ""),
            float(order.get("order_total", 0)),
            order.get("currency", "USD"),
            int(order.get("items_order_quantity", 0)),
            marketplace_id,
            marketplace_map.get(marketplace_id, ""),
            order.get("geo", ""),
        ]

        batch.append(row)

        if len(batch) >= INSERT_BATCH:

            client.insert(
                "order_list",
                batch,
                column_names=[
                    "id",
                    "purchase_order_id",
                    "order_date",
                    "order_status",
                    "order_total",
                    "currency",
                    "items_order_quantity",
                    "marketplace_id",
                    "marketplace_name",
                    "country",
                ],
            )

            total_inserted += len(batch)

            print(f"[INFO] Inserted {total_inserted} rows")

            batch = []

    if batch:
        try:
            client.insert(
                "order_list",
                batch,
                column_names=[
                    "id",
                    "purchase_order_id",
                    "order_date",
                    "order_status",
                    "order_total",
                    "currency",
                    "items_order_quantity",
                    "marketplace_id",
                    "marketplace_name",
                    "country",
                ],
            )
        except Exception as e:
            print("INSERT FAILED", e)
            print("BATCH SIZE", len(batch))

        total_inserted += len(batch)

    print("Mongo processed:", processed)
    print(
        f"[DONE] Migrated {total_inserted} orders "
        f"in {round(time.time() - t0, 2)} sec"
    )

    return {
        "success": True,
        "inserted": total_inserted,
    }
