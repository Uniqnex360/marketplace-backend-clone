from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from clickhouse.config import client
from clickhouse.common import send_error_response, send_response
from omnisight.models import OrderItems, Order


from datetime import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from clickhouse.config import client


# to take backup of a table
@csrf_exempt
def clickhouse_test_url(request):
    """Only creates a full backup table of fact_order_items"""

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table = f"fact_order_items_backup_{timestamp_str}"

    try:
        # Create backup table structure
        client.command(f"CREATE TABLE {backup_table} AS fact_order_items;")

        # Copy all data to backup table
        client.command(f"INSERT INTO {backup_table} SELECT * FROM fact_order_items;")

        return JsonResponse(
            {
                "status": True,
                "message": f"Backup created successfully as '{backup_table}'. No data modified.",
            }
        )

    except Exception as e:
        return JsonResponse({"status": False, "message": str(e)}, status=500)


@csrf_exempt
def clickhouse_test_url(request):
    """Get minimum and maximum order dates from fact_order_items."""

    try:
        result = client.query("""
            SELECT
                MIN(order_date) AS min_order_date,
                MAX(order_date) AS max_order_date
            FROM daas.fact_order_items
        """)

        min_order_date, max_order_date = result.result_rows[0]

        return JsonResponse(
            {
                "status": True,
                "min_order_date": (
                    str(min_order_date) if min_order_date is not None else None
                ),
                "max_order_date": (
                    str(max_order_date) if max_order_date is not None else None
                ),
            }
        )

    except Exception as e:
        return JsonResponse(
            {
                "status": False,
                "message": str(e),
            },
            status=500,
        )


@csrf_exempt
def sandbox_clickhouse(request):

    cursor = Order.objects.only("purchase_order_id", "merchant_order_id").limit(2)

    data = [
        {
            "purchase_order_id": obj.purchase_order_id,
            "merchant_order_id": obj.merchant_order_id,
        }
        for obj in cursor
        if obj.purchase_order_id and obj.merchant_order_id
    ]

    return send_response({"count": len(data), "data": data})


@csrf_exempt
def get_all_order_ids(request):
    """Fetch all order_ids from fact_order_items"""

    try:
        query = """
        SELECT DISTINCT order_id
        FROM fact_order_items
        WHERE order_id IS NOT NULL
        """

        result = client.query(query).result_rows

        order_ids = [row[0] for row in result] if result else []

        return send_response({"count": len(order_ids), "order_ids": order_ids})

    except Exception as e:
        return send_error_response(e)
