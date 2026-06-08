from django.views.decorators.csrf import csrf_exempt
from clickhouse.config import client
from clickhouse.common import send_error_response, send_response


@csrf_exempt
def clickhouse_test_url(request):
    """health check query for clickhouse"""

    try:
        # Simple health check query
        result = client.command("SELECT 1")
        return send_response()

    except Exception as e:
        return send_error_response(e)


from omnisight.models import OrderItems, Order


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

        return send_response({
            "count": len(order_ids),
            "order_ids": order_ids
        })

    except Exception as e:
        return send_error_response(e)