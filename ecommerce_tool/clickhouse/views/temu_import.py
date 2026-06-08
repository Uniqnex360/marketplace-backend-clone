from clickhouse.helpers import migrate_temp_orders_to_orders
from django.views.decorators.csrf import csrf_exempt



@csrf_exempt
def migrate_temp_collections(request):
    """health check query for clickhouse"""
    data = migrate_temp_orders_to_orders(request)
    return data
