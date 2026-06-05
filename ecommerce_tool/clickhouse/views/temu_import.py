from clickhouse.helpers import migrate
from django.views.decorators.csrf import csrf_exempt



@csrf_exempt
def migrate_temp_collections(request):
    """health check query for clickhouse"""
    migrate()
    return {}
