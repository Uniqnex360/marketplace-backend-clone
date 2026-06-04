from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from clickhouse.tasks import migrate_mongo_order_item_to_clickhouse_task


@csrf_exempt
def migrate_mongo_order_item_to_clickhouse(request):

    task = migrate_mongo_order_item_to_clickhouse_task.delay()

    return {"status": "started", "task_id": task.id}
