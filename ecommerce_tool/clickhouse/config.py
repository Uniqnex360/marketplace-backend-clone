import clickhouse_connect
from django.conf import settings

print(settings.CLICKHOUSE_HOST, "host")
client = clickhouse_connect.get_client(
    host=settings.CLICKHOUSE_HOST,
    port=int(settings.CLICKHOUSE_PORT or 8123),
    username=settings.CLICKHOUSE_USER,
    password=settings.CLICKHOUSE_PASSWORD,
    database=settings.CLICKHOUSE_DB,
)