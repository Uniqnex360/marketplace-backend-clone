from datetime import datetime, timezone
from omnisight.models  import Order

start = datetime(2025, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
end = datetime(2025, 7, 15, 23, 59, 59, tzinfo=timezone.utc)

orders = Order.objects(created_at__gte=start, created_at__lte=end)
print(f"Orders found: {orders.count()}")
for o in orders[:5]:
    print(f"{o.created_at} | {o.order_id}")
