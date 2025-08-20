# import os
# from celery import Celery

# # Set the default Django settings module for the 'celery' program.
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ecommerce_tool.settings')

# app = Celery(
#     'ecommerce_tool',
#     broker='redis://:foobaredUniqnex@localhost:6379/0',
#     backend='redis://:foobaredUniqnex@localhost:6379/0',
# )

# # Load task modules from Django settings
# app.config_from_object('django.conf:settings', namespace='CELERY')

# # Auto-discover tasks from Django apps
# app.autodiscover_tasks()

# from celery.schedules import crontab

# app.conf.beat_schedule = {
#     'sync-orders-every-20-minutes': {
#         'task': 'omnisight.tasks.sync_orders',
#         'schedule': crontab(minute='0,15,30,45'),
#     },
#     'sync-walmart_orders-every-15-minutes': {
#         'task': 'omnisight.tasks.sync_walmart_orders',
#         'schedule': crontab(minute='0,15,30,45'),
#     },
#     'sync-inventry-every-hour': {
#         'task': 'omnisight.tasks.sync_inventry',
#         'schedule': crontab(minute=10),
#     },
#     'sync-products-every-10-hours': {
#         'task': 'omnisight.tasks.sync_products',
#         'schedule': crontab(minute=0, hour='*/10'),
#     },
#     'sync-products-every-4-hours': {
#         'task': 'omnisight.tasks.sync_price',
#         'schedule': crontab(minute=0, hour='*/4'),
#     },
#     'sync-walmart-price-every-4-hours': {
#         'task': 'omnisight.tasks.sync_WalmartPrice',
#         'schedule': crontab(minute=30, hour='*/4'),
#     },
#     'backfill-merchant-shipping-cost-every-20-minutes': {
#         'task': 'omnisight.tasks.backfill_missing_shipping_cost',
#         'schedule': crontab(minute='0,20,40'),   
#         'args': (200,),
#     },
# }


# app.conf.timezone = 'UTC'
