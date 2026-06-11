from .test import urlpatterns as test
from .temu_import import urlpatterns as temu_import
from .order_list import urlpatterns as order_list

urlpatterns = test + temu_import + order_list
