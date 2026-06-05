from .test import urlpatterns as test
from .temu_import import urlpatterns as temu_import

urlpatterns = test + temu_import
