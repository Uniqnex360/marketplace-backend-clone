from omnisight.operations.helium_utils import convertLocalTimeToUTC
from bson import ObjectId
from omnisight.models import *

def get_order_count(start_date, end_date, marketplace_id=None, timezone_str='UTC'):
        if timezone_str != 'UTC':
            start_date, end_date = convertLocalTimeToUTC(start_date, end_date, timezone_str)

        match_conditions = {
            "order_date": {"$gte": start_date, "$lte": end_date},
            "order_status": {"$ne": "Cancelled"},
            "order_total": {"$gt": 0}
        }

        if marketplace_id and marketplace_id != "all":
            match_conditions["marketplace_id"] = ObjectId(marketplace_id)

        pipeline = [
            {"$match": match_conditions},
            {"$group": {"_id": None, "count": {"$sum": 1}}}
        ]

        order_count_result = list(Order.objects.aggregate(*pipeline))
        order_count = order_count_result[0].get("count", 0) if order_count_result else 0

        return order_count