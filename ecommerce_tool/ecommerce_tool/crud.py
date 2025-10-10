from datetime import datetime
class DatabaseModel():
    def get_document(queryset,filter={},field_list=[]):
        data = queryset(**filter).limit(1).only(*field_list)
        if len(data):
            data = data[0]
        else:
            data = None
        return data
    

    def list_documents(queryset, pipeline=None, field_list=[], sort_list=[], lower_limit=None, upper_limit=None):
        if pipeline is not None:
            # Use aggregation pipeline
            cursor = queryset.aggregate(*pipeline)
            if field_list:
            # Project only specified fields
                cursor = cursor.project(*field_list)
            if sort_list:
            # Sort by fields
                for field, direction in sort_list:
                    if direction == 1:
                        cursor = cursor.order_by(field)
                    elif direction == -1:
                        cursor = cursor.order_by(f"-{field}")
            if lower_limit is not None or upper_limit is not None:
                cursor = cursor.skip(lower_limit or 0).limit(upper_limit - lower_limit if lower_limit is not None and upper_limit is not None else None)
            return cursor
        else:
        # Fallback to filter
            data = queryset.filter(**filter).skip(lower_limit).limit(upper_limit - lower_limit if lower_limit is not None and upper_limit is not None else None).only(*field_list).order_by(*sort_list)
            return data
    
    def update_documents(queryset, filter={}, json={}):
        json['updated_at']=datetime.utcnow()
        data = queryset(**filter).update(**json)
        return bool(data)
    
    def save_documents(queryset,  json={}):
        obj = queryset(**json)
        obj.save()
        return obj

    def delete_documents(queryset,  json={}):
        queryset(**json).delete()
        return True
    def count_documents(queryset,filter={}):
        count = queryset(**filter).count()
        return count