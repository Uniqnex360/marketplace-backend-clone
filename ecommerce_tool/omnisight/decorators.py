import hashlib
import json
import functools
from urllib import response
from django.core.cache import cache
from django.http import JsonResponse, HttpResponse
from django.core.serializers.json import DjangoJSONEncoder

def redis_cache(timeout=900, key_prefix="cache", include_args=True, include_kwargs=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = _generate_cache_key(func, key_prefix, args if include_args else (), kwargs if include_kwargs else {})
            cached_payload = cache.get(cache_key)
            if cached_payload is not None:
                response_type=cached_payload.get("__response_type__")
                data=cached_payload.get("__data__") 
                if response_type=="json":
                    return JsonResponse(data,safe=False)
                elif response_type=='http':
                    return HttpResponse(data)
                elif response_type=='drf':
                    return JsonResponse(data,safe=False)
                elif response_type=='raw':
                    return data
                else:
                    return data
            result = func(*args, **kwargs)
            response_type = None
            cacheable_data = None
            
            if isinstance(result, JsonResponse):
                response_type = 'json'
                try:
                    cacheable_data = json.loads(result.content.decode('utf-8'))
                except Exception:
                    pass
            elif isinstance(result, HttpResponse):
                response_type = 'http'
                try:
                    content = result.content.decode('utf-8')
                    content_type = result.get('Content-Type', '')
                    if 'application/json' in content_type:
                        cacheable_data = json.loads(content)
                        response_type = 'json'
                    else:
                        cacheable_data = content
                except Exception:
                    pass
            elif hasattr(result, 'data'):  
                response_type = 'drf'
                try:
                    cacheable_data = result.data
                except Exception:
                    pass
            elif isinstance(result, (dict, list, str, int, float, bool, type(None))):
                response_type = 'raw'
                cacheable_data = result
            else:
                response_type = 'repr'
                try:
                    cacheable_data = repr(result)
                except Exception:
                    pass
            if cacheable_data is not None:
                try:
                    cache_payload = {
                        '__response_type__': response_type,
                        '__data__': cacheable_data,
                    }
                    cache.set(cache_key, cache_payload, timeout)
                except Exception:
                    pass 
            return result
        return wrapper
    return decorator


def _generate_cache_key(func, prefix, args, kwargs):
    def serialize_arg(arg):
        if hasattr(arg, '__dict__'):
            if hasattr(arg, 'body') and hasattr(arg, 'GET') and hasattr(arg, 'POST'):
                try:
                    body = arg.body.decode('utf-8') if arg.body else '{}'
                    parsed_body = json.loads(body) if body.strip() else {}
                    return {
                        'GET': dict(arg.GET),
                        'POST': dict(arg.POST),
                        'body': parsed_body,
                        'path': arg.path,
                        'method': arg.method,
                    }
                except Exception:
                    return repr(arg)
            else:
                return repr(arg)
        elif callable(arg):
            return f"<callable {getattr(arg, '__name__', 'unknown')}>"
        else:
            return arg
    serialized_args = [serialize_arg(arg) for arg in args]
    serialized_kwargs = {k: serialize_arg(v) for k, v in kwargs.items()}
    key_dict = {
        'args': serialized_args,
        'kwargs': serialized_kwargs
    }
    try:
        key_string = json.dumps(key_dict, sort_keys=True, cls=DjangoJSONEncoder, default=str)
    except Exception:
        key_string = repr(key_dict)
    hash_digest = hashlib.md5(key_string.encode('utf-8')).hexdigest()
    
    return f"{prefix}:{func.__module__}.{func.__name__}:{hash_digest}"