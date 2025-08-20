from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework import status
import jwt
from rest_framework.renderers import JSONRenderer
from datetime import timedelta
from django.http import HttpResponse,JsonResponse
from ecommerce_tool.settings import SENDGRID_API_KEY
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from django.conf import settings
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import ignore_api_functions, authenticated_api, user
from bson import ObjectId
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser







SIMPLE_JWT = {
  'ACCESS_TOKEN_LIFETIME': timedelta(minutes=500),
  'ALGORITHM': 'HS256',
  'SIGNING_KEY': settings.SECRET_KEY,
  'SESSION_COOKIE_DOMAIN' : '192.168.30.148',
  'SESSION_COOKIE_MAX_AGE' : 12000000,
  'AUTH_COOKIE': 'access_token',  # Cookie name. Enables cookies if value is set.
  'AUTH_COOKIE_SECURE': True,    # Whether the auth cookies should be secure (https:// only).
  'AUTH_COOKIE_SAMESITE': 'None',  # Whether to set the flag restricting cookie leaks on cross-site requests. This can be 'Lax', 'Strict', or 'None' to disable the flag.
}
import time
import logging

# class LogResponseTimeMiddleware:
#     def __init__(self, get_response):
#         self.get_response = get_response
#         self.logger = logging.getLogger(__name__)

#     def __call__(self, request):
#         start_time = time.time()

#         # Process the request and get the response
#         response = self.get_response(request)

#         end_time = time.time()
#         duration = (end_time - start_time) # in milliseconds

#         # Logging request and response time
#         method = request.method
#         path = request.get_full_path()
#         status_code = response.status_code

#         print(f"[Middleware] {method} {path} -> {status_code} in {duration:.2f} ms")

#         # Print response content (optional - for debugging only)
#         if hasattr(response, 'content'):
#             try:
#                 print(f"[Middleware] Response content: {response.content[:500]}")  # Limit for readability
#             except Exception as e:
#                 print(f"[Middleware] Could not print response content: {e}")

#         return response

def obtainManufactureIdFromToken(request): 
    token = request.COOKIES.get('authentication_token', "")
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT['manufacture_unit_id']
    except Exception as e:
        return validationObjJWT
    

def obtainUserIdFromToken(request): 
    token = request.COOKIES.get('authentication_token', "")
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT['id']
    except Exception as e:
        return validationObjJWT
    

def obtainUserRoleFromToken(request): 
    token = request.COOKIES.get('authentication_token', "")
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT['role_name']
    except Exception as e:
        return validationObjJWT



def skip_for_paths():
    """
    decorator for skipping middleware based on path
    """
    def decorator(f):
        def check_if_health(self, request):
            path = request.path.split("/")
            ignore_function = DatabaseModel.get_document(ignore_api_functions.objects,{"name__in" : path})
            if ignore_function:
                response = self.get_response(request)
                return response
            return f(self, request)
        return check_if_health
    return decorator

def createJsonResponse(request, token = None):
    authentication_token = ''
    if token:
        header,payload1,signature = str(token).split(".")
        authentication_token = header+'.'+payload1
    else:
        authentication_token = request.COOKIES.get('authentication_token', "").split(".")[0]
    data_map = dict()
    data_map['data'] = dict()
    response = Response(content_type = 'application/json') 
    response.data = data_map
    response.accepted_renderer = JSONRenderer()
    response.accepted_media_type = "application/json"
    response.renderer_context = {}
    response.data['message'] = 'success'
    response.data['status'] = True
    response.data['authentication_token'] = authentication_token
    response.status_code = 200
    return response

def createCookies(token,response):
    response.set_cookie(
        key = "authentication_token",
        value = token,
        max_age = SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        secure = SIMPLE_JWT['AUTH_COOKIE_SECURE'],
        httponly = True,
        samesite = SIMPLE_JWT['AUTH_COOKIE_SAMESITE'],
        # domain = SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )
    return response

def check_authentication(request):
    token = request.COOKIES.get('authentication_token', "")
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT
    except Exception as e:
        return validationObjJWT

def refresh_cookies(request,response):
    token = request.COOKIES.get('authentication_token', "")
    createCookies(token, response)


import json

@csrf_exempt
def checkAuthentication(request):
    path = request.path.split("/")
    user_id = None

    if request.method == "POST":
        if not request.body:
            return False

        try:
            data = json.loads(request.body.decode("utf-8"))  # Manually parse JSON
            user_id = data.get("user_id")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return False

    elif request.method == "GET":
        user_id = request.GET.get("user_id", None)
    if user_id:
        user_role = DatabaseModel.get_document(user.objects, {"id": ObjectId(user_id)}, ["role_id"]).role_id.id
        if user_role:
            has_permission = DatabaseModel.get_document(authenticated_api.objects, {
                "name__in": path,
                "allowed_roles__in": [user_role]
            })
            return bool(has_permission)

    return False





# class customMiddleware:
#     def __init__(self, get_response):
#         self.get_response = get_response

#     @skip_for_paths()
#     def __call__(self, request):
#         response = createJsonResponse(request)
#         # try:
#         #     jwtObj = check_authentication(request)
#         #     if jwtObj != None:
#         #         refresh_cookies(request, response)
                
                
#         #         is_authorised = True
#         #         if is_authorised:
#         #             res = self.get_response(request)
#         #             if isinstance(res, Response):
#         #                 response.data['data'] = res.data
#         #             else:
#         #                 response.data['data'] = res
#         #         else:
#         #             response.status_code = status.HTTP_401_UNAUTHORIZED
#         #     else:
#         #         response.status_code = status.HTTP_401_UNAUTHORIZED
#         #         response.data['message'] = 'Invalid token'
#         # except Exception as e:
#         #     print("Exception Class --", e.__class__)
#         #     print("Exception Class name --", e.__class__.__name__)
#         #     print("Exception --")
#         #     print(e)
#         #     response.data['data'] = False
#         #     if (e.__class__.__name__ == 'ExpiredSignatureError' or e.__class__.__name__ == 'DecodeError'):
#         #         response.status_code = status.HTTP_401_UNAUTHORIZED
#         #         response.data['message'] = 'Invalid token'
#         #     elif e.__class__.__name__ == 'ValidationError':
#         #         print(str(e))
#         #         print(e.message)
#         #     else:
#         #         response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
#         res = self.get_response(request)
#         if isinstance(res, HttpResponse) and not isinstance(res, JsonResponse):
#             return res
#         if isinstance(res, Response):
#             response.data['data'] = res.data
#         else:
#             response.data['data'] = res
#         response.accepted_renderer = JSONRenderer()
#         response.accepted_media_type = "application/json"
#         response.renderer_context = {}
#         response.render()
#         return response


def send_email(to_email, subject, body):
    message = Mail(
        from_email='contactdigicommerce@gmail.com',
        to_emails=to_email,
        subject=subject,
        plain_text_content=body,
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Email sent successfully! Status Code: {response.status_code}")
    except Exception as e:
        print(f"Failed to send email: {e}")



class customMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    @skip_for_paths()
    def __call__(self, request):
        response = createJsonResponse(request)
        if not checkAuthentication(request):
            response.status_code = status.HTTP_401_UNAUTHORIZED
            response.data['message'] = 'Permission denied'
            response.accepted_renderer = JSONRenderer()
            response.accepted_media_type = "application/json"
            response.renderer_context = {}
            response.render()
            return response

        res = self.get_response(request)
        if isinstance(res, HttpResponse) and not isinstance(res, JsonResponse):
            return res
        if isinstance(res, Response):
            response.data['data'] = res.data
        else:
            response.data['data'] = res
        response.accepted_renderer = JSONRenderer()
        response.accepted_media_type = "application/json"
        response.renderer_context = {}
        response.render()
        return response