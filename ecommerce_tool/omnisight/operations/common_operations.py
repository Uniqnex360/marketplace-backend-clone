from django.shortcuts import render
from omnisight.models import *
from django.http import JsonResponse,HttpResponse
from ecommerce_tool.custom_mideleware import SIMPLE_JWT, createJsonResponse, createCookies,send_email
from rest_framework.decorators import api_view
from django.middleware import csrf
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
import jwt
from bson import ObjectId
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from ecommerce_tool.settings import SENDGRID_API_KEY
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import random
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from io import BytesIO
import base64
from datetime import datetime, timedelta

   
def checkEmailExistOrNot(request):
    email = request.GET.get('email')
    data = dict()
    pipeline = [
        {
            "$match" : {
                "email" : email.lower(),
                }
        },
        {
            "$limit" : 1
        },
        {
           "$project" :{
                "_id": 1
           }
        }
    ]
    email_obj = list(user.objects.aggregate(*(pipeline)))
    if email_obj != []:
        data['is_exist'] = True
    else:
        data['is_exist'] = False
    return data


@csrf_exempt
def signupUser(request):
    data = dict()
    if request.method == 'POST':
        json_req = JSONParser().parse(request)
        otp = random.randint(100000, 999999)
        client_role_id = DatabaseModel.get_document(role.objects,{"name" : "client"},['id']).id
        user_obj = {
            "first_name" : json_req['first_name'],
            "last_name" : json_req['last_name'],
            "email":json_req['email'].lower(),
            "mobile_number":json_req['mobile_number'],
            "password" : json_req['email'].lower(),
            "role_id" : client_role_id,
            "otp" : otp,
}
        new_user_obj = DatabaseModel.save_documents(user,user_obj)
        name = f"{json_req['first_name']} {json_req['last_name']}"

        subject = "Welcome to Data Extraction - Verify Your Account"
        body = f"""Dear {name},  

                   Thank you for signing up for Data Extraction! We're excited to have you on board.  

                   To verify your account, please enter the following **verification code**:  

                   **Verification Code: {otp}**  

                   For security reasons, this code will expire in **15 minutes**.  

                   If you didn't sign up for Data Extraction or need assistance, please contact our support team.  

                   Welcome aboard!  

                   Best regards,  
                   The Data Extraction Team  
                   [https://doccrux.netlify.app/](https://doccrux.netlify.app/)
                    """
        send_email(json_req['email'].lower(), subject, body)
        data['status'] = True
        data['message'] = "verification code send to your email"
        data['user_id'] = str(new_user_obj.id)
    return JsonResponse((data), safe=False)

@csrf_exempt
def verifyOtp(request):
    data = dict()
    data['status'] = False
    if request.method == 'POST':
        json_req = JSONParser().parse(request)
        user_id = json_req['user_id']
        otp = json_req['otp']
        user_obj = DatabaseModel.get_document(user.objects,{"user_id" : user_id},['otp','otp_generated_time'])
        if user_obj != None:
            current_time = datetime.now()

            # Check if the time difference is less than 15 minutes
            time_limit = timedelta(minutes=15)

            # Check if the difference is within 15 minutes
            is_within_15_minutes = datetime.now() - current_time <= time_limit

            if otp == user_obj.otp and is_within_15_minutes == True:
                DatabaseModel.update_documents(user.objects,{"id" : user_id},{"is_verified" : True})
                data['status'] = True
                data['message'] = "User Verified Sucessfully"
            else:
                if otp == user_obj.otp and is_within_15_minutes == False:
                    data['message'] = "Verification code is expired. Please request a new one."
                else:
                    data['message'] = "Verification code is invalid. Insert the correct verification code or request a new one"
        else:
            data['message'] = "User does not exists"
    return JsonResponse((data), safe=False)


@csrf_exempt
def forgotPassword(request):
    data = dict()
    if request.method == 'POST':
        json_req = JSONParser().parse(request)
        email = json_req['email']
        otp = random.randint(100000, 999999)
        user_obj = DatabaseModel.get_document(user.objects,{"email" : email},['id','first_name','last_name'])
        if user_obj != None:
            DatabaseModel.update_documents(user.objects,{'id' : user_obj.id},{"otp" : otp, 'otp_generated_time' : datetime.now()})
            name = f"{user_obj.first_name} {user_obj.last_name}"

            subject = "Reset Your Password - Market Place"
            body = f"""Dear {name},  

                       We received a request to reset your password for your Market Place account.  

                       Your **verification code** for password reset is: **{otp}**  

                       If you did not request this change or believe it was a mistake, please ignore this email. Your password will not be changed unless you enter this code.  

                       For security reasons, this code will expire in **15 minutes**.

                       If you need further assistance, feel free to contact our support team.  

                       Best regards,  
                       The Market Place Team  
                       [https://marketplace.netlify.app/](https://marketplace.netlify.app/) 
                        """
            send_email(json_req['email'].lower(), subject, body)
            data['status'] = True
            data['message'] = "otp send to your email"
            data['user_id'] = str(user_obj.id)
        else:
            data['status'] = False
            data['message'] = "The email address you entered is not found in our records."
            data['user_id'] = ""
    return JsonResponse((data), safe=False)

@csrf_exempt
def changePassword(request):
    data = dict()
    data['status'] = False
    if request.method == 'POST':
        json_req = JSONParser().parse(request)
        user_id = json_req['user_id']
        otp = json_req['otp']
        password = json_req['password']
        user_obj = DatabaseModel.get_document(user.objects,{"id" : user_id},['otp','otp_generated_time'])
        if user_obj != None:
            current_time = datetime.now()

            # Check if the time difference is less than 15 minutes
            time_limit = timedelta(minutes=15)

            # Check if the difference is within 15 minutes
            is_within_15_minutes = datetime.now() - current_time <= time_limit

            if otp == user_obj.otp and is_within_15_minutes == True:
                DatabaseModel.update_documents(user.objects,{"id" : user_id},{"password" : password})
                data['status'] = True
                data['message'] = "Password Changed Sucessfully"
            else:
                if otp == user_obj.otp and is_within_15_minutes == False:
                    data['message'] = "OTP is expired. Please request a new one."
                else:
                    data['message'] = "OTP is invalid. Insert the correct OTP or request a new one"
        else:
            data['message'] = "User does not exists"
    return JsonResponse((data), safe=False)


@api_view(('GET', 'POST'))
@csrf_exempt
def loginUser(request):
    jsonRequest = JSONParser().parse(request)
    user_data_obj = DatabaseModel.get_document(user.objects,jsonRequest)
    token = ''
    valid = False
    if user_data_obj:
        DatabaseModel.update_documents(user.objects,{"id" : user_data_obj.id},{'last_login' : datetime.now()})
        role_name = user_data_obj.role_id.name
        payload = {
            'id': str(user_data_obj.id),
            'name': f"{user_data_obj.first_name} {user_data_obj.last_name or ''}".strip(),
            'email': user_data_obj.email,
            'role_name': role_name,
            # 'max_age': SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        }
        token = jwt.encode(payload, SIMPLE_JWT['SIGNING_KEY'], algorithm=SIMPLE_JWT['ALGORITHM'])
        valid = True
        response = createJsonResponse(request, token)

        # response = createCookies(token, response)
        # csrf.get_token(request)
        response.data['data']['valid'] = valid
        response.data['data']['role'] = user_data_obj.role_id.name
        response.data['data'] = {
            'valid' : valid,
            'id': str(user_data_obj.id),
            'name': f"{user_data_obj.first_name} {user_data_obj.last_name or ''}".strip(),
            'email': user_data_obj.email,
            'role_name': role_name,
            # 'max_age': SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        }
    else:
        response = createJsonResponse(request, token)
        valid = False   
        response.data['data']['valid'] = valid
        response.data['data']['role'] = ""
        response.data['authentication_token'] = ''
    return response
