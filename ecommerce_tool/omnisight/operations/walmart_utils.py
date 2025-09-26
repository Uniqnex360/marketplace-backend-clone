import requests
import base64
import uuid
from ecommerce_tool.settings import WALMART_API_KEY, WALMART_SECRET_KEY
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import access_token, Marketplace
from datetime import datetime, timedelta
from bson import ObjectId

def oauthFunction():
    accesstoken = None
    AUTH_URL = "https://marketplace.walmartapis.com/v3/token"

    credentials = f"{WALMART_API_KEY}:{WALMART_SECRET_KEY}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
         "Accept": "application/json",  
        "Authorization": f"Basic {encoded_credentials}",
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace"
    }

    data = "grant_type=client_credentials"  # use urlencoded string instead of dict

    response = requests.post(AUTH_URL, headers=headers, data=data)

    if response.status_code == 200:
        try:
            token_data = response.json()
            accesstoken = token_data.get("access_token")
            print("✅ Walmart API credentials are valid!")
        except Exception as e:
            print("❌ Error parsing Walmart API JSON response:", str(e))
            print("Raw Response:", response.text)
    else:
        print("❌ Authentication failed.")
        print("Status Code:", response.status_code)
        print("Response Text:", response.text)
        print("Authorization Header Used:", headers["Authorization"])

    return accesstoken

def getAccesstoken(user_id):
    marketplace_id = DatabaseModel.get_document(
        Marketplace.objects, {"name": "Walmart"}, ['id']
    ).id

    exist_access_token_obj = DatabaseModel.get_document(
        access_token.objects,
        {"user_id": user_id, "marketplace_id": marketplace_id},
        ['access_token_str', 'updation_time']
    )

    access_token_str = None

    if exist_access_token_obj:
        current_time = datetime.now()
        creation_time = exist_access_token_obj.updation_time

        if current_time < creation_time + timedelta(minutes=14):
            access_token_str = exist_access_token_obj.access_token_str
        else:
            access_token_str = oauthFunction()
            if access_token_str:
                DatabaseModel.update_documents(
                    access_token.objects,
                    {"id": exist_access_token_obj.id},
                    {
                        "access_token_str": access_token_str,
                        "updation_time": datetime.now()
                    }
                )
    else:
        access_token_str = oauthFunction()
        if access_token_str:
            DatabaseModel.save_documents(
                access_token,
                {
                    "user_id": ObjectId(user_id),
                    "access_token_str": access_token_str,
                    "marketplace_id": marketplace_id
                }
            )
        else:
            print("❌ Failed to fetch new access token.")

    return access_token_str
