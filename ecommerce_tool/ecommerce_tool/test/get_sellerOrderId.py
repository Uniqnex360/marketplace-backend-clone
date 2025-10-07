# import requests
# import json
# from datetime import datetime, timedelta
# from ecommerce_tool.settings import AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN, SELLER_ID
# from ecommerce_tool.crud import DatabaseModel
# from mongoengine.queryset.visitor import Q
# from omnisight.models import access_token, Marketplace, Order
# from bson import ObjectId

# def refresh_access_token(client_id, client_secret, refresh_token):
#     """
#     Refresh the access token using the refresh token.

#     :param client_id: The LWA client ID.
#     :param client_secret: The LWA client secret.
#     :param refresh_token: The refresh token.
#     :return: The new access token.
#     """
#     url = "https://api.amazon.com/auth/o2/token"
#     payload = {
#         "grant_type": "refresh_token",
#         "refresh_token": refresh_token,
#         "client_id": client_id,
#         "client_secret": client_secret
#     }

#     response = requests.post(url, data=payload)

#     if response.status_code == 200:
#         return response.json().get("access_token")
#     else:
#         raise Exception(f"Failed to refresh access token: {response.status_code} - {response.text}")

# def get_access_token():
#     """
#     Retrieve access token using refresh token.
#     """
#     payload = {
#         "grant_type": "refresh_token",
#         "refresh_token": REFRESH_TOKEN,
#         "client_id": AMAZON_API_KEY,
#         "client_secret": AMAZON_SECRET_KEY
#     }

#     headers = {"Content-Type": "application/x-www-form-urlencoded"}
#     response = requests.post("https://api.amazon.com/auth/o2/token", data=payload, headers=headers)

#     if response.status_code == 200:
#         return response.json().get("access_token")
#     else:
#         print("Error getting access token:", response.text)
#         return None

# def getAccesstoken(user_id):
#     marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ['id']).id
#     exist_access_token_obj = DatabaseModel.get_document(access_token.objects, {"user_id": user_id, "marketplace_id": marketplace_id}, ['access_token_str', 'updation_time'])
#     if exist_access_token_obj is not None:
#         # Get the current time
#         current_time = datetime.now()

#         # Get the creation time of the access token
#         creation_time = exist_access_token_obj.updation_time

#         # Check if the current time is greater than the creation time plus 59 minutes
#         if current_time < creation_time + timedelta(minutes=59):
#             access_token_str = exist_access_token_obj.access_token_str
#         else:
#             access_token_str = get_access_token()
#             if access_token_str is not None:
#                 DatabaseModel.update_documents(access_token.objects, {"id": exist_access_token_obj.id}, {"access_token_str": access_token_str, "updation_time": datetime.now()})
#     else:
#         access_token_str = get_access_token()
#         DatabaseModel.save_documents(access_token, {"user_id": ObjectId(user_id), "access_token_str": access_token_str, "marketplace_id": marketplace_id})
#     return access_token_str

# def get_seller_order_id_by_purchase_order_id(access_token, purchase_order_id):
#     """
#     Retrieve the sellerOrderId for a given purchase_order_id (AmazonOrderId).

#     :param access_token: The access token for the Amazon Seller API.
#     :param purchase_order_id: The AmazonOrderId for which to retrieve the sellerOrderId.
#     :return: The sellerOrderId for the given purchase_order_id, or None if not found.
#     """
#     # Define the URL and headers for the API request
#     url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{purchase_order_id}"
#     headers = {
#         "x-amz-access-token": access_token,
#         "Content-Type": "application/json"
#     }

#     # Make the API request to retrieve the order details
#     response = requests.get(url, headers=headers)

#     # Check if the request was successful
#     if response.status_code == 200:
#         order_details = response.json().get("payload", {})
#         fulfillment_channel = order_details.get("FulfillmentChannel")
#         seller_order_id = order_details.get("SellerOrderId")

#         if fulfillment_channel == "MFN":
#             print(f"Seller Order ID for Amazon Order ID {purchase_order_id}: {seller_order_id}")
#             return seller_order_id
#         else:
#             print(f"Order {purchase_order_id} is not an MFN order.")
#             return None
#     else:
#         print(f"Failed to retrieve order details: {response.status_code} - {response.text}")
#         return None

# def update_missing_merchant_order_ids(access_token):
#     """
#     Update the Order collection with missing merchant_order_ids for MFN orders.

#     :param access_token: The access token for the Amazon Seller API.
#     """
#     # Retrieve all orders where merchant_order_id is missing and order is not canceled
#     orders = Order.objects(
#         (Q(merchant_order_id=None) | Q(merchant_order_id="")) &  # Merchant order ID is missing
#         Q(fulfillment_channel="MFN") &  # Only MFN orders
#         Q(order_status__nin=["Canceled", "Cancelled"])  # Exclude canceled orders
#     )

#     for order in orders:
#         purchase_order_id = order.purchase_order_id
#         seller_order_id = get_seller_order_id_by_purchase_order_id(access_token, purchase_order_id)

#         if seller_order_id:
#             order.update(merchant_order_id=seller_order_id)
#             print(f"Updated order {purchase_order_id} with merchant_order_id: {seller_order_id}")
#         else:
#             print(f"Failed to retrieve sellerOrderId for order {purchase_order_id}")

# if __name__ == "__main__":
#     client_id = AMAZON_API_KEY
#     client_secret = AMAZON_SECRET_KEY
#     refresh_token = REFRESH_TOKEN
#     user_id = "your_user_id"  # Replace with your actual user ID

#     # Refresh the access token
#     access_token = refresh_access_token(client_id, client_secret, refresh_token)

#     # Update missing merchant_order_ids for MFN orders
#     update_missing_merchant_order_ids(access_token)