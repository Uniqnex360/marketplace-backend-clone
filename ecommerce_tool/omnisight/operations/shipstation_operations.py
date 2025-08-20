# import base64
# import requests
# import os

# class ShipStationClient:
#     API_BASE_URL = "https://ssapi.shipstation.com"

#     def __init__(self):
#         self.api_key = os.environ.get('SHIPSTATION_API_KEY')
#         self.api_secret = os.environ.get('SHIPSTATION_API_SECRET')
#         print(os.environ.get('SHIPSTATION_API_KEY'))
#         if not self.api_key or not self.api_secret:
#             raise ValueError("ShipStation API credentials not found in environment variables")

#         credentials = f"{self.api_key}:{self.api_secret}"
#         encoded_credentials = base64.b64encode(credentials.encode()).decode()
#         self.auth_header = {
#             "Authorization": f"Basic {encoded_credentials}",
#             "Content-Type": "application/json"
#         }
#     def list_orders(self, **params):
#         url = f"{self.API_BASE_URL}/orders"
#         if 'orderNumber' in params and params['orderNumber']:
#             params['orderNumber'] = params['orderNumber'].strip()
#             print(f"[ShipStationClient] Requesting: {url} with params: {params}")
#         try:
#             response = requests.get(url, headers=self.auth_header, params=params)
#             print(f"[ShipStationClient] Response status: {response.status_code}")
#             print(f"[ShipStationClient] Response text: {response.text}")  # Print first 500 chars
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"[ShipStationClient] Error listing orders: {e}")
#             if response is not None:
#                 print(f"[ShipStationClient] Response content: {response.text}")
#         return {"error": str(e)}
#     def create_order(self, order_data):
#         url = f"{self.API_BASE_URL}/orders/createorder"
#         try:
#             response = requests.post(url, headers=self.auth_header, json=order_data)
#             print(order_data)
#             if response.status_code != 200:
#                 try:
#                     error_json = response.json()
#                 except:
#                     print(f"[ShipStationClient] Response text: {response.text}")
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"[ShipStationClient] Error creating order: {e}")
#             return {"error": str(e)}

#     def get_order(self, order_id):
#         url = f"{self.API_BASE_URL}/orders/{order_id}"
#         try:
#             response = requests.get(url, headers=self.auth_header)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"[ShipStationClient] Error fetching order {order_id}: {e}")
#             return {"error": str(e)}

#     def get_rates(self, rate_request):
#         url = f"{self.API_BASE_URL}/shipments/getrates"
#         try:
#             response = requests.post(url, headers=self.auth_header, json=rate_request)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"[ShipStationClient] Error getting rates: {e}")
#             return {"error": str(e)}

#     def create_label(self, label_request):
#         url = f"{self.API_BASE_URL}/orders/createlabelfororder"
#         try:
#             response = requests.post(url, headers=self.auth_header, json=label_request)
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"[ShipStationClient] Error creating label: {e}")
#             return {"error": str(e)}
