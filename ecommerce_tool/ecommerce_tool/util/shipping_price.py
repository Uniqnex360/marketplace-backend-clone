# from __future__ import annotations
# import requests
# import json
# import logging
# from ecommerce_tool.settings import SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET
# import sys
# from datetime import datetime, time,timedelta
# import time as t
# import pytz
# from datetime import datetime
# import pytz
# from concurrent.futures import ThreadPoolExecutor, as_completed
# logger = logging.getLogger(__name__)

# def safe_request(url, auth, retries=3):
#     for attempt in range(retries):
#         resp = requests.get(url, auth=auth)
#         if resp.status_code == 429:
#             wait = int(resp.headers.get("Retry-After", 5))
#             t.sleep(wait)
#         else:
#             resp.raise_for_status()
#             return resp.json()
#     raise Exception(f"Failed after {retries} retries for {url}")

# def get_full_order_and_shipping_details(order_number):
#     if not order_number or order_number.strip() == "":
#         return None
#     order_url = f"https://ssapi.shipstation.com/orders?orderNumber={order_number}"
#     shipment_url = f"https://ssapi.shipstation.com/shipments?orderNumber={order_number}"
#     auth = (SHIPSTATION_API_KEY,SHIPSTATION_API_SECRET)
#     try:
#         order_data = safe_request(order_url, auth)
#         order = order_data.get("orders", [None])[0]
#         if not order:
#             logger.info(f"No order found for order number {order_number}")
#             return None
        
#         shipment_data = safe_request(shipment_url, auth)
#         shipments = shipment_data.get("shipments", [])
#         shipment_costs = []
#         for shipment in shipments:
#             shipment_costs.append({
#                 "shipmentId": shipment.get("shipmentId"),
#                 "carrierCode": shipment.get("carrierCode"),
#                 "serviceCode": shipment.get("serviceCode"),
#                 "shipmentCost": shipment.get("shipmentCost"),
#                 "shipDate": shipment.get("shipDate"),
#                 "trackingNumber": shipment.get("trackingNumber"),
#             })
#         order["shipments"] = shipment_costs
#         return order
#     except Exception as e:
#         logger.error(f"Error fetching details for order {order_number}: {e}")
#         return None
    
    
# def fetch_shipment_for_order(session, order_info):
#     order_number = order_info["orderNumber"]
#     po_id = order_info["purchaseOrderId"]
#     order_shipments = []
#     try:
#         shipment_url = f"https://ssapi.shipstation.com/shipments?orderNumber={order_number}"
#         shipment_data = safe_request(shipment_url, auth=session.auth)
#         shipments = shipment_data.get('shipments', [])
#         logger.info(f"Found {len(shipments)} shipments for order {order_number}")
#         for shipment in shipments:
#             shipment_info = {
#                 "orderNumber": order_number,
#                 "purchaseOrderId": po_id,
#                 "shipmentId": shipment.get("shipmentId"),
#                 "carrierCode": shipment.get("carrierCode"),
#                 "serviceCode": shipment.get("serviceCode"),
#                 "shipmentCost": shipment.get("shipmentCost"),
#                 "shipDate": shipment.get("shipDate"),
#                 "trackingNumber": shipment.get("trackingNumber"),
#             }
#             logger.info(f"Adding shipment info: {shipment_info}")
#             order_shipments.append(shipment_info)
#         return order_shipments
#     except Exception as e:
#         logger.error(f"Error fetching shipments for order {order_number}: {e}")
#         return []
# from datetime import datetime, timedelta, time
# import pytz

# def get_utc_range_for_order_date(order_date_utc_iso, local_tz_str='US/Pacific'):
#     local_tz = pytz.timezone(local_tz_str)
#     if isinstance(order_date_utc_iso, str):
#         utc_dt = datetime.strptime(order_date_utc_iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
#     elif isinstance(order_date_utc_iso, datetime):
#         utc_dt = order_date_utc_iso
#         if utc_dt.tzinfo is None:
#             utc_dt = utc_dt.replace(tzinfo=pytz.utc)
#     else:
#         raise ValueError("order_date_utc_iso must be a str or datetime.datetime")
#     local_dt = utc_dt.astimezone(local_tz)
#     end_date = local_dt.date()
#     start_date = end_date - timedelta(days=1)
#     local_start_dt = local_tz.localize(datetime.combine(start_date, time.min))
#     local_end_dt = local_tz.localize(datetime.combine(end_date, time.max))
#     utc_start = local_start_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
#     utc_end = local_end_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
#     logger.info(f"Querying orders between UTC {utc_start} and {utc_end} for local dates {start_date} to {end_date}")
#     return utc_start, utc_end


# def get_orders_by_customer_and_date(customer_email,order_date_utc_iso=None, purchase_order_id=None,local_tz='US/Pacific'):
#     if not purchase_order_id or purchase_order_id.strip() == "":
#         return None
#     start_date, end_date = get_utc_range_for_order_date(order_date_utc_iso, local_tz)
#     base_url = "https://ssapi.shipstation.com/orders"
#     auth = (SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)
#     session = requests.Session()
#     session.auth = auth
#     params = {
#         "customerEmail": customer_email,
#         "pageSize": 100,
#         "page": 1,
#     }
#     params["orderDateStart"] = start_date
#     params["orderDateEnd"] = end_date
#     try:
#         url_with_params = f"{base_url}?customerEmail={customer_email}&pageSize=100&page=1&orderDateStart={start_date}&orderDateEnd={end_date}"
#         data = safe_request(url_with_params, auth=session.auth)
#         total_pages = data.get("pages", 1)
#         orders_all = data.get("orders", [])
#         if total_pages > 1:
#             with ThreadPoolExecutor(max_workers=5) as executor:
#                 futures = [
#                     executor.submit(session.get, base_url, params={**params, "page": page}, timeout=30)
#                     for page in range(2, total_pages + 1)
#                 ]
#                 for future in as_completed(futures):
#                     try:
#                         response = future.result()
#                         response.raise_for_status()
#                         page_data = response.json()
#                         orders_all.extend(page_data.get("orders", []))
#                     except Exception as e:
#                         logger.error(f"Error fetching page: {e}")
#         matching_orders = []
#         for order in orders_all:
#             cust_email=order.get('customerEmail')
#             if not isinstance(cust_email, str) or cust_email.lower() != customer_email.lower():
#                 continue
#             po_id = order.get("advancedOptions", {}).get("customField2")
#             if purchase_order_id and po_id != purchase_order_id:
#                 continue
#             matching_orders.append({
#                 "orderNumber": order.get("orderNumber"),
#                 "purchaseOrderId": po_id
#             })
#         shipment_costs = []
#         if matching_orders:
#             with ThreadPoolExecutor(max_workers=10) as executor:
#                 futures = {
#                     executor.submit(fetch_shipment_for_order, session, order): order
#                     for order in matching_orders
#                 }
#                 for future in as_completed(futures):
#                     try:
#                         shipment_costs.extend(future.result())
#                     except Exception as e:
#                         logger.error(f"Error processing shipments: {e}")
#         return shipment_costs
#     except Exception as e:
#         logger.error(f"Error fetching orders for customer {customer_email}: {e}")
#         return None
#     finally:
#         session.close()
# # if __name__ == "__main__":
# #     cust_email = "sookdeoann@aol.com"
# #     order_date_utc_iso = "2025-05-27T08:48:59.017Z"  
# #     po_id = "109014864215751"
# #     shipping_info = get_orders_by_customer_and_date(
# #         customer_email=cust_email, 
# #         order_date_utc_iso=order_date_utc_iso, 
# #         purchase_order_id=po_id,
# #         local_tz='US/Pacific'  
# #     )
# #     if shipping_info:
# #         for info in shipping_info:
# #             print(f"Order {info['orderNumber']} (PO: {info['purchaseOrderId']}) - Shipping Cost: ${info['shipmentCost']}")
# #     else:
# #         print("No matching orders or shipping costs found.")
