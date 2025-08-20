# import requests
# import uuid
# import xml.etree.ElementTree as ET  # Import XML parser
# from omnisight.operations.amazon_utils import getAccesstoken, get_access_token
# from omnisight.models import *
# import pandas as pd
# from ecommerce_tool.crud import DatabaseModel
# from bson import ObjectId
# import json
# from ecommerce_tool.settings import MARKETPLACE_ID,SELLERCLOUD_USERNAME, SELLERCLOUD_PASSWORD, Role_ARN, Acccess_Key, Secret_Access_Key,AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN, SELLER_ID
# import ast
# from datetime import datetime, timedelta
# import sys
# import math
# import requests
# import boto3
# import time
# import gzip
# import io
# import threading
# from queue import Queue
# # Time range for orders report (last 4 hours)
# from io import StringIO, BytesIO
# import sys # For environment diagnostics
# from sp_api.api import Reports
# from sp_api.base import Marketplaces, SellingApiException
# from forex_python.converter import CurrencyRates
# from ecommerce_tool.util.santize_input import sanitize_value




# def process_excel_for_amazon(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         print(f"Processing row {index + 1}...")
        
#         # Using sanitize_value for all fields
#         product = Product(
#             marketplace_id=marketplace_id,
#             sku=sanitize_value(row['seller-sku'], value_type=str),
#             product_title=sanitize_value(row['item-name'], value_type=str),
#             product_description=sanitize_value(row['item-description'], value_type=str),
#             price=sanitize_value(row['price'], value_type=float),
#             quantity=sanitize_value(row['quantity'], value_type=int),
#             open_date=pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None,
#             image_url=sanitize_value(row['image-url'], value_type=str),
#             zshop_shipping_fee=sanitize_value(row['zshop-shipping-fee'], value_type=str),
#             item_note=sanitize_value(row['item-note'], value_type=str),
#             item_condition=sanitize_value(row['item-condition'], value_type=str),
#             zshop_category=sanitize_value(row['zshop-category1'], value_type=str),
#             zshop_browse_path=sanitize_value(row['zshop-browse-path'], value_type=str),
#             zshop_storefront_feature=sanitize_value(row['zshop-storefront-feature'], value_type=str),
#             asin=sanitize_value(row['asin1'], value_type=str) or 
#                  sanitize_value(row['asin2'], value_type=str) or 
#                  sanitize_value(row['asin3'], value_type=str),
#             will_ship_internationally=bool(sanitize_value(row['will-ship-internationally'], default=False)),
#             expedited_shipping=bool(sanitize_value(row['expedited-shipping'], default=False)),
#             zshop_boldface=bool(sanitize_value(row['zshop-boldface'], default=False)),
#             product_id=sanitize_value(row['product-id'], value_type=str),
#             bid_for_featured_placement=sanitize_value(row['bid-for-featured-placement'], value_type=str),
#             add_delete=sanitize_value(row['add-delete'], value_type=str),
#             pending_quantity=0,
#             delivery_partner=sanitize_value(row['fulfillment-channel'], value_type=str),
#             published_status=sanitize_value(row['status'], value_type=str),
#         )
#         product.save()
        
# file_path2 = "/home/lexicon/Documents/newww.xlsx"
# # process_excel_for_amazon(file_path2)



# def new_process_excel_for_amazon(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         print(f"Processing row {index}...{row['asin1']}")
        
#         product_obj = DatabaseModel.get_document(
#             Product.objects,
#             {"asin": sanitize_value(row['asin1'], value_type=str)}
#         ) 
        
#         if product_obj is not None:
#             print(f"✅ Updated image for SKU: {sanitize_value(row['asin1'], value_type=str)}")

#             updates = {
#                 "product_title": sanitize_value(row['item-name'], value_type=str),
#                 "price": sanitize_value(row['price'], value_type=float),
#                 "sku": sanitize_value(row['seller-sku'], value_type=str),
#                 "product_id": sanitize_value(row['product-id'], value_type=str)
#             }
            
#             DatabaseModel.update_documents(
#                 Product.objects,
#                 {"asin": sanitize_value(row['asin1'], value_type=str)},
#                 updates
#             )
            
# file_path2 = "/home/lexicon/Documents/newww.xlsx"
# # new_process_excel_for_amazon(file_path2)

# def saveProductCategory(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     product_category_obj = list(Category.objects.aggregate(*pipeline))
#     if product_category_obj != []:
#         product_category_id = product_category_obj[0]['_id']
#     if product_category_obj == []:
#         product_category_obj = DatabaseModel.save_documents(
#             Category, {
#                 "name": name,
#                 "level": 1,
#                 "marketplace_id" : marketplace_id,
#                 "end_level" : True
#             }
#         )
#         product_category_id = product_category_obj.id
#     return product_category_id


# def saveBrand(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     brand_obj = list(Brand.objects.aggregate(*pipeline))
#     if brand_obj != []:
#         brand_id = brand_obj[0]['_id']
#     if brand_obj == []:
#         brand_obj = DatabaseModel.save_documents(
#             Brand, {
#                 "name": name,
#                 "marketplace_id" : marketplace_id,
#             }
#         )
#         brand_id = brand_obj.id
#     return brand_id


# def saveManufacturer(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     Manufacturer_obj = list(Manufacturer.objects.aggregate(*pipeline))
#     if Manufacturer_obj != []:
#         Manufacturer_id = Manufacturer_obj[0]['_id']
#     if Manufacturer_obj == []:
#         Manufacturer_obj = DatabaseModel.save_documents(
#             Manufacturer, {
#                 "name": name,
#                 "marketplace_id" : marketplace_id,
#             }
#         )
#         Manufacturer_id = Manufacturer_obj.id
#     return Manufacturer_id


# def processImage(image_list):
#     main_image = ""
#     images = []

#     main_image_candidates = [image for image in image_list if image['variant'] == "MAIN"]
#     other_image_candidates = [image for image in image_list if image['variant'] != "MAIN"]

#     def select_image(candidates):
#         for height in [1000, 500, 75]:
#             for image in candidates:
#                 if image['height'] >= height:
#                     return image['link']
#         return ""

#     main_image = select_image(main_image_candidates)
#     images = [select_image([image]) for image in other_image_candidates if select_image([image])]

#     return main_image, images




# def updateAmazonProductsBasedonAsins(request):
#     marketplace_id = DatabaseModel.get_document(
#         Marketplace.objects, 
#         {"name": "Amazon"}, 
#         ["id"]
#     ).id
    
#     access_token = "Atza|IwEBIL0fPh7bGrkBS8Bo8UyjBeoyreB9C76S8jL8LUhEMqmkPvgjfQla3z5_WMsV8Sd2P7YU36YSxhKDQ9SgAUbVLqYpFnrgCPkc6oviGpK5JfwA4u4n0qDICD-BSNzIZ9uE6lctaNSbQCLy2r2QZN7eZSL7QLKMgmBfICs6uJ3UMmjJWXuCU847r2GwbMnRAONZYM2KbnUTp1nOvURQV_vsVHTvB0hxMxd5R4qsF1_4VUZ7FBEF1uzY7qmvS1Htdo6-Ex478taZAWWKY7aA9RAKa_YuzbzTPfCWJIyaO8xtqYsI6QtIz4M3wcLr1B_3atF_FJndfnLhE8pMncMz9mpRND9F"

#     pipeline = [
#         {
#             "$match": {
#                 "marketplace_id": marketplace_id,
#                 "asin": {"$ne": ""}  # Only products with ASINs
#             }
#         },
#         {
#             "$project": {
#                 "_id": 1,
#                 "asin": 1
#             }
#         }
#     ]
    
#     product_list = list(Product.objects.aggregate(*pipeline))
    
#     for i, product_ins in enumerate(product_list):
#         print(f"Processing product {i}...")
        
#         if product_ins['asin']:
#             PRODUCTS_URL = "https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items"
#             headers = {
#                 "x-amz-access-token": access_token,
#                 "Content-Type": "application/json"
#             }

#             url = f"{PRODUCTS_URL}/{product_ins['asin']}?marketplaceIds={MARKETPLACE_ID}&includedData=attributes,images,summaries"

#             response = requests.get(url, headers=headers)

#             if response.status_code == 200:
#                 product_data = response.json()
                
#                 # Process category
#                 category_name = sanitize_value(
#                     product_data.get('summaries', [{}])[0].get('browseClassification', {}).get('displayName'),
#                     value_type=str
#                 )
#                 if category_name:
#                     saveProductCategory(marketplace_id, category_name)

#                 # Process manufacturer
#                 manufacturer_name = sanitize_value(
#                     product_data.get('summaries', [{}])[0].get('manufacturer'),
#                     value_type=str
#                 )
#                 if manufacturer_name:
#                     saveManufacturer(marketplace_id, manufacturer_name)

#                 # Process features
#                 features = [
#                     sanitize_value(feature['value'], value_type=str) 
#                     for feature in product_data.get('attributes', {}).get('bullet_point', [])
#                     if 'value' in feature
#                 ] if product_data.get('attributes', {}).get('bullet_point') else []

#                 # Process price
#                 price_data = product_data.get('attributes', {}).get('list_price', [{}])[0]
#                 price = sanitize_value(price_data.get('value'), value_type=float)
#                 currency = sanitize_value(price_data.get('currency'), default="USD", value_type=str)

#                 # Process model number
#                 model_number = sanitize_value(
#                     product_data.get('summaries', [{}])[0].get('modelNumber'),
#                     value_type=str
#                 )

#                 # Process brand
#                 brand_name = sanitize_value(
#                     product_data.get('summaries', [{}])[0].get('brand'),
#                     value_type=str
#                 )
#                 brand_id = saveBrand(marketplace_id, brand_name) if brand_name else None

#                 # Process images
#                 main_image, images = processImage(
#                     product_data.get('images', [{}])[0].get('images', [])
#                 )

#                 # Update product
#                 product = Product.objects.get(id=product_ins['_id'])
#                 updates = {
#                     "product_title": sanitize_value(
#                         product_data.get('summaries', [{}])[0].get('itemName'),
#                         value_type=str
#                     ),
#                     "category": category_name,
#                     "brand_name": brand_name,
#                     "brand_id": brand_id,
#                     "manufacturer_name": manufacturer_name,
#                     "model_number": model_number,
#                     "image_url": main_image,
#                     "image_urls": images,
#                     "features": features,
#                     "price": price,
#                     "currency": currency,
#                     "attributes": {
#                         k: v for k, v in product_data.get('attributes', {}).items() 
#                         if k != 'bullet_point' and k != 'list_price'
#                     }
#                 }
                
#                 for field, value in updates.items():
#                     setattr(product, field, value)
                
#                 product.save()
#                 print(f"✅ Updated product {product.product_title}")

# def converttime(iso_string):
#     converted_datetime = datetime.fromisoformat(iso_string[:-1] + "+00:00")
#     return converted_datetime

            
# def process_excel_for_amazon(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         print(f"Processing row {index + 1}...")
        
#         # Using sanitize_value for all fields
#         product = Product(
#             marketplace_id=marketplace_id,
#             sku=sanitize_value(row['seller-sku'], value_type=str),
#             product_title=sanitize_value(row['item-name'], value_type=str),
#             product_description=sanitize_value(row['item-description'], value_type=str),
#             price=sanitize_value(row['price'], value_type=float),
#             quantity=sanitize_value(row['quantity'], value_type=int),
#             open_date=pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None,
#             image_url=sanitize_value(row['image-url'], value_type=str),
#             zshop_shipping_fee=sanitize_value(row['zshop-shipping-fee'], value_type=str),
#             item_note=sanitize_value(row['item-note'], value_type=str),
#             item_condition=sanitize_value(row['item-condition'], value_type=str),
#             zshop_category=sanitize_value(row['zshop-category1'], value_type=str),
#             zshop_browse_path=sanitize_value(row['zshop-browse-path'], value_type=str),
#             zshop_storefront_feature=sanitize_value(row['zshop-storefront-feature'], value_type=str),
#             asin=sanitize_value(row['asin1'], value_type=str) or 
#                  sanitize_value(row['asin2'], value_type=str) or 
#                  sanitize_value(row['asin3'], value_type=str),
#             will_ship_internationally=bool(sanitize_value(row['will-ship-internationally'], default=False)),
#             expedited_shipping=bool(sanitize_value(row['expedited-shipping'], default=False)),
#             zshop_boldface=bool(sanitize_value(row['zshop-boldface'], default=False)),
#             product_id=sanitize_value(row['product-id'], value_type=str),
#             bid_for_featured_placement=sanitize_value(row['bid-for-featured-placement'], value_type=str),
#             add_delete=sanitize_value(row['add-delete'], value_type=str),
#             pending_quantity=0,
#             delivery_partner=sanitize_value(row['fulfillment-channel'], value_type=str),
#             published_status=sanitize_value(row['status'], value_type=str),
#         )
#         product.save()

# def process_excel_for_amazonOrders(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         # Using sanitize_value for all fields
#         currency = "USD"
#         order_total = 0.0
#         BuyerEmail = ""
        
#         OrderTotal = ast.literal_eval(sanitize_value(row['OrderTotal'], default="{}"))
#         customer_email_id = ast.literal_eval(sanitize_value(row['BuyerInfo'], default="{}"))
        
#         if customer_email_id != {}:
#             BuyerEmail = sanitize_value(customer_email_id.get('BuyerEmail'), value_type=str)

#         if OrderTotal != {}:
#             currency = sanitize_value(OrderTotal.get('CurrencyCode'), default="USD", value_type=str)
#             order_total = sanitize_value(OrderTotal.get('Amount'), value_type=float)

#         order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": sanitize_value(row['AmazonOrderId'], value_type=str)})
        
#         if order_obj:
#             print(f"Order with purchase order ID {sanitize_value(row['AmazonOrderId'], value_type=str)} already exists. Skipping...")
#         else:
#             order = Order(
#                 marketplace_id=marketplace_id,
#                 customer_email_id=BuyerEmail,
#                 purchase_order_id=sanitize_value(row['AmazonOrderId'], value_type=str),
#                 earliest_ship_date=converttime(row['EarliestShipDate']) if pd.notnull(row['EarliestShipDate']) else None,
#                 sales_channel=sanitize_value(row['SalesChannel'], value_type=str),
#                 number_of_items_shipped=sanitize_value(row['NumberOfItemsShipped'], value_type=int),
#                 order_type=sanitize_value(row['OrderType'], value_type=str),
#                 is_premium_order=sanitize_value(row['IsPremiumOrder'], value_type=bool),
#                 is_prime=sanitize_value(row['IsPrime'], value_type=bool),
#                 fulfillment_channel=sanitize_value(row['FulfillmentChannel'], value_type=str),
#                 number_of_items_unshipped=sanitize_value(row['NumberOfItemsUnshipped'], value_type=int),
#                 has_regulated_items=sanitize_value(row['HasRegulatedItems'], value_type=bool),
#                 is_replacement_order=sanitize_value(row['IsReplacementOrder'], value_type=bool),
#                 is_sold_by_ab=sanitize_value(row['IsSoldByAB'], value_type=bool),
#                 latest_ship_date=converttime(row['LatestShipDate']) if pd.notnull(row['LatestShipDate']) else None,
#                 ship_service_level=sanitize_value(row['ShipServiceLevel'], value_type=str),
#                 order_date=converttime(row['PurchaseDate']) if pd.notnull(row['PurchaseDate']) else None,
#                 is_ispu=sanitize_value(row['IsISPU'], value_type=bool),
#                 order_status=sanitize_value(row['OrderStatus'], value_type=str),
#                 shipping_information=ast.literal_eval(sanitize_value(row['ShippingAddress'], default="{}")),
#                 is_access_point_order=sanitize_value(row['IsAccessPointOrder'], value_type=bool),
#                 seller_order_id=sanitize_value(row['SellerOrderId'], value_type=str),
#                 payment_method=sanitize_value(row['PaymentMethod'], value_type=str),
#                 is_business_order=sanitize_value(row['IsBusinessOrder'], value_type=bool),
#                 order_total=order_total,
#                 currency=currency,
#                 payment_method_details=eval(sanitize_value(row['PaymentMethodDetails'], default="[]"))[0],
#                 is_global_express_enabled=sanitize_value(row['IsGlobalExpressEnabled'], value_type=bool),
#                 last_update_date=converttime(row['LastUpdateDate']) if pd.notnull(row['LastUpdateDate']) else None,
#                 shipment_service_level_category=sanitize_value(row['ShipmentServiceLevelCategory'], value_type=str),
#                 automated_shipping_settings=ast.literal_eval(sanitize_value(row['AutomatedShippingSettings'], default="{}")),
#             )
#             order.save()

# file_path2 = "/home/lexicon/walmart/Amazonorders.xlsx"
# # process_excel_for_amazonOrders(file_path2)


# def process_amazon_order(json_data, order_date=None):
#     try:
#         product = DatabaseModel.get_document(
#             Product.objects, 
#             {"sku": sanitize_value(json_data.get("SellerSKU"), value_type=str)}, 
#             ["id"]
#         )
#         product_id = product.id if product else None
#     except:
#         product_id = None

#     order_item = OrderItems(
#         OrderId=sanitize_value(json_data.get("OrderItemId"), value_type=str),
#         Platform="Amazon",
#         created_date=order_date if order_date else datetime.now(),
#         ProductDetails=ProductDetails(
#             product_id=product_id,
#             Title=sanitize_value(json_data.get("Title"), default="Unknown Product", value_type=str),
#             SKU=sanitize_value(json_data.get("SellerSKU"), default="Unknown SKU", value_type=str),
#             ASIN=sanitize_value(json_data.get("ASIN"), default="Unknown ASIN", value_type=str),
#             QuantityOrdered=sanitize_value(json_data.get("QuantityOrdered"), value_type=int),
#             QuantityShipped=sanitize_value(json_data.get("QuantityShipped"), value_type=int),
#         ),
#         Pricing=Pricing(
#             ItemPrice=Money(
#                 CurrencyCode=sanitize_value(json_data.get("ItemPrice", {}).get("CurrencyCode"), default="USD", value_type=str),
#                 Amount=sanitize_value(json_data.get("ItemPrice", {}).get("Amount"), value_type=float)
#             ),
#             ItemTax=Money(
#                 CurrencyCode=sanitize_value(json_data.get("ItemTax", {}).get("CurrencyCode"), default="USD", value_type=str),
#                 Amount=sanitize_value(json_data.get("ItemTax", {}).get("Amount"), value_type=float)
#             ),
#             PromotionDiscount=Money(
#                 CurrencyCode=sanitize_value(json_data.get("PromotionDiscount", {}).get("CurrencyCode"), default="USD", value_type=str),
#                 Amount=sanitize_value(json_data.get("PromotionDiscount", {}).get("Amount"), value_type=float)
#             )
#         ),
#         TaxCollection=TaxCollection(
#             Model=sanitize_value(json_data.get("TaxCollection", {}).get("Model"), default="Unknown", value_type=str),
#             ResponsibleParty=sanitize_value(json_data.get("TaxCollection", {}).get("ResponsibleParty"), default="Unknown", value_type=str)
#         ),
#         IsGift=sanitize_value(json_data.get("IsGift"), default="false", value_type=str) == "true",
#         BuyerInfo=json_data.get("BuyerInfo")
#     )
#     order_item.save()
#     return order_item

# def updateOrdersItemsDetailsAmazon(request):
#     """Fetches Amazon orders, processes each item, and updates the Order collection."""
#     marketplace = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"])
#     if not marketplace:
#         print("Amazon Marketplace not found!")
#         return False

#     marketplace_id = marketplace.id
#     order_list = DatabaseModel.list_documents(Order.objects, {"marketplace_id": marketplace_id}, ["id", "order_details"])

#     for order in order_list:
#         order_items_refs = []  # Store references to OrderItems documents

#         for item in order.order_details:
#             print(item)
#             processed_item = process_amazon_order(item)
#             order_items_refs.append(processed_item)  # Append reference

#         # Update Order document with references to OrderItems
#         DatabaseModel.update_documents(Order.objects, {"id": order.id}, {"order_items": order_items_refs})

#     print("Amazon orders updated successfully!")
#     return True



# # def syncRecentAmazonOrders():
# #     access_token = get_access_token()
# #     if not access_token:
# #         return None

# #     url = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"
    
# #     # Amazon only allows retrieving orders from the last 180 days
# #     created_after = (datetime.utcnow() - timedelta(days=18000)).isoformat()

# #     headers = {
# #         "x-amz-access-token": access_token,
# #         "Content-Type": "application/json"
# #     }

# #     params = {
# #         "MarketplaceIds": MARKETPLACE_ID,
# #         "CreatedAfter": created_after,
# #         "MaxResultsPerPage": 100  # Get max orders per page
# #     }

# #     orders = []
# #     response = requests.get(url, headers=headers, params=params)

# #     if response.status_code == 200:
# #         orders_data = response.json()
# #         orders = orders_data.get("payload").get('Orders')
# #         # Check for more pages
# #         marketplace_id = DatabaseModel.get_document(Marketplace.objects,{"name" : "Amazon"},['id']).id

# #         for row in orders:
# #             order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": str(row.get('AmazonOrderId', ""))})
# #             if order_obj:
# #                 print(f"Order with purchase order ID {row.get('AmazonOrderId', '')} already exists. Skipping...")
# #             else:
# #                 order_details = list()
# #                 order_items = list()
# #                 currency = "USD"
# #                 order_total = 0.0
# #                 BuyerEmail = ""
# #                 OrderTotal = row.get('OrderTotal', {})
# #                 customer_email_id = row.get('BuyerInfo', {})
# #                 if customer_email_id:
# #                     BuyerEmail = customer_email_id.get('BuyerEmail', "")

# #                 if OrderTotal:
# #                     currency = OrderTotal.get('CurrencyCode', "USD")
# #                 order_total = OrderTotal.get('Amount', 0.0)
# #                 url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{str(row.get('AmazonOrderId', ''))}/orderItems"

# #                 # Headers
# #                 headers = {
# #                     "x-amz-access-token": access_token,
# #                     "Content-Type": "application/json"
# #                 }

# #                 response = requests.get(url, headers=headers)
# #                 order_date = converttime(row.get('PurchaseDate', "")) if row.get('PurchaseDate') else ""
# #                 if response.status_code == 200:
# #                     report_url = response.json().get("payload", {})
# #                     order_details =  report_url.get('OrderItems', [])
# #                     for order_ins in order_details:
# #                         order_items.append(process_amazon_order(order_ins,order_date))


# #                 order = Order(
# #                     marketplace_id=marketplace_id,
# #                     customer_email_id=BuyerEmail,
# #                     purchase_order_id=str(row.get('AmazonOrderId', "")),
# #                     earliest_ship_date=converttime(row.get('EarliestShipDate', "")) if row.get('EarliestShipDate') else "",
# #                     sales_channel=str(row.get('SalesChannel', "")),
# #                     number_of_items_shipped=int(row.get('NumberOfItemsShipped', 0)),
# #                     order_type=str(row.get('OrderType', "")),
# #                     is_premium_order=row.get('IsPremiumOrder', False),
# #                     is_prime=row.get('IsPrime', False),
# #                     fulfillment_channel=str(row.get('FulfillmentChannel', "")),
# #                     number_of_items_unshipped=int(row.get('NumberOfItemsUnshipped', 0)),
# #                     has_regulated_items=row.get('HasRegulatedItems', False),
# #                     is_replacement_order=row.get('IsReplacementOrder', False),
# #                     is_sold_by_ab=row.get('IsSoldByAB', False),
# #                     latest_ship_date=converttime(row.get('LatestShipDate', "")) if row.get('LatestShipDate') else "",
# #                     ship_service_level=str(row.get('ShipServiceLevel', "")),
# #                     order_date=order_date,
# #                     is_ispu=row.get('IsISPU', False),
# #                     order_status=str(row.get('OrderStatus', "")),
# #                     shipping_information=row.get('ShippingAddress', {}),
# #                     is_access_point_order=row.get('IsAccessPointOrder', False),
# #                     seller_order_id=str(row.get('SellerOrderId', "")),
# #                     payment_method=str(row.get('PaymentMethod', "")),
# #                     is_business_order=row.get('IsBusinessOrder', False),
# #                     order_total=order_total,
# #                     currency=currency,
# #                     payment_method_details=row.get('PaymentMethodDetails', [])[0] if row.get('PaymentMethodDetails') else "",
# #                     is_global_express_enabled=row.get('IsGlobalExpressEnabled', False),
# #                     last_update_date=converttime(row.get('LastUpdateDate', "")) if row.get('LastUpdateDate') else "",
# #                     shipment_service_level_category=str(row.get('ShipmentServiceLevelCategory', "")),
# #                     automated_shipping_settings=row.get('AutomatedShippingSettings', {}),
# #                     order_details =order_details,
# #                     order_items = order_items
# #                 )
# #                 order.save()
           
# #     return orders



# def ProcessAmazonProductAttributes():
#     marketplace_id = DatabaseModel.get_document(
#         Marketplace.objects, 
#         {"name": "Amazon"}, 
#         ['id']
#     ).id
    
#     pipeline = [
#         {
#             "$match": {
#                 "marketplace_id": marketplace_id,
#                 "attributes": {"$exists": True}
#             }
#         },
#         {
#             "$project": {
#                 "_id": 1,
#                 "attributes": 1
#             }
#         },
#     ]
    
#     product_list = list(Product.objects.aggregate(*pipeline))
    
#     for product_ins in product_list:
#         new_dict = {}
#         for key, value in product_ins['attributes'].items():
#             if isinstance(value, list) and len(value) == 1:
#                 single_value = value[0]
#                 filtered_value = {
#                     k: sanitize_value(v, value_type=str if isinstance(v, str) else type(v))
#                     for k, v in single_value.items() 
#                     if k not in ["language_tag", "marketplace_id"]
#                 }
#                 new_dict[key] = filtered_value
#             elif isinstance(value, list):
#                 new_dict[key] = [
#                     {
#                         k: sanitize_value(v, value_type=str if isinstance(v, str) else type(v))
#                         for k, v in item.items()
#                         if k not in ["language_tag", "marketplace_id"]
#                     }
#                     for item in value
#                 ]
#             else:
#                 new_dict[key] = sanitize_value(value)
        
#         print(f"Processed attributes for product {product_ins['_id']}: {len(new_dict)}")
        
#         DatabaseModel.update_documents(
#             Product.objects,
#             {"id": product_ins['_id']},
#             {
#                 "attributes": new_dict,
#                 "old_attributes": product_ins['attributes']
#             }
#         )

# import requests
# from requests.auth import HTTPBasicAuth  # ✅ This is the missing import
# from django.conf import settings
# # ProcessAmazonProductAttributes()
# def get_order_shipping_cost_by_order_number(order_number):
#     url = f"https://ssapi.shipstation.com/orders?orderNumber={sanitize_value(order_number, value_type=str)}"
    
#     try:
#         response = requests.get(
#             url,
#             auth=HTTPBasicAuth(
#                 sanitize_value(settings.SHIPSTATION_API_KEY, value_type=str),
#                 sanitize_value(settings.SHIPSTATION_API_SECRET, value_type=str)
#             ),
#             timeout=30
#         )

#         if response.status_code == 200:
#             data = response.json()
#             orders = data.get("orders", [])
#             if not orders:
#                 return {"error": "No order found with this order number"}
            
#             order = orders[0]
#             return {
#                 "orderId": sanitize_value(order.get("orderId"), value_type=str),
#                 "orderNumber": sanitize_value(order.get("orderNumber"), value_type=str),
#                 "shippingAmount": sanitize_value(order.get("shippingAmount", 0.0), value_type=float),
#                 "currency": sanitize_value(
#                     order.get("advancedOptions", {}).get("storeCurrencyCode", "USD"), 
#                     value_type=str
#                 )
#             }
#         else:
#             raise Exception(f"ShipStation Error: {response.status_code} - {response.text}")
#     except Exception as e:
#         raise Exception(f"Failed to fetch shipping cost: {str(e)}")
# # views.py

# from rest_framework.views import APIView
# from rest_framework.response import Response
# from rest_framework import status
# class ShipStationShippingCostAPIView(APIView):
#     def get(self, request):
#         order_number = request.query_params.get("order_number")
#         if not order_number:
#             return Response({"error": "Missing 'order_number' parameter"}, status=status.HTTP_400_BAD_REQUEST)
        
#         try:
#             cost_info = get_order_shipping_cost_by_order_number(order_number)
#             return Response(cost_info, status=status.HTTP_200_OK)
#         except Exception as e:
#             return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        






# # Sellercloud API endpoints
# TOKEN_URL = "https://onetree.api.sellercloud.us/rest/api/token"
# INVENTORY_URL = "https://onetree.api.sellercloud.us/rest/api/Inventory"

# def get_access_token():
#     payload = {
#         "username": sanitize_value(SELLERCLOUD_USERNAME, value_type=str),
#         "password": sanitize_value(SELLERCLOUD_PASSWORD, value_type=str)
#     }
#     headers = {
#         "Content-Type": "application/json"
#     }
    
#     try:
#         response = requests.post(
#             TOKEN_URL, 
#             json=payload, 
#             headers=headers,
#             timeout=30
#         )
#         response.raise_for_status()
#         return sanitize_value(response.json().get("access_token"), value_type=str)
#     except Exception as e:
#         raise Exception(f"Token fetch failed: {str(e)}")

# def fetch_all_inventory(token):
#     headers = {
#         "Authorization": f"Bearer {sanitize_value(token, value_type=str)}",
#         "Accept": "application/json"
#     }

#     params = {
#         "pageNumber": 1,
#         "pageSize": 50,
#         "sortBy": "ProductID",
#         "sortDirection": "Ascending"
#     }

#     try:
#         # First call to get total items
#         response = requests.get(
#             INVENTORY_URL, 
#             headers=headers, 
#             params=params,
#             timeout=60
#         )
#         response.raise_for_status()
        
#         data = response.json()
#         total_results = sanitize_value(data.get("TotalResults", 0), value_type=int)
#         total_pages = math.ceil(total_results / params["pageSize"])
#         all_inventory = data.get("Items", [])

#         # Fetch remaining pages
#         for page in range(2, total_pages + 1):
#             params["pageNumber"] = page
#             response = requests.get(
#                 INVENTORY_URL, 
#                 headers=headers, 
#                 params=params,
#                 timeout=60
#             )
#             if response.status_code == 200:
#                 items = sanitize_value(response.json().get("Items", []), default=[])
#                 all_inventory.extend(items)
#             else:
#                 logging.warning(f"Failed page {page}: {response.text}")

#         return all_inventory

#     except Exception as e:
#         raise Exception(f"Inventory fetch failed: {str(e)}")
    
# def sync_inventory():
#     print("🚀 Starting Sellercloud inventory sync...")

#     today = datetime.today()
#     start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
#     end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=999999)

#     try:
#         token = get_access_token()
#         inventory = fetch_all_inventory(token)

#         for ins in inventory:
#             sku = str(ins.get('ID'))
#             try:
#                 product_obj = DatabaseModel.get_document(Product.objects, {"sku": sku}, ['id'])
#                 if not product_obj:
#                     print(f"⚠️ Product not found for SKU: {sku}")
#                     continue

#                 # Check if inventory log already exists for today
#                 pipeline = [
#                     {
#                         "$match": {
#                             "product_id": product_obj.id,
#                             "date": {"$gte": start_of_day, "$lte": end_of_day}
#                         }
#                     },
#                     {"$limit": 1}
#                 ]

#                 existing_entry = list(inventry_log.objects.aggregate(*pipeline))
#                 existing_entry = existing_entry[0] if existing_entry else None

#                 if existing_entry:
#                     # Update existing log
#                     DatabaseModel.update_documents(
#                         inventry_log.objects,
#                         {"id": existing_entry['_id']},
#                         {
#                             "available": ins.get('InventoryAvailableQty'),
#                             "reserved": ins.get('ReservedQty')
#                         }
#                     )
#                     print(f"🔁 Updated inventory log for SKU {sku}")
#                 else:
#                     # Create new inventory log
#                     new_inventory_log = inventry_log(
#                         product_id=product_obj,
#                         available=ins.get('InventoryAvailableQty'),
#                         reserved=ins.get('ReservedQty'),
#                         date=datetime.now()
#                     )
#                     new_inventory_log.save()
#                     print(f"✅ Created inventory log for SKU {sku}")

#                 # Update product quantity
#                 DatabaseModel.update_documents(
#                     Product.objects,
#                     {"sku": sku},
#                     {"quantity": ins.get('InventoryAvailableQty')}
#                 )

#             except Exception as inner_err:
#                 print(f"❌ Failed to process SKU {sku}: {inner_err}")

#     except Exception as e:
#         print("🔥 Critical Error in sync_inventory:", e)

# def save_or_update_pageview_session_count(data, today_date):
#     try:
#         # Extract data from the input dictionary
#         product_id = data.get("parentAsin")
#         sales_by_asin = data.get("salesByAsin", {})
#         traffic_by_asin = data.get("trafficByAsin", {})
#         ids = DatabaseModel.list_documents(Product.objects,{"product_id" : product_id},['id'])
#         if ids:
#             ids = [ins.id for ins in ids]



#             # Check if a document with the same ASIN and today's date already exists
#             existing_record = pageview_session_count.objects(asin=product_id, date=today_date).first()

#             if existing_record:
#                 # Update the existing record
#                 existing_record.update(
#                     units_ordered=sales_by_asin.get("unitsOrdered", existing_record.units_ordered),
#                     units_ordered_b2b=sales_by_asin.get("unitsOrderedB2B", existing_record.units_ordered_b2b),
#                     ordered_product_sales_amount=sales_by_asin.get("orderedProductSales", {}).get("amount", existing_record.ordered_product_sales_amount),
#                     ordered_product_sales_currency_code=sales_by_asin.get("orderedProductSales", {}).get("currencyCode", existing_record.ordered_product_sales_currency_code),
#                     ordered_product_sales_b2b_amount=sales_by_asin.get("orderedProductSalesB2B", {}).get("amount", existing_record.ordered_product_sales_b2b_amount),
#                     ordered_product_sales_b2b_currency_code=sales_by_asin.get("orderedProductSalesB2B", {}).get("currencyCode", existing_record.ordered_product_sales_b2b_currency_code),
#                     total_order_items=sales_by_asin.get("totalOrderItems", existing_record.total_order_items),
#                     total_order_items_b2b=sales_by_asin.get("totalOrderItemsB2B", existing_record.total_order_items_b2b),
#                     browser_sessions=traffic_by_asin.get("browserSessions", existing_record.browser_sessions),
#                     browser_sessions_b2b=traffic_by_asin.get("browserSessionsB2B", existing_record.browser_sessions_b2b),
#                     mobile_app_sessions=traffic_by_asin.get("mobileAppSessions", existing_record.mobile_app_sessions),
#                     mobile_app_sessions_b2b=traffic_by_asin.get("mobileAppSessionsB2B", existing_record.mobile_app_sessions_b2b),
#                     sessions=traffic_by_asin.get("sessions", existing_record.sessions),
#                     sessions_b2b=traffic_by_asin.get("sessionsB2B", existing_record.sessions_b2b),
#                     browser_session_percentage=traffic_by_asin.get("browserSessionPercentage", existing_record.browser_session_percentage),
#                     browser_session_percentage_b2b=traffic_by_asin.get("browserSessionPercentageB2B", existing_record.browser_session_percentage_b2b),
#                     mobile_app_session_percentage=traffic_by_asin.get("mobileAppSessionPercentage", existing_record.mobile_app_session_percentage),
#                     mobile_app_session_percentage_b2b=traffic_by_asin.get("mobileAppSessionPercentageB2B", existing_record.mobile_app_session_percentage_b2b),
#                     session_percentage=traffic_by_asin.get("sessionPercentage", existing_record.session_percentage),
#                     session_percentage_b2b=traffic_by_asin.get("sessionPercentageB2B", existing_record.session_percentage_b2b),
#                     browser_page_views=traffic_by_asin.get("browserPageViews", existing_record.browser_page_views),
#                     browser_page_views_b2b=traffic_by_asin.get("browserPageViewsB2B", existing_record.browser_page_views_b2b),
#                     mobile_app_page_views=traffic_by_asin.get("mobileAppPageViews", existing_record.mobile_app_page_views),
#                     mobile_app_page_views_b2b=traffic_by_asin.get("mobileAppPageViewsB2B", existing_record.mobile_app_page_views_b2b),
#                     page_views=traffic_by_asin.get("pageViews", existing_record.page_views),
#                     page_views_b2b=traffic_by_asin.get("pageViewsB2B", existing_record.page_views_b2b),
#                     browser_page_views_percentage=traffic_by_asin.get("browserPageViewsPercentage", existing_record.browser_page_views_percentage),
#                     browser_page_views_percentage_b2b=traffic_by_asin.get("browserPageViewsPercentageB2B", existing_record.browser_page_views_percentage_b2b),
#                     mobile_app_page_views_percentage=traffic_by_asin.get("mobileAppPageViewsPercentage", existing_record.mobile_app_page_views_percentage),
#                     mobile_app_page_views_percentage_b2b=traffic_by_asin.get("mobileAppPageViewsPercentageB2B", existing_record.mobile_app_page_views_percentage_b2b),
#                     page_views_percentage=traffic_by_asin.get("pageViewsPercentage", existing_record.page_views_percentage),
#                     page_views_percentage_b2b=traffic_by_asin.get("pageViewsPercentageB2B", existing_record.page_views_percentage_b2b),
#                     buy_box_percentage=traffic_by_asin.get("buyBoxPercentage", existing_record.buy_box_percentage),
#                     buy_box_percentage_b2b=traffic_by_asin.get("buyBoxPercentageB2B", existing_record.buy_box_percentage_b2b),
#                     unit_session_percentage=traffic_by_asin.get("unitSessionPercentage", existing_record.unit_session_percentage),
#                     unit_session_percentage_b2b=traffic_by_asin.get("unitSessionPercentageB2B", existing_record.unit_session_percentage_b2b),
#                 )
#                 print("Record updated successfully!")
#             else:
#                 # Create a new record
#                 pageview_session = pageview_session_count(
#                     product_id = ids,
#                     asin=product_id,
#                     units_ordered=sales_by_asin.get("unitsOrdered", 0),
#                     units_ordered_b2b=sales_by_asin.get("unitsOrderedB2B", 0),
#                     ordered_product_sales_amount=sales_by_asin.get("orderedProductSales", {}).get("amount", 0.0),
#                     ordered_product_sales_currency_code=sales_by_asin.get("orderedProductSales", {}).get("currencyCode", "USD"),
#                     ordered_product_sales_b2b_amount=sales_by_asin.get("orderedProductSalesB2B", {}).get("amount", 0.0),
#                     ordered_product_sales_b2b_currency_code=sales_by_asin.get("orderedProductSalesB2B", {}).get("currencyCode", "USD"),
#                     total_order_items=sales_by_asin.get("totalOrderItems", 0),
#                     total_order_items_b2b=sales_by_asin.get("totalOrderItemsB2B", 0),
#                     browser_sessions=traffic_by_asin.get("browserSessions", 0),
#                     browser_sessions_b2b=traffic_by_asin.get("browserSessionsB2B", 0),
#                     mobile_app_sessions=traffic_by_asin.get("mobileAppSessions", 0),
#                     mobile_app_sessions_b2b=traffic_by_asin.get("mobileAppSessionsB2B", 0),
#                     sessions=traffic_by_asin.get("sessions", 0),
#                     sessions_b2b=traffic_by_asin.get("sessionsB2B", 0),
#                     browser_session_percentage=traffic_by_asin.get("browserSessionPercentage", 0.0),
#                     browser_session_percentage_b2b=traffic_by_asin.get("browserSessionPercentageB2B", 0.0),
#                     mobile_app_session_percentage=traffic_by_asin.get("mobileAppSessionPercentage", 0.0),
#                     mobile_app_session_percentage_b2b=traffic_by_asin.get("mobileAppSessionPercentageB2B", 0.0),
#                     session_percentage=traffic_by_asin.get("sessionPercentage", 0.0),
#                     session_percentage_b2b=traffic_by_asin.get("sessionPercentageB2B", 0.0),
#                     browser_page_views=traffic_by_asin.get("browserPageViews", 0),
#                     browser_page_views_b2b=traffic_by_asin.get("browserPageViewsB2B", 0),
#                     mobile_app_page_views=traffic_by_asin.get("mobileAppPageViews", 0),
#                     mobile_app_page_views_b2b=traffic_by_asin.get("mobileAppPageViewsB2B", 0),
#                     page_views=traffic_by_asin.get("pageViews", 0),
#                     page_views_b2b=traffic_by_asin.get("pageViewsB2B", 0),
#                     browser_page_views_percentage=traffic_by_asin.get("browserPageViewsPercentage", 0.0),
#                     browser_page_views_percentage_b2b=traffic_by_asin.get("browserPageViewsPercentageB2B", 0.0),
#                     mobile_app_page_views_percentage=traffic_by_asin.get("mobileAppPageViewsPercentage", 0.0),
#                     mobile_app_page_views_percentage_b2b=traffic_by_asin.get("mobileAppPageViewsPercentageB2B", 0.0),
#                     page_views_percentage=traffic_by_asin.get("pageViewsPercentage", 0.0),
#                     page_views_percentage_b2b=traffic_by_asin.get("pageViewsPercentageB2B", 0.0),
#                     buy_box_percentage=traffic_by_asin.get("buyBoxPercentage", 0.0),
#                     buy_box_percentage_b2b=traffic_by_asin.get("buyBoxPercentageB2B", 0.0),
#                     unit_session_percentage=traffic_by_asin.get("unitSessionPercentage", 0.0),
#                     unit_session_percentage_b2b=traffic_by_asin.get("unitSessionPercentageB2B", 0.0),
#                     date=today_date
#                 )
#                 pageview_session.save()
#                 print("New record created successfully!")
#     except Exception as e:
#         print(f"An error occurred: {e}")


# import requests
# import boto3
# import json
# import gzip
# import io
# import time
# from datetime import datetime
# from io import BytesIO, StringIO
# from django.conf import settings
# import logging

# logger = logging.getLogger(__name__)

# def fetch_sales_traffic_report(aws_access_key_id, aws_secret_access_key, role_arn, 
#                              client_id, client_secret, refresh_token, report_date, 
#                              region="us-east-1"):
#     """
#     Fetches sales and traffic report from Amazon SP-API and saves to database.
    
#     Args:
#         aws_access_key_id: AWS access key
#         aws_secret_access_key: AWS secret key  
#         role_arn: AWS role ARN for SP-API
#         client_id: LWA client ID
#         client_secret: LWA client secret
#         refresh_token: LWA refresh token
#         report_date: Date string in YYYY-MM-DD format
#         region: AWS region
        
#     Returns:
#         bool: True if successful, raises exception otherwise
#     """
    
#     # Validate and sanitize all inputs
#     try:
#         aws_access_key_id = sanitize_value(aws_access_key_id, value_type=str)
#         aws_secret_access_key = sanitize_value(aws_secret_access_key, value_type=str)
#         role_arn = sanitize_value(role_arn, value_type=str)
#         client_id = sanitize_value(client_id, value_type=str) 
#         client_secret = sanitize_value(client_secret, value_type=str)
#         refresh_token = sanitize_value(refresh_token, value_type=str)
#         report_date = sanitize_value(report_date, value_type=str)
        
#         # Validate report_date format
#         datetime.strptime(report_date, '%Y-%m-%d')
#     except ValueError as e:
#         logger.error(f"Invalid input parameter: {str(e)}")
#         raise ValueError(f"Invalid input parameter: {str(e)}")

#     try:
#         # 1. Assume AWS Role
#         sts_client = boto3.client(
#             'sts',
#             aws_access_key_id=aws_access_key_id,
#             aws_secret_access_key=aws_secret_access_key,
#             region_name=region
#         )
        
#         assumed_role = sts_client.assume_role(
#             RoleArn=role_arn,
#             RoleSessionName="SPAPISession",
#             DurationSeconds=900
#         )['Credentials']

#         # 2. Get LWA Access Token
#         token_data = {
#             "grant_type": "refresh_token",
#             "refresh_token": refresh_token,
#             "client_id": client_id,
#             "client_secret": client_secret
#         }
        
#         token_response = requests.post(
#             "https://api.amazon.com/auth/o2/token",
#             data=token_data,
#             timeout=30
#         )
#         token_response.raise_for_status()
        
#         access_token = sanitize_value(token_response.json().get('access_token'), value_type=str)
#         if not access_token:
#             raise ValueError("Failed to obtain access token")

#         # 3. Create Report Request
#         headers = {
#             "x-amz-access-token": access_token,
#             "content-type": "application/json",
#             "host": "sellingpartnerapi-na.amazon.com"
#         }

#         body = {
#             "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
#             "marketplaceIds": ["ATVPDKIKX0DER"],  # Amazon US marketplace
#             "dataStartTime": f"{report_date}T00:00:00Z",
#             "dataEndTime": f"{report_date}T23:59:59Z",
#             "reportOptions": {
#                 "dateGranularity": "DAY",
#                 "asinGranularity": "CHILD"
#             }
#         }

#         report_res = requests.post(
#             "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports",
#             headers=headers,
#             data=json.dumps(body),
#             timeout=60
#         )
#         report_res.raise_for_status()
        
#         report_id = sanitize_value(report_res.json().get("reportId"), value_type=str)
#         if not report_id:
#             raise ValueError("Failed to get report ID from response")

#         logger.info(f"Created report request with ID: {report_id}")

#         # 4. Poll Report Status
#         max_attempts = 20  # 10 minutes with 30s intervals
#         report_document_id = None
        
#         for attempt in range(max_attempts):
#             status_res = requests.get(
#                 f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}",
#                 headers=headers,
#                 timeout=30
#             )
#             status_res.raise_for_status()
            
#             status_data = status_res.json()
#             processing_status = sanitize_value(status_data.get("processingStatus"), value_type=str)
            
#             if processing_status == "DONE":
#                 report_document_id = sanitize_value(status_data.get("reportDocumentId"), value_type=str)
#                 break
#             elif processing_status in ("CANCELLED", "FATAL"):
#                 raise Exception(f"Report processing failed with status: {processing_status}")
            
#             time.sleep(30)

#         if not report_document_id:
#             raise TimeoutError("Report processing timed out")

#         # 5. Get Report Document URL
#         doc_res = requests.get(
#             f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{report_document_id}",
#             headers=headers,
#             timeout=30
#         )
#         doc_res.raise_for_status()
        
#         doc_data = doc_res.json()
#         report_url = sanitize_value(doc_data.get("url"), value_type=str)
#         compression_algo = sanitize_value(doc_data.get("compressionAlgorithm"), value_type=str)

#         # 6. Download and Process Report
#         report_response = requests.get(report_url, timeout=60)
#         report_response.raise_for_status()
        
#         if compression_algo == "GZIP":
#             with gzip.GzipFile(fileobj=BytesIO(report_response.content)) as gz_file:
#                 content = gz_file.read().decode('utf-8')
#         else:
#             content = report_response.text

#         report_data = json.loads(content)
#         report_start_date = datetime.strptime(report_date, '%Y-%m-%d')
        
#         # Process each ASIN's data
#         for asin_data in report_data.get('salesAndTrafficByAsin', []):
#             try:
#                 save_or_update_pageview_session_count(asin_data, report_start_date)
#             except Exception as e:
#                 logger.error(f"Failed to process ASIN data: {str(e)}")
#                 continue

#         return True

#     except requests.exceptions.RequestException as e:
#         logger.error(f"API request failed: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"Unexpected error in fetch_sales_traffic_report: {str(e)}")
#         raise
    
# def syncPageviews():
#     start_date = datetime.today() - timedelta(days=2)
#     report_date = start_date.strftime("%Y-%m-%d")
#     fetch_sales_traffic_report(Acccess_Key, Secret_Access_Key, Role_ARN,AMAZON_API_KEY,AMAZON_SECRET_KEY,REFRESH_TOKEN,report_date,region="us-east-1")
#     return True


# def get_and_download_report(sp_api_client, report_type, start_time, end_time):
#     TARGET_MARKETPLACE = Marketplaces.US
#     try:
#         report_request_payload = {
#             "reportType": report_type,
#             "marketplaceIds": [TARGET_MARKETPLACE.marketplace_id],
#             "dataStartTime": start_time.isoformat().split('.')[0] + 'Z',
#             "dataEndTime": end_time.isoformat().split('.')[0] + 'Z'
#         }

#         print(f"  Data time range: {report_request_payload['dataStartTime']} to {report_request_payload['dataEndTime']}")

#         # 1. Create the report request
#         try:
#             create_report_response = sp_api_client.create_report(**report_request_payload)
#             report_id = create_report_response.payload['reportId']
#             print(f"Report request submitted. Report ID: {report_id}")
#         except SellingApiException as e:
#             print(f"API error submitting report request {report_type}: {e}")
#             return None
#         except Exception as e:
#             print(f"An unexpected error during report submission: {e}")
#             return None

#         # 2. Poll for report completion
#         print("Polling report status...")
#         report_status = None
#         report_document_id = None
#         max_polls = 60
#         poll_interval = 30

#         for i in range(max_polls):
#             time.sleep(poll_interval)
#             try:
#                 get_report_response = sp_api_client.get_report(reportId=report_id)
#                 report_status = get_report_response.payload['processingStatus']

#                 print(f" Poll {i+1}/{max_polls}: Status = {report_status}")

#                 if report_status == 'DONE':
#                     report_document_id = get_report_response.payload['reportDocumentId']
#                     print("Report processing complete.")
#                     break
#                 elif report_status in ['FATAL', 'CANCELLED']:
#                     print(f"Report processing failed with status: {report_status}")
#                     return None
#             except SellingApiException as e:
#                 print(f"API error while polling report {report_id}: {e}")
#             except Exception as e:
#                 print(f"An unexpected error occurred while polling report {report_id}: {e}")

#         if report_status != 'DONE' or not report_document_id:
#             print(f"Report {report_id} did not complete successfully within the polling limit.")
#             return None

#         # 3. Download the report content (as bytes)
#         print(f"Fetching report document {report_document_id}...")
#         try:
#             report_document = sp_api_client.get_report_document(reportDocumentId=report_document_id)
#             report_content_url = report_document.payload['url']
#             response = requests.get(report_content_url, stream=True)
#             if response.status_code == 200:
#                 report_bytes = response.content
#                 print("Report content downloaded successfully.")
#                 return report_bytes
#             else:
#                 print(f"Failed to download report. Status code: {response.status_code}")
#                 return None

#         except Exception as e:
#             print(f"An unexpected error occurred during report download: {e}")
#             return None

#     except Exception as e:
#         print(f"An unexpected error occurred during the overall report process: {e}")
#         return None

# def ordersAmazon():
#     TARGET_MARKETPLACE = Marketplaces.US  # Update for your target

#     ORDERS_REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days=3)
#     print("Starting Amazon SP-API orders retrieval script for last 3 days...")

#     # Initialize the SP-API client credentials
#     credentials = {
#         'lwa_app_id': AMAZON_API_KEY,
#         'lwa_client_secret': AMAZON_SECRET_KEY,
#         'refresh_token': REFRESH_TOKEN,
#         'aws_access_key': Acccess_Key,
#         'aws_secret_key': Secret_Access_Key,
#         'role_arn': Role_ARN
#     }

#     try:
#         reports_client = Reports(
#             credentials=credentials,
#             marketplace=TARGET_MARKETPLACE
#         )
#     except Exception as e:
#         print(f"SP-API client initialization failed: {e}")
#         sys.exit(1)

#     # Get the Orders Report for the last 3 days
#     orders_content = get_and_download_report(
#         reports_client,
#         ORDERS_REPORT_TYPE,
#         start_time=start_time,
#         end_time=end_time
#     )
#     if not orders_content:
#         print("No orders content received.")
#         return None

#     # Decompress and load orders into DataFrame
#     orders_df = None
#     try:
#         try:
#             # Try decompressing (usual case, gzipped)
#             with gzip.GzipFile(fileobj=BytesIO(orders_content)) as gz:
#                 report_content_str = gz.read().decode('utf-8')
#         except OSError:
#             # Not gzipped (rare fallback)
#             report_content_str = orders_content.decode('utf-8')
#         orders_df = pd.read_csv(StringIO(report_content_str), sep='\t', low_memory=False)
#     except pd.errors.EmptyDataError:
#         print("The downloaded report was empty. No orders found in the specified time range.")
#         orders_df = None
#     except Exception as e:
#         print(f"Error reading orders report content into DataFrame: {e}")
#         orders_df = None

#     print(f"Orders DataFrame loaded: {orders_df.shape if orders_df is not None else 'None'}")
#     return orders_df


# def syncRecentAmazonOrders():
#     def safe_convert(from_currency, amount):
#         fallback_rates = {'MXN': 0.058, 'CAD': 0.73}
#         return amount * fallback_rates.get(from_currency, 1)

#     df = ordersAmazon()
#     if df is None or df.empty:
#         return False

#     marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ['id']).id

#     # Preload SKU-to-Product ID map
#     sku_to_product_id = {p.sku: p.id for p in Product.objects.only('id', 'sku') if p.sku}

#     order_items_bulk = []
#     orders_bulk = []
#     process_count = 0

#     for _, s in df.iterrows():
#         if sanitize_value(s.get('sales-channel'), value_type=str) == "Non-Amazon":
#             continue

#         process_count += 1
#         if process_count % 100 == 0:
#             print(f"Processed {process_count} rows...")

#         purchase_order_id = sanitize_value(s.get('amazon-order-id'), value_type=str)
#         order_obj = Order.objects(purchase_order_id=purchase_order_id).only('id').first()

#         if order_obj:
#             DatabaseModel.update_documents(Order.objects, {"purchase_order_id": purchase_order_id}, {
#                 "items_order_quantity": sanitize_value(s.get('quantity'), value_type=int),
#                 "shipping_price": sanitize_value(s.get('shipping-price'), value_type=float),
#                 "sales_channel": sanitize_value(s.get('sales-channel'), value_type=str),
#                 "merchant_order_id": sanitize_value(s.get('merchant-order-id'), value_type=str),
#                 "order_status": sanitize_value(s.get('order-status'), value_type=str)
#             })
#             continue

#         # Extract and convert values safely
#         sku = sanitize_value(s.get('sku'), value_type=str)
#         currency = sanitize_value(s.get('currency'), default="USD", value_type=str)

#         item_price = safe_convert(currency, sanitize_value(s.get('item-price'), value_type=float))
#         item_tax = safe_convert(currency, sanitize_value(s.get('item-tax'), value_type=float))
#         shipping_price = safe_convert(currency, sanitize_value(s.get('shipping-price'), value_type=float))
#         shipping_tax = safe_convert(currency, sanitize_value(s.get('shipping-tax'), value_type=float))
#         promo_discount = safe_convert(currency, sanitize_value(s.get('item-promotion-discount'), value_type=float))
#         promo_discount_tax = safe_convert(currency, sanitize_value(s.get('item-promotion-discount-tax'), value_type=float))

#         order_date_str = sanitize_value(s.get('purchase-date'), value_type=str)
#         order_date = (
#             datetime.strptime(order_date_str, "%Y-%m-%dT%H:%M:%S%z")
#             if pd.notna(order_date_str) else datetime.now()
#         )

#         last_update_date_str = sanitize_value(s.get('last-updated-date'), value_type=str)
#         last_update_date = (
#             datetime.strptime(last_update_date_str, "%Y-%m-%dT%H:%M:%S%z")
#             if pd.notna(last_update_date_str) else datetime.now()
#         )

#         fulfillment_channel = "MFN" if sanitize_value(s.get('fulfillment-channel'), value_type=str) == "Merchant" else "AFN"

#         ship_address = {}
#         if pd.notna(s.get('ship-city')): ship_address['City'] = sanitize_value(s.get('ship-city'), value_type=str)
#         if pd.notna(s.get('ship-state')): ship_address['StateOrRegion'] = sanitize_value(s.get('ship-state'), value_type=str)
#         if pd.notna(s.get('ship-postal-code')): ship_address['PostalCode'] = sanitize_value(s.get('ship-postal-code'), value_type=str)
#         if pd.notna(s.get('ship-country')): ship_address['CountryCode'] = sanitize_value(s.get('ship-country'), value_type=str)

#         product_id = sku_to_product_id.get(sku)

#         order_item = OrderItems(
#             OrderId=purchase_order_id,
#             Platform="Amazon",
#             created_date=order_date,
#             ProductDetails=ProductDetails(
#                 product_id=product_id,
#                 Title=sanitize_value(s.get('product-name'), value_type=str),
#                 SKU=sku,
#                 ASIN=sanitize_value(s.get('asin'), value_type=str),
#                 QuantityOrdered=sanitize_value(s.get('quantity'), value_type=int),
#                 QuantityShipped=sanitize_value(s.get('quantity'), value_type=int),
#             ),
#             Pricing=Pricing(
#                 ItemPrice=Money(CurrencyCode="USD", Amount=item_price),
#                 ItemTax=Money(CurrencyCode="USD", Amount=item_tax),
#                 PromotionDiscount=Money(CurrencyCode="USD", Amount=promo_discount)
#             )
#         )
#         order_items_bulk.append(order_item)

#         order = Order(
#             marketplace_id=marketplace_id,
#             purchase_order_id=purchase_order_id,
#             last_update_date=last_update_date,
#             sales_channel=sanitize_value(s.get('sales-channel'), value_type=str),
#             items_order_quantity=sanitize_value(s.get('quantity'), value_type=int),
#             shipping_price=shipping_price,
#             number_of_items_shipped=sanitize_value(s.get('number-of-items'), value_type=int),
#             fulfillment_channel=fulfillment_channel,
#             ship_service_level=sanitize_value(s.get('ship-service-level'), value_type=str),
#             order_date=order_date,
#             order_status=sanitize_value(s.get('order-status'), value_type=str),
#             shipping_information=ship_address,
#             merchant_order_id=sanitize_value(s.get('merchant-order-id'), value_type=str),
#             order_total=item_price + item_tax + shipping_price + shipping_tax - promo_discount - promo_discount_tax,
#             currency="USD",
#         )
#         orders_bulk.append(order)

#     # Bulk insert
#     if order_items_bulk:
#         OrderItems.objects.insert(order_items_bulk, load_bulk=False)

#     for i, order in enumerate(orders_bulk):
#         order.order_items = [order_items_bulk[i].id]
#         order.save()

#     print("Amazon orders synced successfully.")
#     return len(orders_bulk) 

# syncRecentAmazonOrders()
    

# # def FetchProductsDetails():
# #     credentials = {
# #         'lwa_app_id': AMAZON_API_KEY,
# #         'lwa_client_secret': AMAZON_SECRET_KEY,
# #         'refresh_token': REFRESH_TOKEN,
# #         'aws_access_key': Acccess_Key,
# #         'aws_secret_key': Secret_Access_Key,
# #         'role_arn': Role_ARN
# #     }
# # syncRecentAmazonOrders()
    

# def FetchProductsDetails():
#     credentials = {
#         'lwa_app_id': AMAZON_API_KEY,
#         'lwa_client_secret': AMAZON_SECRET_KEY,
#         'refresh_token': REFRESH_TOKEN,
#         'aws_access_key': Acccess_Key,
#         'aws_secret_key': Secret_Access_Key,
#         'role_arn': Role_ARN
#     }

#     report = Reports(
#         marketplace=Marketplaces.US,
#         credentials=credentials
#     ).create_report(reportType='GET_MERCHANT_LISTINGS_ALL_DATA')

#     report_id = report.payload['reportId']
#     print(f"Report created with ID: {report_id}")

#     while True:
#         response = Reports(marketplace=Marketplaces.US, credentials=credentials).get_report(report_id)
#         report_status = response.payload['processingStatus']
#         print(f"Report status: {report_status}")

#         if report_status == 'DONE':
#             break
#         elif report_status in ['CANCELLED', 'FATAL']:
#             raise Exception(f"Report {report_id} failed with status: {report_status}")

#         time.sleep(30)

#     document_id = response.payload['reportDocumentId']
#     doc = Reports(marketplace=Marketplaces.US, credentials=credentials).get_report_document(reportDocumentId=document_id)
#     download_url = doc.payload['url']

#     res = requests.get(download_url)
#     res.raise_for_status()

#     if 'compressionAlgorithm' in doc.payload and doc.payload['compressionAlgorithm'] == 'GZIP':
#         buf = BytesIO(res.content)
#         with gzip.GzipFile(fileobj=buf) as f:
#             raw_bytes = f.read()
#     else:
#         raw_bytes = res.content

#     try:
#         content = raw_bytes.decode('utf-8')
#     except UnicodeDecodeError:
#         try:
#             content = raw_bytes.decode('cp1252')
#         except UnicodeDecodeError:
#             content = raw_bytes.decode('latin1')

#     df = pd.read_csv(StringIO(content), sep='\t', low_memory=False)
#     # Print all rows line by line
#     for index, row in df.iterrows():
        
#         row_data = row.to_dict()
#         sku = ""
#         if pd.notna(row_data['seller-sku']):
#             sku = row_data['seller-sku']

#         price = 0.0
#         if pd.notna(row_data['price']):
#             price = row_data['price']

#         published_status = "Active"
#         if pd.notna(row_data['status']):
#             published_status = row_data['status']

#         product_obj = DatabaseModel.get_document(Product.objects,{"sku" : sku},['id','price'])
#         if product_obj:
#             if product_obj.price != price:
#                 DatabaseModel.update_documents(Product.objects, {"sku": sku}, {"published_status": published_status,"price": price})
#                 productPriceChange(
#                     product_id=product_obj.id,
#                     old_price=product_obj.price,
#                     new_price=price,
#                     reason="Price updated from Amazon report"
#                 ).save()

#     return True