from __future__ import annotations
import requests
import uuid
import xml.etree.ElementTree as ET  
from omnisight.operations.walmart_utils import getAccesstoken, oauthFunction
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timedelta
import threading
import numpy as np  
import pandas as pd  
from ecommerce_tool.util.santize_input import sanitize_value
def fetchAllProducts(request):
    data = dict()
    user_id = request.GET.get('user_id')
    offset = sanitize_value(request.GET.get('skip'), default=0, value_type=int)
    limit = sanitize_value(request.GET.get('limit'), default=100, value_type=int)
    ACCESS_TOKEN = getAccesstoken(user_id)
    ALL_PRODUCTS_URL = "https://marketplace.walmartapis.com/v3/items"
    headers = {
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }
    all_products = []
    total_items = 0
    print("Fetching Products...")
    url = f"{ALL_PRODUCTS_URL}?limit={limit}&offset={offset}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        all_products.extend(data.get("ItemResponse", []))
        print(len(all_products), "products fetched")
        if total_items == 0:
            total_items = data.get("totalItems", 0)
    else:
        print(f"❌ Failed to Fetch Products. Status Code: {response.status_code}")
        print("Response:", response.text)
    data = {
        "total_items": total_items,
        "all_products": all_products
    }
    return data
def fetchProductDetails(request=None):
    ACCESS_TOKEN = oauthFunction()  
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Walmart"}, ['id']).id
    pipeline = [
        {"$match": {
            "marketplace_id": marketplace_id
        }},
        {
            "$project": {
                "_id": 1,
                "sku": {"$ifNull": ["$sku", ""]}
            }
        }
    ]
    product_list = list(Product.objects.aggregate(*pipeline))
    for product_ins in product_list:
        sku = product_ins['sku']
        update_obj = {}
        if sku != "":
            PRODUCT_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/items/{sku}?include=images,attributes,fulfillment,variants,productType"
            headers = {
                "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
                "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
                "WM_SVC.NAME": "Walmart Marketplace",
                "Accept": "application/json"
            }
            print(f"Fetching details for SKU: {sku}...")
            STOCK_URL = f"https://marketplace.walmartapis.com/v3/inventory/?sku={sku}"
            response = requests.get(PRODUCT_DETAILS_URL, headers=headers)
            headers1 = {
                "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
                "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
                "WM_SVC.NAME": "Walmart Marketplace",
                "Accept": "application/json"
            }
            response1 = requests.get(STOCK_URL, headers=headers1)
            if response.status_code == 200:
                try:
                    print("✅ Product details fetched successfully", sku)
                    update_obj['price'] = sanitize_value(response.json()['ItemResponse'][0]['price']['amount'], value_type=float)
                except:
                    print("❌ Product details doesn't contain price", sku)
            else:
                print("❌ Failed to Fetch Products price", sku)
            if response1.status_code == 200:
                try:
                    print("✅ Stock Details fetched successfully", sku)
                    update_obj['quantity'] = sanitize_value(response1.json()['quantity']['amount'], value_type=int)
                except:
                    print("❌ Product details doesn't contain Inventory", sku)
            else:
                print("❌ Failed to Fetch Products inventory", sku)
            if update_obj != {}:
                DatabaseModel.update_documents(Product.objects, {"id": product_ins['_id']}, update_obj)
    return True
def saveProductCategory(marketplace_id, name, level, parent_id):
    pipeline = [
        {"$match": {"name": name,
                    "marketplace_id": marketplace_id}},
        {
            "$project": {
                "_id": 1
            }
        },
        {
            "$limit": 1
        }
    ]
    product_category_obj = list(Category.objects.aggregate(*pipeline))
    if product_category_obj != []:
        product_category_id = product_category_obj[0]['_id']
    if product_category_obj == []:
        product_category_obj = DatabaseModel.save_documents(
            Category, {
                "name": name,
                "level": level,
                "marketplace_id": marketplace_id,
            }
        )
        product_category_id = product_category_obj.id
    if parent_id is not None:
        DatabaseModel.update_documents(Category.objects, {"id": product_category_id}, {"parent_category_id": ObjectId(parent_id)})
    return product_category_id
def process_excel_for_walmart(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')
    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
        if pd.notnull(row['shelf']):
            breadcrumb_path = eval(row['shelf'])  
            parent_category = None
            for level, category_name in enumerate(breadcrumb_path):
                s = saveProductCategory(marketplace_id, category_name, level, parent_category)
                parent_category = s
            last_level_category = breadcrumb_path[-1]
        else:
            last_level_category = ""
        price_data = eval(row['price'])  
        if pd.notnull(row['shelf']):
            shelf_path = ' > '.join(breadcrumb_path)
        else:
            shelf_path = ""
        product = Product(
            marketplace_id=marketplace_id,
            sku=sanitize_value(str(row['sku']), value_type=str),
            product_id=sanitize_value(str(row['wpid']), value_type=str),
            upc=sanitize_value(str(int(row['upc'])), value_type=str),
            gtin=sanitize_value(str(int(row['gtin'])), value_type=str),
            product_title=sanitize_value(row['productName'], value_type=str),
            category=sanitize_value(last_level_category, value_type=str),
            shelf_path=sanitize_value(shelf_path, value_type=str),
            product_type=sanitize_value(row['productType'], value_type=str),
            item_condition=sanitize_value(row['condition'], value_type=str),
            availability=sanitize_value(row['availability'], value_type=str),
            price=sanitize_value(price_data['amount'], default=0.0, value_type=float),
            currency=sanitize_value(price_data['currency'], value_type=str),
            published_status=sanitize_value(row['publishedStatus'], value_type=str),
            unpublished_reasons=sanitize_value(row['unpublishedReasons'], value_type=str),
            lifecycle_status=sanitize_value(row['lifecycleStatus'], value_type=str),
            is_duplicate=sanitize_value(row['isDuplicate'], value_type=bool),
        )
        product.save()
def fetchAllorders1(request):
    orders = []
    user_id = request.GET.get('user_id')
    access_token = getAccesstoken(user_id)
    limit = sanitize_value(request.GET.get('limit'), default=100, value_type=int)  
    skip = sanitize_value(request.GET.get('skip'), default=0, value_type=int)  
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }
    total_fetched = 0
    next_cursor = None  
    while total_fetched < (skip + limit):
        print(skip, limit)
        url = f"{base_url}?createdStartDate=2024-01-01T00:00:00Z&limit=100"
        if next_cursor:
            url = f"{base_url}{next_cursor}"  
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            fetched_orders = result.get('list', {}).get('elements', {}).get('order', [])
            total_fetched += len(fetched_orders)
            if total_fetched >= skip:
                orders.extend(fetched_orders)
            if len(orders) >= limit:
                break
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break  
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break
    return orders[:limit]  
def fetchOrderDetails(request):
    data = dict()
    user_id = request.GET.get('user_id')
    purchase_order_id = request.GET.get('purchaseId')
    ACCESS_TOKEN = getAccesstoken(user_id)
    ORDER_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/orders/{purchase_order_id}"
    """
    Fetch order details from Walmart API using the generated access token and purchase order ID.
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,  
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
        "WM_SVC.NAME": "Walmart Marketplace",  
        "Accept": "application/json"
    }
    response = requests.get(ORDER_DETAILS_URL, headers=headers)
    if response.status_code == 200:
        order_details = response.json()
        data = order_details['order']
        print("✅ Order Details Fetched Successfully!")
    else:
        print(f"❌ Error fetching order details: [HTTP {response.status_code}] {response.text}")
    return data
def fetchBrand(request):
    data = dict()
    user_id = request.GET.get('user_id')
    sku = request.GET.get('sku')
    ACCESS_TOKEN = getAccesstoken(user_id)
    ORDER_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/catalog/items/{sku}"
    """
    Fetch order details from Walmart API using the generated access token and purchase order ID.
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,  
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
        "WM_SVC.NAME": "Walmart Marketplace",  
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    response = requests.get(ORDER_DETAILS_URL, headers=headers)
    if response.status_code == 200:
        order_details = response.json()
        data = order_details
        print("✅ Order Details Fetched Successfully!")
    else:
        print(f"❌ Error fetching order details: [HTTP {response.status_code}] {response.text}")
    return data
def process_excel_for_walmartorders(file_path):
    df = pd.read_excel(file_path)
    marketplace = "Walmart"
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')
    for index, row in df.iterrows():
        print(f"Processing row {index}...")
        shipNode = eval(row['shipNode']) if pd.notnull(row['shipNode']) else {}
        order_details = eval(row['orderLines']) if pd.notnull(row['orderLines']) else []
        order_total = 0
        currency = "USD"
        order_status = ""
        for order_line_ins in order_details['orderLine']:
            for charge_ins in order_line_ins['charges']['charge']:
                tax = 0
                if charge_ins['tax'] is not None:
                    tax = float(charge_ins['tax']['taxAmount']['amount'])
                order_total += float(charge_ins['chargeAmount']['amount']) + tax
                currency = charge_ins['chargeAmount']['currency']
            order_status = order_line_ins['orderLineStatuses']['orderLineStatus'][0]['status']
        order_date = row['orderDate'] if pd.notnull(row['orderDate']) else ""
        if order_date != "":
            order_date = datetime.fromtimestamp(int(order_date) / 1000)
        order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": str(row['purchaseOrderId'])})
        if order_obj is not None:
            print(f"Order with purchase order ID {row['purchaseOrderId']} already exists. Skipping...")
            DatabaseModel.update_documents(Order.objects, {"purchase_order_id": str(row['purchaseOrderId'])}, {"order_status": order_status, "currency": currency, "order_total": order_total})
        else:
            print(f"Creating order with purchase order ID {row['purchaseOrderId']}...")
            order = Order(
                marketplace_id=marketplace_id,
                purchase_order_id=sanitize_value(str(row['purchaseOrderId']), value_type=str),
                customer_order_id=sanitize_value(str(row['customerOrderId']), value_type=str),
                customer_email_id=sanitize_value(str(row['customerEmailId']), value_type=str),
                order_date=order_date,
                shipping_information=eval(row['shippingInfo']) if pd.notnull(row['shippingInfo']) else "",
                fulfillment_channel=sanitize_value(shipNode['type'], value_type=str),
                order_details=order_details['orderLine'],
                order_total=order_total,
                currency=sanitize_value(currency, value_type=str),
                order_status=sanitize_value(order_status, value_type=str),
            )
            order.save()
def saveBrand(marketplace_id, name):
    pipeline = [
        {"$match": {"name": name,
                    "marketplace_id": marketplace_id}},
        {
            "$project": {
                "_id": 1
            }
        },
        {
            "$limit": 1
        }
    ]
    brand_obj = list(Brand.objects.aggregate(*pipeline))
    if brand_obj != []:
        brand_id = brand_obj[0]['_id']
    if brand_obj == []:
        brand_obj = DatabaseModel.save_documents(
            Brand, {
                "name": name,
                "marketplace_id": marketplace_id,
            }
        )
        brand_id = brand_obj.id
    return brand_id
def update_product_images_from_csv(file_path):
    df = pd.read_csv(file_path)
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')
    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
        sku = sanitize_value(str(row['SKU']), value_type=str)
        quantity = sanitize_value(row['Input Quantity'], value_type=int)
        if sku and quantity:
            product = DatabaseModel.get_document(Product.objects, {"sku": sku, "marketplace_id": marketplace_id})
            if product:
                product.quantity = quantity
                product.save()
                print(f"✅ Updated image for SKU: {sku}")
            else:
                print(f"❌ Product with SKU: {sku} not found")
        else:
            print(f"❌ Invalid data in row {index + 1}")
def process_walmart_order(json_data, order_date=None, po_id=""):
    """Processes a single Walmart order item and saves it to the OrderItems collection."""
    try:
        product = DatabaseModel.get_document(Product.objects, {"sku": json_data.get("item", {}).get("sku", "Unknown SKU"), }, ["id"])
        product_id = product.id if product else None
    except:
        product_id = None
    try:
        product_price = {
            "CurrencyCode": json_data['charges']['charge'][0]['chargeAmount']['currency'],
            "Amount": float(json_data['charges']['charge'][0]['chargeAmount']['amount'])
        }
    except:
        product_price = {
            "CurrencyCode": "USD",
            "Amount": 0.0
        }
    try:
        tax_price = {
            "CurrencyCode": json_data ['charges']['charge'][0]['tax']['taxAmount']['currency'],
            "Amount": float(json_data['charges']['charge'][0]['tax']['taxAmount']['amount'])
        }
    except:
        tax_price = {
            "CurrencyCode": "USD",
            "Amount": 0.0
        }
    order_line_statuses = json_data.get("orderLineStatuses", {}).get("orderLineStatus", [])
    order_line_status = order_line_statuses[0] if order_line_statuses else {}
    tracking_info = order_line_status.get("trackingInfo", {})
    order_item = OrderItems(
        OrderId=po_id,
        created_date=order_date if order_date else datetime.now(),
        Platform="Walmart",
        ProductDetails=ProductDetails(
            product_id=product_id,
            Title=json_data.get("item", {}).get("productName", "Unknown Product"),
            SKU=json_data.get("item", {}).get("sku", "Unknown SKU"),
            Condition=json_data.get("item", {}).get("condition", "Unknown Condition"),
            QuantityOrdered=int(json_data.get("orderLineQuantity", {}).get("amount", 0)),
            QuantityShipped=int(order_line_status.get("statusQuantity", {}).get("amount", 0)),
        ),
        Pricing=Pricing(
            ItemPrice=Money(**product_price),
            ItemTax=Money(**tax_price)
        ),
        Fulfillment=Fulfillment(
            FulfillmentOption=json_data.get("fulfillment", {}).get("fulfillmentOption", "Unknown"),
            ShipMethod=json_data.get("fulfillment", {}).get("shipMethod", "Unknown"),
            Carrier=tracking_info.get("carrierName", {}).get("carrier", "Unknown"),
            TrackingNumber=tracking_info.get("trackingNumber", "Unknown"),
            TrackingURL=tracking_info.get("trackingURL", "Unknown"),
            ShipDateTime=datetime.fromtimestamp(tracking_info.get("shipDateTime", 0) / 1000) if tracking_info.get("shipDateTime") else None
        ),
        OrderStatus=OrderStatus(
            Status=order_line_status.get("status", "Unknown"),
            StatusDate=datetime.fromtimestamp(json_data.get("statusDate", 0) / 1000) if json_data.get("statusDate") else None
        ),
        TaxCollection=TaxCollection(
            Model="MarketplaceFacilitator",
            ResponsibleParty="Walmart"
        ),
        IsGift=False,
        BuyerInfo=None
    )
    order_item.save()  
    return order_item  
def updateOrdersItemsDetails(request):
    """Updates order items details for Walmart orders in the database."""
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Walmart"}).id
    order_list = DatabaseModel.list_documents(Order.objects, {"marketplace_id": marketplace_id}, ["id", "order_details"])
    for ins in order_list:
        order_items = []
        for item in ins.order_details:
            order_item = process_walmart_order(item)
            order_items.append(order_item)
        DatabaseModel.update_documents(Order.objects, {"id": ins.id}, {"order_items": order_items})
    return True
def syncRecentWalmartOrders():
    access_token = oauthFunction()
    marketplace_id = DatabaseModel.get_document(
        Marketplace.objects, {'name': "Walmart"}, ['id']
    ).id
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }
    today = datetime.utcnow()
    start_date = (today - timedelta(days=3)).strftime("%Y-%m-%dT00:00:00Z")
    end_date = today.strftime("%Y-%m-%dT23:59:59Z")
    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}&limit=100"
    fetched_orders = []
    next_cursor = None
    while True:
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        response = requests.get(paged_url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            fetched_orders.extend(new_orders)
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break
    unique_orders = {}
    for order in fetched_orders:
        po_id = order.get('purchaseOrderId')
        if po_id and po_id not in unique_orders:
            unique_orders[po_id] = order
    orders = list(unique_orders.values())
    def process_order(row):
        try:
            po_id = str(row.get('purchaseOrderId', ""))
            if Order.objects(purchase_order_id=po_id).only('id').first():
                print(f"Order {po_id} already exists. Updating status...")
                status = row['orderLines']['orderLine'][0]['orderLineStatuses']['orderLineStatus'][0]['status']
                DatabaseModel.update_documents(Order.objects, {"purchase_order_id": po_id}, {"order_status": status})
                return
            print(f"Creating order {po_id}...")
            order_date_ts = row.get('orderDate')
            order_date = datetime.fromtimestamp(int(order_date_ts) / 1000) if order_date_ts else None
            ship_node = row.get('shipNode', {})
            order_lines = row.get('orderLines', {}).get('orderLine', [])
            order_items = []
            order_total = 0
            currency = "USD"
            for order_line in order_lines:
                for charge in order_line.get('charges', {}).get('charge', []):
                    try:
                        charge_amount=charge.get('chargeAmount',{}).get('amount',0)
                        tax_obj=charge.get('tax')
                        tax_amount=0.0
                        if tax_obj:
                            tax_amount=tax_obj.get('taxAmount',{}).get('amount',0.0)
                        charge_val=float(charge_amount or 0.0)
                        tax_val=float(tax_amount or 0.0)
                        order_total += charge_val+tax_val
                        currency=charge.get('chargeAmount',{}).get('currency',currency)
                    except Exception as e:
                        print(f"Charge parse error: {e}")
                order_items.append(process_walmart_order(order_line, order_date, po_id))
            order_status = order_lines[0].get('orderLineStatuses', {}).get('orderLineStatus', [{}])[0].get('status', "")
            shipping_info = row.get('shippingInfo', {})
            order = Order(
                marketplace_id=marketplace_id,
                purchase_order_id=po_id,
                customer_order_id=sanitize_value(row.get('customerOrderId', ""), value_type=str),
                customer_email_id=sanitize_value(row.get('customerEmailId', ""), value_type=str),
                order_date=order_date,
                shipping_information=shipping_info,
                fulfillment_channel=sanitize_value(ship_node.get('type', ""), value_type=str),
                order_details=order_lines,
                order_items=order_items,
                order_total=order_total,
                currency=sanitize_value(currency, value_type=str),
                order_status=sanitize_value(order_status, value_type=str),
                items_order_quantity=len(order_items),
            )
            order.save()
        except Exception as e:
            print(f"⚠️ Error processing order {row.get('purchaseOrderId', '')}: {e}")   
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_order, orders)
    return orders
def get_all_walmart_items(access_token, limit=50):
    url = "https://marketplace.walmartapis.com/v3/items"
    headers = {
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_SVC.NAME": "Walmart Marketplace",
        "WM_QOS.CORRELATION_ID": "123456abcdef",  
        "Accept": "application/json"
    }
    start = 0
    items = []
    while True:
        params = {
            "limit": limit,
            "offset": start
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("ItemResponse", []))
        for ins in items:
            print(ins)
        if len(data.get("ItemResponse", [])) < limit:
            break
        start += limit
    return items
def syncWalmartPrice():
    token = oauthFunction()
    products = get_all_walmart_items(token)
    market_place_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Walmart"}, ['id']).id
    for row in products:
        try:
            price = sanitize_value(row['price']['amount'], value_type=float)
        except:
            price = 0.0
        published_status = sanitize_value(row['publishedStatus'], value_type=str)
        product_obj = DatabaseModel.get_document(Product.objects, {"sku": row['sku'], "marketplace_id": market_place_id}, ['id', 'price'])
        if product_obj:
            if product_obj.price != price:
                DatabaseModel.update_documents(Product.objects, {"sku": row['sku'], "marketplace_id": market_place_id}, {"published_status": published_status, "price": price})
                productPriceChange(
                    product_id=product_obj.id,
                    old_price=product_obj.price,
                    new_price=price,
                    reason="Price updated from Walmart API"
                ).save()
    return True
