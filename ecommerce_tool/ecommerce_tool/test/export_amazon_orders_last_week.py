from mongoengine import connect
from omnisight.models import Order, Product, Fee  
import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import pytz
from ecommerce_tool.settings import (
    AMAZON_API_KEY,
    AMAZON_SECRET_KEY,
    REFRESH_TOKEN,
    MARKETPLACE_ID
)
from ecommerce_tool.util.shipping_price import get_full_order_and_shipping_details,get_orders_by_customer_and_date
pacific = pytz.timezone("US/Pacific")
def get_amazon_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": AMAZON_API_KEY,
        "client_secret": AMAZON_SECRET_KEY,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"Failed to get access token: {e}")
        return None
def create_order_report(access_token, start_time, end_time):
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": start_time,
        "dataEndTime": end_time
    }
    response = requests.post(url, headers=headers, data=json.dumps(body))
    response.raise_for_status()
    return response.json().get("reportId")
def poll_report(access_token, report_id):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": access_token}
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        status = response.json().get("processingStatus")
        print(f" Report status: {status}")
        if status == "DONE":
            return response.json().get("reportDocumentId")
        elif status in ["CANCELLED", "FATAL"]:
            print(f"Report generation failed with status: {status}")
            return None
        time.sleep(30)
def download_report(access_token, document_id, output_filename):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": access_token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    doc_info = response.json()
    download_url = doc_info["url"]
    file_response = requests.get(download_url)
    with open(output_filename, "wb") as f:
        f.write(file_response.content)
    print(f"Report downloaded to {output_filename}")
    return output_filename
def load_report_to_dataframe(file_path):
    df = pd.read_csv(file_path, sep="\t", dtype=str)
    print(f"Report loaded with {len(df)} rows")
    return df
def create_order_report(access_token, start_date, end_date):
    """Create an order report with all available fields"""
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    payload = {
        "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
        "dataStartTime": start_date,
        "dataEndTime": end_date,
        "marketplaceIds": ["ATVPDKIKX0DER"]  
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 202:
        report_id = response.json()["reportId"]
        print(f"Report created with ID: {report_id}")
        return report_id
    else:
        print(f"Failed to create report: {response.status_code}")
        print(response.text)
        return None
def get_amazon_orders_report(start_date: datetime, end_date: datetime):
    from mongoengine import connect
    from omnisight.models import Order, Product, Fee  
    connect(
        db='ecommerce_db',
        host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
        port=27017
    )
    access_token = get_amazon_access_token()
    if not access_token:
        return
    iso_start = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_end = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"Requesting Amazon orders report from {iso_start} to {iso_end}")
    report_id = create_order_report(access_token, iso_start, iso_end)
    if not report_id:
        return
    document_id = poll_report(access_token, report_id)
    if not document_id:
        return
    filename = f"amazon_orders_{start_date.strftime('%Y-%m-%d')}.tsv"
    download_report(access_token, document_id, filename)
    df = load_report_to_dataframe(filename)
    if df is None:
        return
    import pytz
    from datetime import datetime
    pacific = pytz.timezone("US/Pacific")
    today_pacific = start_date.astimezone(pacific).date()
    def is_today_pacific(purchase_date_str,target_date):
        try:
            dt_utc = datetime.strptime(purchase_date_str, "%Y-%m-%dT%H:%M:%S%z")
            dt_pacific = dt_utc.astimezone(pacific)
            return dt_pacific.date() == target_date.date()
        except Exception:
            return False
    if 'purchase-date' in df.columns:
        df_today = df[df['purchase-date'].apply(lambda x: is_today_pacific(x, target_date))]
        def get_merchant_shipment_cost(amazon_order_id):
            try:
                print(f"[MerchantCost] Looking for order: {amazon_order_id}")
                order = Order.objects(purchase_order_id=amazon_order_id).first()
                if not order:
                    print(f"[MerchantCost] No order found in DB for {amazon_order_id} — cost=0.0")
                    return 0.0
                cost=0.0
                db_cost = float(getattr(order, "merchant_shipment_cost", 0.0) or 0.0)
                if db_cost>0:
                    print(f"[MerchantCost] Using DB value for {amazon_order_id}: ${db_cost:.2f}")
                    cost = db_cost
                elif order.fulfillment_channel == "MFN":
                    print(f"[MerchantCost] Order {amazon_order_id} is MFN — fetching from ShipStation...")
                    ss_order = get_full_order_and_shipping_details(amazon_order_id)
                    if ss_order and 'shipments' in ss_order and ss_order['shipments']:
                        last_shipment_cost = float(ss_order['shipments'][-1].get('shipmentCost', 0) or 0)
                        if last_shipment_cost > 0:
                            cost = last_shipment_cost
                            print(f"[MerchantCost] Got cost from ShipStation: ${cost:.2f}")
                            order.merchant_shipment_cost = cost
                            order.save()
                        else:
                            print(f"[MerchantCost] No shipment cost found in ShipStation for {amazon_order_id}")
                    else:
                        print(f"[MerchantCost] No ShipStation data found for {amazon_order_id}")
                else:
                    print(f"[MerchantCost] Using DB value for {amazon_order_id}: ${cost:.2f}")
                print(f"[MerchantCost] Final merchant shipping cost for {amazon_order_id}: ${cost:.2f}")
                return cost
            except Exception as e:
                print(f" Error fetching merchant_shipment_cost for {amazon_order_id}: {e}")
                return 0.0
            
        def get_vendor_funding_by_sku(sku):
            try:
                if not sku:
                    return 0.0
                product = Product.objects(sku=sku).first()
                if product:
                    vendor_funding = float(getattr(product, 'vendor_funding', 0.0) or 0.0)
                    return vendor_funding
                return 0.0
            except Exception as e:
                print(f"Error fetching vendor funding for SKU {sku}: {e}")
                return 0.0
            
        def get_product_cost_procurement_price(sku):
            try:
                if not sku:
                    return 0.0
                product = Product.objects(sku=sku).first()
                if product:
                    cost_price = float(getattr(product, 'cogs', 0.0) or 0.0)
                    if cost_price == 0.0:
                        cost_price = float(getattr(product, 'product_cost', 0.0) or 0.0)
                    if cost_price == 0.0:
                        cost_price = float(getattr(product, 'w_product_cost', 0.0) or 0.0)
                    if cost_price == 0.0:
                        cost_price = float(getattr(product, 'total_cogs', 0.0) or 0.0)
                    return cost_price
                return 0.0
            except Exception as e:
                print(f"Error fetching product cost for SKU {sku}: {e}")
                return 0.0
        def get_amazon_fee(amazon_order_id, item_price):
            try:
                order = Order.objects(purchase_order_id=amazon_order_id).first()
                amazon_fee = 0.0
                try:
                    item_price=float(item_price or 0)
                except ValueError:
                    print(f" item-price value '{item_price}' is invalid for order {amazon_order_id}")
                    item_price=0.0
                if order:
                    order_date = getattr(order, 'order_date', None)
                    if order_date:
                        from datetime import timedelta
                        start_date = order_date - timedelta(days=1)
                        end_date = order_date + timedelta(days=1)
                        fees = Fee.objects(
                            marketplace__in=['Amazon', 'amazon'],
                            date__gte=start_date,
                            date__lte=end_date
                        )
                        for fee in fees:
                            fee_type = getattr(fee, 'fee_type', '').lower()
                            if any(keyword in fee_type for keyword in ['referral', 'commission', 'selling', 'amazon']):
                                amazon_fee += float(getattr(fee, 'amount', 0.0) or 0.0)
                if amazon_fee == 0.0 and item_price > 0:
                    product = None
                    if order and hasattr(order, 'order_items') and order.order_items:
                        for order_item in order.order_items:
                            if hasattr(order_item, 'ProductDetails') and order_item.ProductDetails:
                                product = order_item.ProductDetails.product_id
                                break
                    if product:
                        referral_fee = float(getattr(product, 'referral_fee', 0.0) or 0.0)
                        if referral_fee > 0:
                            amazon_fee = referral_fee
                        else:
                            amazon_fee = float(item_price) * 0.15  
                    else:
                        amazon_fee = float(item_price) * 0.15  
                return amazon_fee
            except Exception as e:
                print(f" Error fetching Amazon fee for {amazon_order_id}: {e}")
                return 0.0
            
        def calculate_funding(row):
            try:
                item_price = float(row.get('item-price', 0) or 0)
                shipping_price = float(row.get('shipping-price', 0) or 0)
                item_tax = float(row.get('item-tax', 0) or 0)
                shipping_tax = float(row.get('shipping-tax', 0) or 0)
                item_promotion = float(row.get('item-promotion-discount', 0) or 0)
                ship_promotion = float(row.get('ship-promotion-discount', 0) or 0)
                sku = row.get('sku', '') 
                total_funding = get_vendor_funding_by_sku(sku)
                return total_funding
            except Exception as e:
                print(f" Error calculating funding: {e}")
                return 0.0
        def calculate_tax(row):
            try:
                item_tax = float(row.get('item-tax', 0) or 0)
                shipping_tax = float(row.get('shipping-tax', 0) or 0)
                return item_tax + shipping_tax
            except Exception as e:
                print(f" Error calculating tax: {e}")
                return 0.0
        if 'amazon-order-id' in df_today.columns:
            print("🔄 Fetching merchant shipment costs from DB / ShipStation...")
            df_today['merchant_shipment_cost'] = df_today['amazon-order-id'].apply(get_merchant_shipment_cost)
            print(f"✅ Added merchant shipment costs for {len(df_today)} orders")
        else:
            print(" 'amazon-order-id' column not found - cannot fetch merchant shipment costs")
            df_today['merchant_shipment_cost'] = 0.0
        print("🔄 Adding custom fields...")
        if 'amazon-order-id' in df_today.columns:
            df_today['Order ID'] = df_today['amazon-order-id']
        if 'buyer-email' in df_today.columns:
            df_today['Customer ID'] = df_today['buyer-email']
        elif 'buyer-name' in df_today.columns:
            df_today['Customer ID'] = df_today['buyer-name']
        else:
            df_today['Customer ID'] = ''
        if 'purchase-date' in df_today.columns:
            def format_order_date(purchase_date_str):
                try:
                    if not purchase_date_str:
                        return ''
                    dt_utc = datetime.strptime(purchase_date_str, "%Y-%m-%dT%H:%M:%S%z")
                    dt_pacific = dt_utc.astimezone(pacific)
                    return dt_pacific.strftime("%m/%d/%Y")
                except Exception as e:
                    print(f" Error formatting date {purchase_date_str}: {e}")
                    return purchase_date_str  
            df_today['Order Date'] = df_today['purchase-date'].apply(format_order_date)
        if 'sku' in df_today.columns:
            print("📦 Fetching product costs (procurement prices)...")
            df_today['Product Cost (Procurement Price)'] = df_today['sku'].apply(get_product_cost_procurement_price)
        else:
            df_today['Product Cost (Procurement Price)'] = 0.0
        df_today['Ship Cost (Merchant Cost)'] = df_today['merchant_shipment_cost']
        if 'item-price' in df_today.columns:
            df_today['Product Price (Customer Price)'] = df_today['item-price'].astype(float)
        else:
            df_today['Product Price (Customer Price)'] = 0.0
        if 'shipping-price' in df_today.columns:
            df_today['Shipping Cost (Taken from ShipStation (under from cost summary))'] = df_today['shipping-price'].astype(float)
        else:
            df_today['Shipping Cost (Taken from ShipStation (under from cost summary))'] = 0.0
        print("💰 Calculating funding...")
        df_today['Funding'] = df_today.apply(calculate_funding, axis=1)
        if 'amazon-order-id' in df_today.columns and 'item-price' in df_today.columns:
            print("💳 Fetching Amazon fees...")
            df_today['Amazon Fee'] = df_today.apply(lambda row: get_amazon_fee(row['amazon-order-id'], row['item-price']), axis=1)
        else:
            df_today['Amazon Fee'] = 0.0
        print("🧾 Calculating taxes...")
        df_today['Tax'] = df_today.apply(calculate_tax, axis=1)
        print("\n📋 Available columns in the report:")
        print(df.columns.tolist())
        df_today.to_excel(filename.replace(".tsv", "_today.xlsx"), index=False)
        print(f"📁 Excel saved with all fields: {filename.replace('.tsv', '_today.xlsx')}")
        financial_columns = [
            'amazon-order-id', 'purchase-date', 'order-status',
            'sku', 'product-name', 'quantity-purchased',
            'item-price', 'item-tax', 'shipping-price', 'shipping-tax',
            'item-promotion-discount', 'ship-promotion-discount',
            'merchant_shipment_cost',
            'currency',
            'Order ID', 'Customer ID', 'Order Date',
            'Product Cost (Procurement Price)', 'Ship Cost (Merchant Cost)',
            'Product Price (Customer Price)', 'Shipping Cost (Taken from ShipStation (under from cost summary))',
            'Funding', 'Amazon Fee', 'Tax'
        ]
        available_financial_cols = [col for col in financial_columns if col in df_today.columns]
        df_summary = df_today[available_financial_cols].copy()
        if 'item-price' in df_summary.columns and 'shipping-price' in df_summary.columns:
            df_summary['total-charged'] = (
                df_summary['item-price'].fillna(0).astype(float) +
                df_summary['shipping-price'].fillna(0).astype(float) +
                df_summary['item-tax'].fillna(0).astype(float) +
                df_summary['shipping-tax'].fillna(0).astype(float) -
                df_summary['item-promotion-discount'].fillna(0).astype(float) -
                df_summary['ship-promotion-discount'].fillna(0).astype(float)
            )
            if 'merchant_shipment_cost' in df_summary.columns:
                df_summary['net_after_merchant_shipping'] = (
                    df_summary['total-charged'] -
                    df_summary['merchant_shipment_cost'].fillna(0).astype(float)
                )
        if 'Product Cost (Procurement Price)' in df_today.columns:
            total_product_costs = df_today['Product Cost (Procurement Price)'].fillna(0).astype(float).sum()
            print(f"💰 Total product costs (procurement): ${total_product_costs:.2f}")
        if 'Funding' in df_today.columns:
            total_funding = df_today['Funding'].fillna(0).astype(float).sum()
            print(f"💰 Total funding (customer payments): ${total_funding:.2f}")
        if 'Amazon Fee' in df_today.columns:
            total_amazon_fees = df_today['Amazon Fee'].fillna(0).astype(float).sum()
            print(f"💳 Total Amazon fees: ${total_amazon_fees:.2f}")
        if 'merchant_shipment_cost' in df_today.columns:
            total_merchant_shipping = df_today['merchant_shipment_cost'].fillna(0).astype(float).sum()
            print(f"🚚 Total merchant shipment costs: ${total_merchant_shipping:.2f}")
        df_summary.to_excel(filename.replace(".tsv", "_financial_summary.xlsx"), index=False)
        print(f"📁 Financial summary saved: {filename.replace('.tsv', '_financial_summary.xlsx')}")
        return df_today
    else:
        print("'purchase-date' column not found in report.")
        return df
import os
import json
import time
import gzip
import requests
import pandas as pd
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from ecommerce_tool.settings import (
    AMAZON_API_KEY,
    AMAZON_SECRET_KEY,
    REFRESH_TOKEN,
    MARKETPLACE_ID
)
pacific = pytz.timezone("US/Pacific")
def create_report(access_token, report_type, marketplace_ids, data_start_time=None, data_end_time=None):
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": report_type,
        "marketplaceIds": marketplace_ids,
    }
    if data_start_time:
        body["dataStartTime"] = data_start_time
    if data_end_time:
        body["dataEndTime"] = data_end_time
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        return response.json().get("reportId")
    except Exception as e:
        print(f"Failed to create report of type {report_type}: {e}")
        return None
def poll_report(access_token, report_id):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": access_token}
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get("processingStatus")
            print(f" Report status: {status}")
            if status == "DONE":
                return response.json().get("reportDocumentId")
            elif status in ["CANCELLED", "FATAL"]:
                print(f" Report generation failed with status: {status}. Response: {response.json()}")
                return None
            time.sleep(30)
        except Exception as e:
            print(f" Error polling report {report_id}: {e}")
            return None
def download_report(access_token, document_id, output_filename):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": access_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        doc_info = response.json()
        download_url = doc_info["url"]
        file_response = requests.get(download_url)
        file_response.raise_for_status()
        content = file_response.content
        if doc_info.get('compressionAlgorithm') == 'GZIP':
            print("Detected GZIP compression, decompressing...")
            content = gzip.decompress(content)
        with open(output_filename, "wb") as f:
            f.write(content)
        print(f" Report downloaded and decompressed to {output_filename}")
        return output_filename
    except Exception as e:
        print(f" Failed to download report document {document_id}: {e}")
        return None
def load_report_to_dataframe(file_path):
    try:
        encodings_to_try = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        df = None
        successful_encoding = None
        for encoding in encodings_to_try:
            try:
                print(f" Trying encoding: {encoding}")
                df = pd.read_csv(file_path, sep="\t", dtype=str, encoding=encoding)
                successful_encoding = encoding
                print(f" Successfully loaded with encoding: {encoding}")
                break
            except UnicodeDecodeError as e:
                print(f"Failed with {encoding}: {e}")
                continue
            except Exception as e:
                print(f" Other error with {encoding}: {e}")
                continue
        if df is not None:
            print(f"Report loaded with {len(df)} rows using {successful_encoding} encoding")
            return df
        else:
            print("Failed to load report with any of the attempted encodings")
            return None
    except Exception as e:
        print(f"Failed to load report to DataFrame from {file_path}: {e}")
        return None
def get_product_category(access_token, asin, marketplace_id):
    url = f"https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items/{asin}"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    params = {
        "marketplaceIds": marketplace_id,
        "includedData": "attributes,categories,productTypes"
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            categories = []
            product_types = []
            if "categories" in data:
                for category in data["categories"]:
                    if "displayName" in category:
                        categories.append(category["displayName"])
            if "productTypes" in data:
                for pt in data["productTypes"]:
                    if "displayName" in pt:
                        product_types.append(pt["displayName"])
            return {
                "categories": " | ".join(categories) if categories else "",
                "product_types": " | ".join(product_types) if product_types else "",
                "primary_category": categories[0] if categories else ""
            }
    except Exception as e:
        print(f"Error getting category for ASIN {asin}: {e}")
    return {
        "categories": "",
        "product_types": "",
        "primary_category": ""
    }
def get_categories_for_products(access_token, asins, marketplace_id, max_workers=5):
    print(f" Fetching category information for {len(asins)} products...")
    category_data = {}
    def fetch_category(asin):
        time.sleep(0.5)  
        return asin, get_product_category(access_token, asin, marketplace_id)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_asin = {executor.submit(fetch_category, asin): asin for asin in asins}
        completed = 0
        for future in as_completed(future_to_asin):
            asin, category_info = future.result()
            category_data[asin] = category_info
            completed += 1
            if completed % 10 == 0:  
                print(f"Progress: {completed}/{len(asins)} products processed")
    print(f"Category data fetched for {len(category_data)} products")
    return category_data
def get_amazon_products_report_with_categories():
    access_token = get_amazon_access_token()
    if not access_token:
        return None
    report_type = "GET_MERCHANT_LISTINGS_ALL_DATA"
    print(f"Requesting Amazon products report ({report_type})")
    report_id = create_report(access_token, report_type, [MARKETPLACE_ID])
    if not report_id:
        return None
    document_id = poll_report(access_token, report_id)
    if not document_id:
        return None
    filename = f"amazon_products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tsv"
    downloaded_path = download_report(access_token, document_id, filename)
    if not downloaded_path:
        return None
    df = load_report_to_dataframe(downloaded_path)
    if df is None:
        return None
    asin_column = None
    possible_asin_columns = ['asin', 'ASIN', 'asin1', 'ASIN1', 'item-name']
    for col in possible_asin_columns:
        if col in df.columns:
            asin_column = col
            break
    if not asin_column:
        print("Could not find ASIN column in the report. Available columns:")
        print(df.columns.tolist())
        print("📋 Saving report without category information...")
    else:
        print(f"Found ASIN column: {asin_column}")
        unique_asins = df[asin_column].dropna().unique()
        unique_asins = [asin for asin in unique_asins if asin and str(asin).strip()]
        if len(unique_asins) > 0:
            print(f"Found {len(unique_asins)} unique ASINs to process")
            category_data = get_categories_for_products(access_token, unique_asins, MARKETPLACE_ID)
            df['primary_category'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('primary_category', ''))
            df['all_categories'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('categories', ''))
            df['product_types'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('product_types', ''))
            print("Category information added to the dataframe")
        else:
            print("No valid ASINs found in the report")
    excel_filename = downloaded_path.replace(".tsv", ".xlsx")
    df.to_excel(excel_filename, index=False)
    print(f"📁 Excel saved with category information: {excel_filename}")
    return df
def get_amazon_products_report_alternative():
    access_token = get_amazon_access_token()
    if not access_token:
        return None
    report_types_to_try = [
        "GET_MERCHANT_LISTINGS_DATA",  
        "GET_FLAT_FILE_OPEN_LISTINGS_DATA",  
        "GET_FBA_MYI_ALL_INVENTORY_DATA"  
    ]
    for report_type in report_types_to_try:
        print(f"📦 Trying report type: {report_type}")
        report_id = create_report(access_token, report_type, [MARKETPLACE_ID])
        if report_id:
            document_id = poll_report(access_token, report_id)
            if document_id:
                filename = f"amazon_products_{report_type}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tsv"
                downloaded_path = download_report(access_token, document_id, filename)
                if downloaded_path:
                    df = load_report_to_dataframe(downloaded_path)
                    if df is not None:
                        print(f"✅ Successfully got report with {report_type}")
                        print("Available columns:", df.columns.tolist())
                        excel_filename = downloaded_path.replace(".tsv", ".xlsx")
                        df.to_excel(excel_filename, index=False)
                        print(f"📁 Excel saved: {excel_filename}")
                        return df
        print(f"Failed to get report with {report_type}, trying next...")
    print("All alternative report types failed")
    return None
def get_amazon_orders_report_with_shipment_costs(start_date: datetime, end_date: datetime):
    connect(
        db='ecommerce_db',
        host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
        port=27017
    )
    access_token = get_amazon_access_token()
    if not access_token:
        return
    iso_start = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_end = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"📦 Requesting Amazon orders report from {iso_start} to {iso_end}")
    report_id = create_order_report(access_token, iso_start, iso_end)
    if not report_id:
        return
    document_id = poll_report(access_token, report_id)
    if not document_id:
        return
    filename = f"amazon_orders_{start_date.strftime('%Y-%m-%d')}.tsv"
    download_report(access_token, document_id, filename)
    df = load_report_to_dataframe(filename)
    if df is None:
        return
def get_fba_shipping_and_fee_report(start_date: datetime, end_date: datetime):
    access_token = get_amazon_access_token()
    if not access_token:
        return None
    report_type = "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA"
    marketplace_ids = [MARKETPLACE_ID]
    print(f"📦 Requesting FBA shipping/fee report from {start_date} to {end_date}")
    report_id = create_report(
        access_token,
        report_type,
        marketplace_ids,
        data_start_time=start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        data_end_time=end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    )
    if not report_id:
        return None
    document_id = poll_report(access_token, report_id)
    if not document_id:
        return None
    filename = f"fba_fees_{start_date.strftime('%Y-%m-%d')}.tsv"
    downloaded_path = download_report(access_token, document_id, filename)
    if not downloaded_path:
        return None
    df = load_report_to_dataframe(downloaded_path)
    if df is None:
        return None
    excel_filename = downloaded_path.replace(".tsv", ".xlsx")
    df.to_excel(excel_filename, index=False)
    print(f"📁 Excel saved: {excel_filename}")
    print(f"\n📊 FBA Fees Report Loaded: {df.shape[0]} rows")
    print("📋 Columns:", df.columns.tolist())
    print(df.head())
    return df
if __name__ == "__main__":
    pacific = pytz.timezone("US/Pacific")
    target_date = pacific.localize(datetime(2025, 8, 11))
    start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_utc = start_of_day.astimezone(pytz.utc)
    end_utc = end_of_day.astimezone(pytz.utc)
    df = get_amazon_orders_report(start_utc, end_utc)
    def is_target_pacific_day(purchase_date_str):
        try:
            dt_utc = datetime.strptime(purchase_date_str, "%Y-%m-%dT%H:%M:%S%z")
            dt_pacific = dt_utc.astimezone(pacific)
            return dt_pacific.date() == target_date.date()
        except Exception:
            return False
    if df is not None and 'purchase-date' in df.columns:
        df_today = df[df['purchase-date'].apply(is_target_pacific_day)]
        df_today.to_excel("filtered_orders.xlsx", index=False)
