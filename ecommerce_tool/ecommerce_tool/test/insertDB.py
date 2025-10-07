import pandas as pd 
from pymongo import MongoClient
import datetime
import requests
import time
import sys
import uuid
from omnisight.operations.walmart_utils import oauthFunction

sys.path.append('/home/lexicon/Documents/Harisankar/Projects/ecommerce-project/MarketLink-Hari/ecommerce_tool/')
from omnisight.models import Product
from mongoengine import connect, DoesNotExist
client = MongoClient('mongodb://plmp_admin:admin%401234@54.86.75.104:27017/')
db = client['ecommerce_db']
product_collection = db['product']
# connect(
#     db="ecommerce_db",
#     host="mongodb://plmp_admin:admin%401234@54.86.75.104:27017/"
# )
def fetch_walmart_data(sku,access_token):
    """
    Fetch product data from Walmart API using SKU
    """
    try:
        
        url = f"https://marketplace.walmartapis.com/v3/items/{sku}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "WM_SEC.ACCESS_TOKEN": access_token,
            "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
            "WM_SVC.NAME": "Walmart Marketplace",
            "Accept": "application/json"
        }
        response = requests.get(url,headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('ItemResponse') and len(data['ItemResponse']) > 0:
                return data['ItemResponse'][0]  
            else:
                print(f"⚠️ No item data found for SKU: {sku}")
                return None
        else:
            print(f"❌ API error for SKU {sku}: Status {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for SKU {sku}: {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error for SKU {sku}: {str(e)}")
        return None
def update_product_wpid(sku, walmart_data):
    try:
        product = Product.objects.get(sku=sku)
        wpid = walmart_data.get('wpid')
        if wpid:
            product.wpid = wpid
            product.updated_at = datetime.datetime.now()
            product.save()
            print(f"✅ Updated SKU: {sku} | WPID: {wpid}")
            return True
        else:
            print(f"❌ No WPID found for SKU: {sku}")
            return False
    except DoesNotExist:
        print(f"❌ Product with SKU {sku} not found in database")
        return False
    except Exception as e:
        print(f"❌ Error updating product SKU {sku}: {str(e)}")
        return False
    
def process_excel_for_wpid_update(excel_path, sheet_name="Walmart", delay_seconds=1):
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        print(f"📊 Loaded {len(df)} rows from Excel sheet: {sheet_name}")
        sku_column = None
        if "SKU" in df.columns:
            sku_column = "SKU"
        else:
            print(f"❌ No 'SKU' column found. Available columns: {list(df.columns)}")
            return

        if not sku_column:
            print(f"❌ No SKU column found. Available columns: {list(df.columns)}")
            return
        print(f"🔍 Using SKU column: {sku_column}")
        access_token=oauthFunction()
        token_time=time.time()
        total_processed = 0
        successful_updates = 0
        not_found_in_db = 0
        already_has_wpid = 0
        api_errors = 0
        for index, row in df.iterrows():
            if time.time()-token_time>720:
                print(f"Refreshing walmart access token")
                access_token=oauthFunction()
                token_time=time.time()
            sku = str(row[sku_column]).strip() if not pd.isna(row[sku_column]) else ""
            if not sku or sku == "" or sku.lower() == 'nan':
                print(f"⚠️ Empty SKU at row {index + 1}, skipping")
                continue
            total_processed += 1
            print(f"\n🔄 Processing SKU: {sku} (Row {index + 1}/{len(df)})")
            try:
                product = Product.objects.get(sku=sku)
                if hasattr(product, 'wpid') and product.wpid and str(product.wpid).strip():
                    print(f"✅ SKU {sku} already has WPID: {product.wpid}, skipping")
                    already_has_wpid += 1
                    continue
            except DoesNotExist:
                print(f"❌ Product with SKU {sku} not found in database")
                not_found_in_db += 1
                continue
            walmart_data = fetch_walmart_data(sku,access_token)
            if walmart_data:
                if update_product_wpid(sku, walmart_data):
                    successful_updates += 1
                else:
                    api_errors += 1
            else:
                api_errors += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        print("\n" + "="*60)
        print("🎯 WPID UPDATE SUMMARY")
        print("="*60)
        print(f"📊 Total rows processed: {total_processed}")
        print(f"✅ Successfully updated: {successful_updates}")
        print(f"⚠️ Already had WPID: {already_has_wpid}")
        print(f"❌ Not found in database: {not_found_in_db}")
        print(f"🔥 API/Update errors: {api_errors}")
        print(f"🎉 Success rate: {(successful_updates/total_processed)*100:.1f}%" if total_processed > 0 else "N/A")
        print("="*60)
    except Exception as e:
        print(f"❌ Error processing Excel file: {str(e)}")
        
def update_single_sku_wpid(sku):
    """
    Update WPID for a single SKU (for testing)
    """
    print(f"🔄 Processing single SKU: {sku}")
    try:
        product = Product.objects.get(sku=sku)
        print(f"✅ Found product in database: {product.product_title or 'No title'}")
        if hasattr(product, 'wpid') and product.wpid and str(product.wpid).strip():
            print(f"⚠️ Product already has WPID: {product.wpid}")
            return True
    except DoesNotExist:
        print(f"❌ Product with SKU {sku} not found in database")
        return False
    walmart_data = fetch_walmart_data(sku)
    if walmart_data:
        return update_product_wpid(sku, walmart_data)
    else:
        print(f"❌ Failed to fetch Walmart data for SKU: {sku}")
        return False
if __name__ == "__main__":
    excel_path = '/home/lexicon/Downloads/Amazon_Walmart_Catalog - Updated11-1 (2).xlsx'
    sheet_name = "Walmart"
    delay_between_requests = 1  
    print("🚀 Starting WPID Update Process...")
    print(f"📁 Excel file: {excel_path}")
    print(f"📋 Sheet: {sheet_name}")
    print(f"⏱️ Delay between API calls: {delay_between_requests}s")
    print("-" * 60)
    process_excel_for_wpid_update(excel_path, sheet_name, delay_between_requests)
    print("\n🏁 WPID update process completed!")