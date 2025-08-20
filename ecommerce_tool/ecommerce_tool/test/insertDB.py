import pandas as pd 
from pymongo import MongoClient
import datetime
import sys
sys.path.append('/home/lexicon/Documents/Harisankar/Projects/ecommerce-project/MarketLink-Hari/ecommerce_tool/')
from omnisight.models import Product
from mongoengine import connect

# Mongo connection
client = MongoClient('mongodb://plmp_admin:admin%401234@54.86.75.104:27017/')
db = client['ecommerce_db']
product_collection = db['product']

connect(
    db="ecommerce_db",
    host="mongodb://plmp_admin:admin%401234@54.86.75.104:27017/"
)

# Load Excel
excel_path = '/home/lexicon/Downloads/Amazon_Walmart_Catalog - Updated1.xlsx'
df = pd.read_excel(excel_path, sheet_name="New Products")


def parse_currency(value):
    if pd.isna(value) or value == "":
        return 0.0
    value_str = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(value_str)
    except ValueError:
        return 0.0

products_to_insert = []
for _, row in df.iterrows():
    try:
        product = Product(
            master_sku=row["master_sku"] if "master_sku" in row else "",
            parent_sku=row["parent_sku"] if "parent_sku" in row else "",
            sku=row["Seller SKU"] if "Seller SKU" in row else "",
            product_title=row["product_title"] if "product_title" in row else "",
            brand_name=row["brand_name"] if "brand_name" in row else "",
            pack_size=int(row["pack_size"]) if "pack_size" in row and not pd.isna(row["pack_size"]) else 0,
            product_cost=parse_currency(row["product_cost"]) if "product_cost" in row else 0.0,
            price=0.0,  # not in Excel, default to 0
            referral_fee=0.0,  # not in Excel, default to 0
            vendor_funding=parse_currency(row["Funding"]) if "Funding" in row else 0.0,
            asin=row["asin"] if "asin" in row else "",
            manufacturer_name=row["manufacturer_name"] if "manufacturer_name" in row else "",
            product_created_date=datetime.datetime.now(),
            producted_last_updated_date=datetime.datetime.now()
        )
        products_to_insert.append(product)
    except Exception as e:
        print(f"❌ Error logging product {row.get('master_sku', 'UNKNOWN')}: {e}")

if products_to_insert:
    Product.objects.insert(products_to_insert)
    print(f"✅ {len(products_to_insert)} products inserted successfully!")
else:
    print("⚠️ No products to insert")
