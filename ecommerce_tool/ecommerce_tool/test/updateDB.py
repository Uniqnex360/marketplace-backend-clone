from __future__ import annotations
import pandas as pd
from pymongo import MongoClient


client = MongoClient('mongodb://plmp_admin:admin%401234@54.86.75.104:27017/')
db = client['ecommerce_db']
product_collection = db["product"]


excel_path = '/home/lexicon/Downloads/Amazon_Walmart_Catalog - Updated1.xlsx'
df = pd.read_excel(excel_path, sheet_name="Pack Size")


changes = []
not_found = []

for _, row in df.iterrows():
    sku = row['Seller SKU']
    cost = row['Cost']
    pack_size = row['Pack Size'] if not pd.isna(row['Pack Size']) else None

    
    current_doc = product_collection.find_one({"sku": sku})
    if not current_doc:
        print(f"⚠️ SKU {sku} not found in DB, skipping")
        not_found.append({"SKU": sku, "Excel Cost": cost, "Excel Pack Size": pack_size})
        continue

    updates = {}
    before_cost = current_doc.get("product_cost")
    before_pack_size = current_doc.get("pack_size")

    
    if pd.notna(cost) and cost != before_cost:
        updates["product_cost"] = cost

    
    if pack_size is not None and pack_size != before_pack_size:
        updates["pack_size"] = int(pack_size)

    
    if updates:
        product_collection.update_one({"sku": sku}, {"$set": updates})
        changes.append({
            "SKU": sku,
            "Before Cost": before_cost,
            "After Cost": updates.get("product_cost", before_cost),
            "Before Pack Size": before_pack_size,
            "After Pack Size": updates.get("pack_size", before_pack_size)
        })
        print(f"✅ Updated SKU {sku} | Cost: {before_cost} → {updates.get('product_cost', before_cost)} | "
              f"Pack Size: {before_pack_size} → {updates.get('pack_size', before_pack_size)}")
    else:
        print(f"ℹ️ No update needed for SKU {sku}")

print("Update complete!")


log_file = "/home/lexicon/Downloads/updated_costs_log12.xlsx"
with pd.ExcelWriter(log_file, engine="openpyxl") as writer:
    if changes:
        pd.DataFrame(changes).to_excel(writer, index=False, sheet_name="Updated SKUs")
    if not_found:
        pd.DataFrame(not_found).to_excel(writer, index=False, sheet_name="Not Found SKUs")

print(f"📂 Change log saved to {log_file}")
