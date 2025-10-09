# import pandas as pd
# from datetime import datetime
# from mongoengine import connect
# import os
# import sys
# from bson import ObjectId

# # ======= CONFIG =======
# project_root = os.path.abspath(
#     os.path.join(os.path.dirname(__file__), "..", "..")
# )   
# print("📦 Added project root:", project_root)
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# from omnisight.models import Order, OrderItems, Marketplace

# EXCEL_PATH = "/home/lexicon/Downloads/orders_and_items_export_20251006_223508 (1).xlsx"

# # ======= DATABASE CONNECTION =======
# DATABASE_HOST = 'mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0'
# DATABASE_NAME = 'ecommerce_db'

# print(f"🔗 Connecting to database: {DATABASE_NAME}")

# try:
#     connect(
#         db=DATABASE_NAME,
#         host=DATABASE_HOST,
#         alias='default'
#     )
#     print("✅ Database connection established!")
    
#     # Verify connection and check existing data
#     from mongoengine import connection
#     db = connection.get_db()
#     print(f"📊 Connected to database: {db.name}")
#     print(f"📚 Existing OrderItems: {OrderItems.objects.count()}")
#     print(f"📚 Existing Orders: {Order.objects.count()}")
#     print(f"📚 Existing Marketplaces: {Marketplace.objects.count()}")
    
# except Exception as e:
#     print(f"❌ Database connection failed: {e}")
#     sys.exit(1)

# # ======= HELPERS =======
# def safe_float(v):
#     try:
#         if pd.isna(v):
#             return 0.0
#         return float(v)
#     except (TypeError, ValueError):
#         return 0.0

# def safe_int(v):
#     try:
#         if pd.isna(v):
#             return 0
#         return int(v)
#     except (TypeError, ValueError):
#         return 0

# def safe_bool(v):
#     if pd.isna(v):
#         return False
#     return str(v).strip().lower() in ["true", "1", "yes", "y"]

# def safe_string(v):
#     if pd.isna(v) or v is None:
#         return ""
#     return str(v).strip()

# def safe_date(v):
#     if pd.notnull(v) and str(v).strip():
#         try:
#             return pd.to_datetime(v)
#         except Exception:
#             return None
#     return None

# def normalize_channel(channel):
#     if not channel or not isinstance(channel, str):
#         return "Unknown"
#     channel = channel.strip()
#     mapping = {
#         "Amazon.com": "Amazon",
#         "Amazon": "Amazon",
#         "eBay": "eBay", 
#         "TikTok": "TikTok",
#         "Tesco": "Tesco"
#     }
#     return mapping.get(channel, channel)

# def normalize_country(country_code):
#     if not country_code or not isinstance(country_code, str):
#         return "Unknown"
#     code = country_code.strip().upper()
#     mapping = {
#         "US": "United States",
#         "UK": "United Kingdom",
#         "CA": "Canada"
#     }
#     return mapping.get(code, "Unknown")

# def get_marketplace_by_name_or_channel(row):
#     """Get marketplace by name, or fallback to channel if name is missing"""
#     marketplace_name = safe_string(row.get('marketplace_name'))
#     sales_channel = safe_string(row.get('sales_channel'))
#     channel = safe_string(row.get('channel'))  # This is the Channel column from Excel (eBay, Tesco, etc.)
#     marketplace_url = safe_string(row.get('marketplace_url'))
#     marketplace_image_url = safe_string(row.get('marketplace_image_url'))
    
#     # Priority 1: Use Channel column (capital C) - this maps to marketplace names
#     if channel and channel != "Unknown":
#         search_name = normalize_channel(channel)
#     # Priority 2: marketplace_name
#     elif marketplace_name and marketplace_name != "Unknown":
#         search_name = marketplace_name
#     # Priority 3: sales_channel
#     elif sales_channel and sales_channel != "Unknown":
#         search_name = normalize_channel(sales_channel)
#     else:
#         search_name = "Unknown"
    
#     # Try to find existing marketplace - use only_fields to avoid loading invalid fields like 'country'
#     try:
#         marketplace = Marketplace.objects(name=search_name).only('id', 'name', 'url', 'image_url', 'created_at', 'updated_at').first()
#     except Exception as e:
#         print(f"⚠️  Error querying marketplace {search_name}: {e}")
#         marketplace = None
    
#     if not marketplace:
#         # Create new marketplace with only valid fields
#         marketplace_data = {
#             'name': search_name,
#             'created_at': str(datetime.now()),
#             'updated_at': str(datetime.now())
#         }
        
#         if marketplace_url:
#             marketplace_data['url'] = marketplace_url
#         if marketplace_image_url:
#             marketplace_data['image_url'] = marketplace_image_url
        
#         try:
#             marketplace = Marketplace(**marketplace_data).save()
#             print(f"✨ Created new marketplace: {search_name}")
#         except Exception as e:
#             print(f"⚠️  Error creating marketplace {search_name}: {e}")
#             # Try to get or create Unknown marketplace
#             try:
#                 marketplace = Marketplace.objects(name="Unknown").only('id', 'name', 'url', 'image_url', 'created_at', 'updated_at').first()
#             except:
#                 marketplace = None
                
#             if not marketplace:
#                 try:
#                     marketplace = Marketplace(
#                         name="Unknown",
#                         created_at=str(datetime.now()),
#                         updated_at=str(datetime.now())
#                     ).save()
#                 except Exception as e2:
#                     print(f"❌ Could not create Unknown marketplace: {e2}")
#                     return None
    
#     return marketplace

# def get_order_items_from_ids(order_item_ids_str):
#     """Extract OrderItem references from the order_item_ids column"""
#     if not order_item_ids_str or pd.isna(order_item_ids_str):
#         return []
    
#     order_items = []
#     id_strings = str(order_item_ids_str).split(',')
    
#     for id_str in id_strings:
#         id_str = id_str.strip()
#         if id_str:
#             try:
#                 order_item = OrderItems.objects(id=ObjectId(id_str)).first()
#                 if order_item:
#                     order_items.append(order_item)
#                 else:
#                     print(f"⚠️  OrderItem with ID {id_str} not found in DB")
#             except Exception as e:
#                 print(f"⚠️  Invalid ObjectId format: {id_str} - {e}")
    
#     return order_items

# # ======= IMPORT ORDERS =======
# print("\n" + "="*60)
# print("📦 IMPORTING ORDERS (OrderItems already in DB)")
# print("="*60)

# # Configuration for resuming
# SKIP_EXISTING_WITH_CHANNEL = True  # Set to True to skip orders that already have channel/geo set
# START_FROM_ROW = 0  # Set to row number to resume from (0 to start from beginning)

# df_orders = pd.read_excel(EXCEL_PATH, sheet_name="Orders")
# print(f"Loaded {len(df_orders)} Orders rows from Excel.")

# # Normalize column names
# df_orders.columns = [c.strip().replace(" ", "_").lower() for c in df_orders.columns]

# print(f"📋 Sample columns: {list(df_orders.columns[:15])}")

# if START_FROM_ROW > 0:
#     print(f"⏭️  Skipping first {START_FROM_ROW} rows (resuming from row {START_FROM_ROW})")
#     df_orders = df_orders.iloc[START_FROM_ROW:]

# orders_inserted = 0
# orders_updated = 0
# orders_failed = 0
# orders_skipped = 0
# orders_already_complete = 0

# for i, row in df_orders.iterrows():
#     try:
#         # Get the channel name first
#         channel_name = safe_string(row.get('channel'))  # Channel from Excel (eBay, Tesco, etc.)
#         if not channel_name:
#             channel_name = safe_string(row.get('sales_channel'))  # Fallback
        
#         # Normalize the channel name
#         normalized_channel = normalize_channel(channel_name) if channel_name else "Unknown"
        
#         # Get marketplace
#         marketplace = get_marketplace_by_name_or_channel(row)
        
#         if not marketplace:
#             print(f"⚠️  Row {i}: Could not get marketplace, skipping...")
#             orders_skipped += 1
#             continue

#         # Get existing OrderItems
#         order_items = get_order_items_from_ids(row.get('order_item_ids'))

#         # Build shipping information
#         shipping_info = {}
        
#         if safe_string(row.get('shipping_info_city')):
#             shipping_info["City"] = safe_string(row.get('shipping_info_city'))
#         if safe_string(row.get('shipping_info_stateorregion')):
#             shipping_info["StateOrRegion"] = safe_string(row.get('shipping_info_stateorregion'))
#         if safe_string(row.get('shipping_info_postalcode')):
#             shipping_info["PostalCode"] = safe_string(row.get('shipping_info_postalcode'))
#         if safe_string(row.get('shipping_info_countrycode')):
#             shipping_info["CountryCode"] = safe_string(row.get('shipping_info_countrycode'))
#             shipping_info["CountryName"] = normalize_country(row.get('shipping_info_countrycode'))
#         if safe_string(row.get('shipping_info_phone')):
#             shipping_info["Phone"] = safe_string(row.get('shipping_info_phone'))
#         if safe_string(row.get('shipping_info_carriermethodname')):
#             shipping_info["CarrierMethodName"] = safe_string(row.get('shipping_info_carriermethodname'))
#         if safe_string(row.get('shipping_info_methodcode')):
#             shipping_info["MethodCode"] = safe_string(row.get('shipping_info_methodcode'))
#         if safe_date(row.get('shipping_info_estimateddeliverydate')):
#             shipping_info["EstimatedDeliveryDate"] = safe_date(row.get('shipping_info_estimateddeliverydate'))
#         if safe_date(row.get('shipping_info_estimatedshipdate')):
#             shipping_info["EstimatedShipDate"] = safe_date(row.get('shipping_info_estimatedshipdate'))

#         # Create order data dictionary
#         order_data = {
#             'purchase_order_id': safe_string(row.get('purchase_order_id')),
#             'customer_order_id': safe_string(row.get('customer_order_id')),
#             'seller_order_id': safe_string(row.get('seller_order_id')),
#             'merchant_order_id': safe_string(row.get('merchant_order_id')),
#             'shipstation_id': safe_string(row.get('shipstation_id')),
#             'shipstation_synced': safe_bool(row.get('shipstation_synced')),
#             'shipstation_sync_date': safe_date(row.get('shipstation_sync_date')),
#             'shipping_rates_fetched': safe_bool(row.get('shipping_rates_fetched')),
#             'shipping_rates_date': safe_date(row.get('shipping_rates_date')),
#             'shipping_cost': safe_float(row.get('shipping_cost')),
#             'tracking_number': safe_string(row.get('tracking_number')),
#             'merchant_shipment_cost': safe_float(row.get('merchant_shipment_cost')),
#             'customer_email_id': safe_string(row.get('customer_email_id')),
#             'order_date': safe_date(row.get('order_date')),
#             'pacific_date': safe_date(row.get('pacific_date')),
#             'earliest_ship_date': safe_date(row.get('earliest_ship_date')),
#             'latest_ship_date': safe_date(row.get('latest_ship_date')),
#             'last_update_date': safe_date(row.get('last_update_date')),
#             'ship_service_level': safe_string(row.get('ship_service_level')),
#             'shipment_service_level_category': safe_string(row.get('shipment_service_level_category')),
#             'order_status': safe_string(row.get('order_status')),
#             'number_of_items_shipped': safe_int(row.get('number_of_items_shipped')),
#             'number_of_items_unshipped': safe_int(row.get('number_of_items_unshipped')),
#             'fulfillment_channel': safe_string(row.get('fulfillment_channel')),
#             'sales_channel': normalized_channel,  # Use the Channel from Excel, normalized
#             'geo': safe_string(row.get('geo')),  # Add Geo field
#             'channel': normalized_channel,  # Also save as channel field for consistency
#             'order_type': safe_string(row.get('order_type')),
#             'is_premium_order': safe_bool(row.get('is_premium_order')),
#             'is_prime': safe_bool(row.get('is_prime')),
#             'has_regulated_items': safe_bool(row.get('has_regulated_items')),
#             'is_replacement_order': safe_bool(row.get('is_replacement_order')),
#             'is_sold_by_ab': safe_bool(row.get('is_sold_by_ab')),
#             'is_ispu': safe_bool(row.get('is_ispu')),
#             'is_access_point_order': safe_bool(row.get('is_access_point_order')),
#             'is_business_order': safe_bool(row.get('is_business_order')),
#             'marketplace': safe_string(row.get('marketplace')),
#             'marketplace_id': marketplace,
#             'payment_method': safe_string(row.get('payment_method')),
#             'payment_method_details': safe_string(row.get('payment_method_details')),
#             'order_total': safe_float(row.get('order_total')),
#             'currency': safe_string(row.get('currency')),
#             'is_global_express_enabled': safe_bool(row.get('is_global_express_enabled')),
#             'customer_name': safe_string(row.get('customer_name')),
#             'order_channel': safe_string(row.get('order_channel')),
#             'items_order_quantity': safe_int(row.get('items_order_quantity')),
#             'shipping_price': safe_float(row.get('shipping_price')),
#             'shipping_information': shipping_info,
#             'order_items': order_items,
#             'updated_at': datetime.utcnow()
#         }
        
#         # Remove None values and empty strings
#         order_data = {k: v for k, v in order_data.items() if v not in [None, ""]}
        
#         # Check if order already exists
#         purchase_order_id = safe_string(row.get('purchase_order_id'))
#         if not purchase_order_id:
#             print(f"⚠️  Row {i}: Missing purchase_order_id, skipping...")
#             orders_skipped += 1
#             continue

#         existing = Order.objects(purchase_order_id=purchase_order_id).first()
        
#         if existing:
#             # Check if order already has channel and geo data (skip if configured)
#             if SKIP_EXISTING_WITH_CHANNEL and existing.channel and existing.geo:
#                 orders_already_complete += 1
#                 if orders_already_complete % 1000 == 0:
#                     print(f"⏭️  Skipped {orders_already_complete} orders that already have channel/geo data")
#                 continue
            
#             # Update existing order
#             updated_fields = []
#             for field, value in order_data.items():
#                 if field == 'id':
#                     continue
#                 if value not in [None, "", [], {}, 0, False]:
#                     try:
#                         setattr(existing, field, value)
#                         updated_fields.append(field)
#                     except Exception as field_error:
#                         print(f"⚠️  Could not set field '{field}' on order {purchase_order_id}: {field_error}")
            
#             existing.updated_at = datetime.utcnow()
            
#             try:
#                 existing.save()
#                 orders_updated += 1
#                 if (orders_updated + orders_inserted) % 1000 == 0:
#                     print(f"Progress: {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated, ⏭️  {orders_already_complete} already complete")
#             except Exception as save_error:
#                 print(f"❌ Failed to save updated order {purchase_order_id}: {save_error}")
#                 orders_failed += 1
#         else:
#             # Insert new order
#             try:
#                 order = Order(**order_data)
#                 order.save()
#                 orders_inserted += 1
#                 if (orders_updated + orders_inserted) % 1000 == 0:
#                     print(f"Progress: {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated, ⏭️  {orders_already_complete} already complete")
#             except Exception as save_error:
#                 print(f"❌ Failed to insert order {purchase_order_id}: {save_error}")
#                 print(f"   Error details: {save_error}")
#                 orders_failed += 1
#                 # Show traceback for first 3 failures
#                 if orders_failed <= 3:
#                     import traceback
#                     traceback.print_exc()

#     except Exception as e:
#         orders_failed += 1
#         print(f"❌ Order Row {i} failed: {e}")
#         print(f"   Purchase Order ID: {safe_string(row.get('purchase_order_id'))}")
        
#         # Show traceback for first 3 failures
#         if orders_failed <= 3:
#             import traceback
#             traceback.print_exc()

#     # Progress indicator
#     if (i + 1) % 5000 == 0:
#         print(f"Processed {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated, ❌ {orders_failed} failed, ⏭️  {orders_skipped} skipped")

# print(f"\n✅ Finished importing Orders:")
# print(f"   ✅ Inserted: {orders_inserted}")
# print(f"   🔄 Updated: {orders_updated}")
# print(f"   ❌ Failed: {orders_failed}")
# print(f"   ⏭️  Skipped: {orders_skipped}")

# # ======= FINAL VERIFICATION =======
# print("\n" + "="*60)
# print("🔍 FINAL DATABASE VERIFICATION")
# print("="*60)

# total_orders = Order.objects.count()
# total_items = OrderItems.objects.count()
# total_marketplaces = Marketplace.objects.count()

# print(f"📊 Final Counts:")
# print(f"   Orders: {total_orders}")
# print(f"   OrderItems: {total_items}")
# print(f"   Marketplaces: {total_marketplaces}")

# if total_orders > 0:
#     sample_order = Order.objects.first()
#     print(f"\n✅ Sample Order Found:")
#     print(f"   Purchase Order ID: {sample_order.purchase_order_id}")
#     print(f"   Order Date: {sample_order.order_date}")
#     print(f"   Marketplace: {sample_order.marketplace_id.name if sample_order.marketplace_id else 'None'}")
#     print(f"   Linked OrderItems: {len(sample_order.order_items)}")
    
#     # Check if OrderItems are properly linked
#     orders_with_items = Order.objects(order_items__exists=True, order_items__not__size=0).count()
#     print(f"\n📈 Orders with linked OrderItems: {orders_with_items}/{total_orders}")
# else:
#     print("\n❌ WARNING: No orders found in database after import!")

# print("\n🎉 IMPORT COMPLETE!")
# print("="*60)