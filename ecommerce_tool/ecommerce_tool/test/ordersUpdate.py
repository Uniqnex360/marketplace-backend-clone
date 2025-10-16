import pandas as pd
from datetime import datetime
from mongoengine import connect
import os
import sys
from bson import ObjectId


project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)   
print("📦 Added project root:", project_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from omnisight.models import Order, OrderItems, Marketplace, Product, ProductDetails, Pricing, Money, Fulfillment, OrderStatus, TaxCollection, BuyerInfo

EXCEL_PATH = "/home/lexicon/Downloads/orders_and_items_export_20251006_223508 (1) (2).xlsx"


DATABASE_HOST = 'mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0'
DATABASE_NAME = 'ecommerce_db'

print(f"🔗 Connecting to database: {DATABASE_NAME}")

try:
    connect(
        db=DATABASE_NAME,
        host=DATABASE_HOST,
        alias='default'
    )
    print("✅ Database connection established!")
    
    
    from mongoengine import connection
    db = connection.get_db()
    print(f"📊 Connected to database: {db.name}")
    print(f"📚 Existing OrderItems: {OrderItems.objects.count()}")
    print(f"📚 Existing Orders: {Order.objects.count()}")
    print(f"📚 Existing Products: {Product.objects.count()}")
    print(f"📚 Existing Marketplaces: {Marketplace.objects.count()}")
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)


def safe_float(v):
    try:
        if pd.isna(v):
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0

def safe_int(v):
    try:
        if pd.isna(v):
            return 0
        return int(v)
    except (TypeError, ValueError):
        return 0

def safe_bool(v):
    if pd.isna(v):
        return False
    return str(v).strip().lower() in ["true", "1", "yes", "y"]

def safe_string(v):
    if pd.isna(v) or v is None:
        return ""
    return str(v).strip()

def safe_date(v):
    if pd.notnull(v) and str(v).strip():
        try:
            return pd.to_datetime(v)
        except Exception:
            return None
    return None

def normalize_channel(channel):
    if not channel or not isinstance(channel, str):
        return "Unknown"
    channel = channel.strip()
    mapping = {
        "Amazon.com": "Amazon",
        "Amazon": "Amazon",
        "eBay": "eBay", 
        "TikTok": "TikTok",
        "Tesco": "Tesco"
    }
    return mapping.get(channel, channel)

def normalize_country(country_code):
    if not country_code or not isinstance(country_code, str):
        return "Unknown"
    code = country_code.strip().upper()
    mapping = {
        "US": "United States",
        "UK": "United Kingdom",
        "CA": "Canada"
    }
    return mapping.get(code, "Unknown")

def get_or_create_product(row):
    """Get product by ASIN, SKU, or title"""
    asin = safe_string(row.get('product_details_asin'))
    sku = safe_string(row.get('product_details_sku'))
    title = safe_string(row.get('product_details_title'))
    
    product = None
    
    # Try to find by ASIN first
    if asin:
        try:
            product = Product.objects(asin=asin).first()
        except:
            pass
    
    # Try to find by SKU
    if not product and sku:
        try:
            product = Product.objects(sku=sku).first()
        except:
            pass
    
    # If product not found, create a basic one
    if not product:
        try:
            product_data = {
                'product_title': title if title else 'Unknown Product',
                'sku': sku if sku else f'AUTO-SKU-{datetime.now().timestamp()}',
                'asin': asin if asin else None,
                'price': safe_float(row.get('product_price')),
                'currency': safe_string(row.get('product_currency')) or '$',
                'brand_name': safe_string(row.get('product_brand')),
                'manufacturer_name': safe_string(row.get('product_manufacturer')),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Remove None values
            product_data = {k: v for k, v in product_data.items() if v not in [None, ""]}
            
            product = Product(**product_data).save()
            print(f"✨ Created new product: {sku or asin or title[:30]}")
        except Exception as e:
            print(f"⚠️  Error creating product: {e}")
            return None
    
    return product

def get_marketplace_by_name_or_channel(row):
    """Get marketplace by name, or fallback to channel if name is missing"""
    marketplace_name = safe_string(row.get('marketplace_name'))
    # sales_channel = safe_string(row.get('sales_channel'))
    channel = safe_string(row.get('channel'))  
    marketplace_url = safe_string(row.get('marketplace_url'))
    marketplace_image_url = safe_string(row.get('marketplace_image_url'))
    
    search_name=normalize_channel(marketplace_name)
    # if channel and channel != "Unknown":
    #     search_name = normalize_channel(channel)
    
    # elif marketplace_name and marketplace_name != "Unknown":
    #     search_name = marketplace_name
    
    # elif sales_channel and sales_channel != "Unknown":
    #     search_name = normalize_channel(sales_channel)
    # else:
    #     search_name = "Unknown"
    
    
    try:
        marketplace = Marketplace.objects(name=search_name).only('id', 'name', 'url', 'image_url', 'created_at', 'updated_at').first()
    except Exception as e:
        print(f"⚠️  Error querying marketplace {search_name}: {e}")
        marketplace = None
    
    if not marketplace:
        
        marketplace_data = {
            'name': search_name,
            'created_at': str(datetime.now()),
            'updated_at': str(datetime.now())
        }
        
        if marketplace_url:
            marketplace_data['url'] = marketplace_url
        if marketplace_image_url:
            marketplace_data['image_url'] = marketplace_image_url
        
        try:
            marketplace = Marketplace(**marketplace_data).save()
            print(f"✨ Created new marketplace: {search_name}")
        except Exception as e:
            print(f"⚠️  Error creating marketplace {search_name}: {e}")
            
            try:
                marketplace = Marketplace.objects(name="Unknown").only('id', 'name', 'url', 'image_url', 'created_at', 'updated_at').first()
            except:
                marketplace = None
                
            if not marketplace:
                try:
                    marketplace = Marketplace(
                        name="Unknown",
                        created_at=str(datetime.now()),
                        updated_at=str(datetime.now())
                    ).save()
                except Exception as e2:
                    print(f"❌ Could not create Unknown marketplace: {e2}")
                    return None
    
    return marketplace

def get_order_items_from_ids(order_item_ids_str):
    """Extract OrderItem references from the order_item_ids column"""
    if not order_item_ids_str or pd.isna(order_item_ids_str):
        return []
    
    order_items = []
    id_strings = str(order_item_ids_str).split(',')
    
    for id_str in id_strings:
        id_str = id_str.strip()
        if id_str:
            try:
                order_item = OrderItems.objects(id=ObjectId(id_str)).first()
                if order_item:
                    order_items.append(order_item)
                else:
                    print(f"⚠️  OrderItem with ID {id_str} not found in DB")
            except Exception as e:
                print(f"⚠️  Invalid ObjectId format: {id_str} - {e}")
    
    return order_items

# ======================================================================
# PHASE 1: IMPORT ORDER ITEMS FIRST
# ======================================================================
print("\n" + "="*60)
print("PHASE 1: IMPORTING ORDER ITEMS")
print("="*60)

df_items = pd.read_excel(EXCEL_PATH, sheet_name="OrderItems")
print(f"Loaded {len(df_items)} OrderItems rows from Excel.")

# Normalize column names
df_items.columns = [c.strip().replace(" ", "_").lower() for c in df_items.columns]

print(f"📋 OrderItems columns: {list(df_items.columns[:15])}")

items_inserted = 0
items_updated = 0
items_failed = 0
items_skipped = 0

for i, row in df_items.iterrows():
    try:
        # Get or create product
        product = get_or_create_product(row)
        
        if not product:
            items_skipped += 1
            continue

        # Build ProductDetails embedded document
        product_details = ProductDetails(
            product_id=product,
            Title=safe_string(row.get('product_details_title')) or 'Unknown',
            SKU=safe_string(row.get('product_details_sku')) or 'Unknown',
            ASIN=safe_string(row.get('product_details_asin')),
            Condition=safe_string(row.get('product_details_condition')),
            QuantityOrdered=safe_int(row.get('product_details_quantityordered')),
            QuantityShipped=safe_int(row.get('product_details_quantityshipped'))
        )

        # Build Pricing embedded document
        item_price = Money(
            CurrencyCode=safe_string(row.get('pricing_itemprice_currencycode')) or 'USD',
            Amount=safe_float(row.get('pricing_itemprice_amount'))
        )
        
        item_tax = None
        if safe_float(row.get('pricing_itemtax_amount')) > 0:
            item_tax = Money(
                CurrencyCode=safe_string(row.get('pricing_itemtax_currencycode')) or 'USD',
                Amount=safe_float(row.get('pricing_itemtax_amount'))
            )
        
        promo_discount = None
        if safe_float(row.get('pricing_promotiondiscount_amount')) > 0:
            promo_discount = Money(
                CurrencyCode=safe_string(row.get('pricing_promotiondiscount_currencycode')) or 'USD',
                Amount=safe_float(row.get('pricing_promotiondiscount_amount'))
            )
        
        ship_promo_discount = None
        if safe_float(row.get('pricing_shippromotiondiscount_amount')) > 0:
            ship_promo_discount = Money(
                CurrencyCode=safe_string(row.get('pricing_shippromotiondiscount_currencycode')) or 'USD',
                Amount=safe_float(row.get('pricing_shippromotiondiscount_amount'))
            )
        
        pricing = Pricing(
            ItemPrice=item_price,
            ItemTax=item_tax,
            PromotionDiscount=promo_discount,
            ShipPromotionDiscount=ship_promo_discount
        )

        # Build Fulfillment embedded document
        fulfillment = None
        if safe_string(row.get('fulfillment_carrier')) or safe_string(row.get('fulfillment_trackingnumber')):
            fulfillment = Fulfillment(
                FulfillmentOption=safe_string(row.get('fulfillment_fulfillmentoption')),
                ShipMethod=safe_string(row.get('fulfillment_shipmethod')),
                Carrier=safe_string(row.get('fulfillment_carrier')),
                TrackingNumber=safe_string(row.get('fulfillment_trackingnumber')),
                TrackingURL=safe_string(row.get('fulfillment_trackingurl')),
                ShipDateTime=safe_date(row.get('fulfillment_shipdatetime'))
            )

        # Build OrderStatus embedded document
        order_status = None
        if safe_string(row.get('order_status_status')):
            order_status = OrderStatus(
                Status=safe_string(row.get('order_status_status')),
                StatusDate=safe_date(row.get('order_status_statusdate')) or datetime.now()
            )

        # Build TaxCollection embedded document
        tax_collection = TaxCollection(
            Model=safe_string(row.get('tax_collection_model')) or 'Unknown',
            ResponsibleParty=safe_string(row.get('tax_collection_responsibleparty')) or 'Unknown'
        )

        # Create OrderItem data dictionary
        order_item_data = {
            'OrderId': safe_string(row.get('order_id')),
            'Platform': safe_string(row.get('platform')),
            'ProductDetails': product_details,
            'Pricing': pricing,
            'Fulfillment': fulfillment,
            'OrderStatus': order_status,
            'TaxCollection': tax_collection,
            'IsGift': safe_bool(row.get('is_gift')),
            'BuyerInfo': None,
            'created_date': safe_date(row.get('created_date')) or datetime.now(),
            'document_created_date': safe_date(row.get('document_created_date')),
            'PromotionDiscount': safe_float(row.get('promotion_discount')),
            'net_profit': safe_float(row.get('net_profit')),
            'tax_checked': safe_bool(row.get('tax_checked')),
            'pricing_checked': safe_bool(row.get('pricing_checked')),
            'updated_at': datetime.utcnow()
        }
        
        # Remove None values
        order_item_data = {k: v for k, v in order_item_data.items() if v is not None}
        
        # Check if OrderItem already exists by order_item_id from Excel
        order_item_id = safe_string(row.get('order_item_id'))
        
        existing = None
        if order_item_id:
            try:
                existing = OrderItems.objects(id=ObjectId(order_item_id)).first()
            except:
                pass
        
        if existing:
            # Update existing OrderItem
            for field, value in order_item_data.items():
                if field != 'id':
                    try:
                        setattr(existing, field, value)
                    except:
                        pass
            
            existing.updated_at = datetime.utcnow()
            existing.save()
            items_updated += 1
            if (items_updated + items_inserted) % 1000 == 0:
                print(f"OrderItems Progress: {i+1}/{len(df_items)} | ✅ {items_inserted} inserted, 🔄 {items_updated} updated")
        else:
            # Insert new OrderItem with specific ID
            try:
                if order_item_id:
                    try:
                        oid = ObjectId(order_item_id)
                        
                        # Prepare document for MongoDB
                        doc = order_item_data.copy()
                        doc['_id'] = oid
                        
                        # Convert embedded documents to dicts
                        if 'ProductDetails' in doc:
                            doc['ProductDetails'] = doc['ProductDetails'].to_mongo()
                        if 'Pricing' in doc:
                            doc['Pricing'] = doc['Pricing'].to_mongo()
                        if 'Fulfillment' in doc and doc['Fulfillment']:
                            doc['Fulfillment'] = doc['Fulfillment'].to_mongo()
                        if 'OrderStatus' in doc and doc['OrderStatus']:
                            doc['OrderStatus'] = doc['OrderStatus'].to_mongo()
                        if 'TaxCollection' in doc:
                            doc['TaxCollection'] = doc['TaxCollection'].to_mongo()
                        if 'BuyerInfo' in doc and doc['BuyerInfo']:
                            doc['BuyerInfo'] = doc['BuyerInfo'].to_mongo()
                        
                        # Insert directly with specific _id
                        db.order_items.insert_one(doc)
                        items_inserted += 1
                        
                    except Exception as id_error:
                        # Fallback to auto-generated ID
                        order_item = OrderItems(**order_item_data)
                        order_item.save()
                        items_inserted += 1
                else:
                    order_item = OrderItems(**order_item_data)
                    order_item.save()
                    items_inserted += 1
                
                if (items_updated + items_inserted) % 1000 == 0:
                    print(f"OrderItems Progress: {i+1}/{len(df_items)} | ✅ {items_inserted} inserted, 🔄 {items_updated} updated")
            except Exception as save_error:
                items_failed += 1
                if items_failed <= 3:
                    print(f"❌ Failed to insert OrderItem: {save_error}")

    except Exception as e:
        items_failed += 1
        if items_failed <= 3:
            print(f"❌ OrderItem Row {i} failed: {e}")

    # Progress indicator
    if (i + 1) % 5000 == 0:
        print(f"OrderItems Processed {i+1}/{len(df_items)} | ✅ {items_inserted} inserted, 🔄 {items_updated} updated, ❌ {items_failed} failed")

print(f"\n✅ Finished importing OrderItems:")
print(f"   ✅ Inserted: {items_inserted}")
print(f"   🔄 Updated: {items_updated}")
print(f"   ❌ Failed: {items_failed}")
print(f"   ⏭️  Skipped: {items_skipped}")

# ======================================================================
# PHASE 2: IMPORT ORDERS
# ======================================================================
print("\n" + "="*60)
print("PHASE 2: IMPORTING ORDERS")
print("="*60)

SKIP_EXISTING_WITH_CHANNEL = True  
START_FROM_ROW = 0  

df_orders = pd.read_excel(EXCEL_PATH, sheet_name="Orders")
print(f"Loaded {len(df_orders)} Orders rows from Excel.")

df_orders.columns = [c.strip().replace(" ", "_").lower() for c in df_orders.columns]

print(f"📋 Orders columns: {list(df_orders.columns[:15])}")

if START_FROM_ROW > 0:
    print(f"⏭️  Skipping first {START_FROM_ROW} rows (resuming from row {START_FROM_ROW})")
    df_orders = df_orders.iloc[START_FROM_ROW:]

orders_inserted = 0
orders_updated = 0
orders_failed = 0
orders_skipped = 0
orders_already_complete = 0

for i, row in df_orders.iterrows():
    try:
        
        channel_name = safe_string(row.get('channel'))  
        if not channel_name:
            channel_name = safe_string(row.get('marketplace_name'))  
        
        
        normalized_channel = normalize_channel(channel_name) if channel_name else "Unknown"
        
        
        marketplace = get_marketplace_by_name_or_channel(row)
        
        if not marketplace:
            orders_skipped += 1
            continue

        
        order_items = get_order_items_from_ids(row.get('order_item_ids'))

        
        shipping_info = {}
        
        if safe_string(row.get('shipping_info_city')):
            shipping_info["City"] = safe_string(row.get('shipping_info_city'))
        if safe_string(row.get('shipping_info_stateorregion')):
            shipping_info["StateOrRegion"] = safe_string(row.get('shipping_info_stateorregion'))
        if safe_string(row.get('shipping_info_postalcode')):
            shipping_info["PostalCode"] = safe_string(row.get('shipping_info_postalcode'))
        if safe_string(row.get('shipping_info_countrycode')):
            shipping_info["CountryCode"] = safe_string(row.get('shipping_info_countrycode'))
            shipping_info["CountryName"] = normalize_country(row.get('shipping_info_countrycode'))
        if safe_string(row.get('shipping_info_phone')):
            shipping_info["Phone"] = safe_string(row.get('shipping_info_phone'))
        if safe_string(row.get('shipping_info_carriermethodname')):
            shipping_info["CarrierMethodName"] = safe_string(row.get('shipping_info_carriermethodname'))
        if safe_string(row.get('shipping_info_methodcode')):
            shipping_info["MethodCode"] = safe_string(row.get('shipping_info_methodcode'))
        if safe_date(row.get('shipping_info_estimateddeliverydate')):
            shipping_info["EstimatedDeliveryDate"] = safe_date(row.get('shipping_info_estimateddeliverydate'))
        if safe_date(row.get('shipping_info_estimatedshipdate')):
            shipping_info["EstimatedShipDate"] = safe_date(row.get('shipping_info_estimatedshipdate'))

        
        order_data = {
            'purchase_order_id': safe_string(row.get('purchase_order_id')),
            'customer_order_id': safe_string(row.get('customer_order_id')),
            'seller_order_id': safe_string(row.get('seller_order_id')),
            'merchant_order_id': safe_string(row.get('merchant_order_id')),
            'shipstation_id': safe_string(row.get('shipstation_id')),
            'shipstation_synced': safe_bool(row.get('shipstation_synced')),
            'shipstation_sync_date': safe_date(row.get('shipstation_sync_date')),
            'shipping_rates_fetched': safe_bool(row.get('shipping_rates_fetched')),
            'shipping_rates_date': safe_date(row.get('shipping_rates_date')),
            'shipping_cost': safe_float(row.get('shipping_cost')),
            'tracking_number': safe_string(row.get('tracking_number')),
            'merchant_shipment_cost': safe_float(row.get('merchant_shipment_cost')),
            'customer_email_id': safe_string(row.get('customer_email_id')),
            'order_date': safe_date(row.get('order_date')),
            'pacific_date': safe_date(row.get('pacific_date')),
            'earliest_ship_date': safe_date(row.get('earliest_ship_date')),
            'latest_ship_date': safe_date(row.get('latest_ship_date')),
            'last_update_date': safe_date(row.get('last_update_date')),
            'ship_service_level': safe_string(row.get('ship_service_level')),
            'shipment_service_level_category': safe_string(row.get('shipment_service_level_category')),
            'order_status': safe_string(row.get('order_status')),
            'number_of_items_shipped': safe_int(row.get('number_of_items_shipped')),
            'number_of_items_unshipped': safe_int(row.get('number_of_items_unshipped')),
            'fulfillment_channel': safe_string(row.get('fulfillment_channel')),
            'sales_channel': normalized_channel,  
            'geo': safe_string(row.get('geo')),  
            'channel': normalized_channel,  
            'order_type': safe_string(row.get('order_type')),
            'is_premium_order': safe_bool(row.get('is_premium_order')),
            'is_prime': safe_bool(row.get('is_prime')),
            'has_regulated_items': safe_bool(row.get('has_regulated_items')),
            'is_replacement_order': safe_bool(row.get('is_replacement_order')),
            'is_sold_by_ab': safe_bool(row.get('is_sold_by_ab')),
            'is_ispu': safe_bool(row.get('is_ispu')),
            'is_access_point_order': safe_bool(row.get('is_access_point_order')),
            'is_business_order': safe_bool(row.get('is_business_order')),
            'marketplace': safe_string(row.get('marketplace')),
            'marketplace_id': marketplace,
            'payment_method': safe_string(row.get('payment_method')),
            'payment_method_details': safe_string(row.get('payment_method_details')),
            'order_total': safe_float(row.get('order_total')),
            'currency': safe_string(row.get('currency')),
            'is_global_express_enabled': safe_bool(row.get('is_global_express_enabled')),
            'customer_name': safe_string(row.get('customer_name')),
            'order_channel': safe_string(row.get('order_channel')),
            'items_order_quantity': safe_int(row.get('items_order_quantity')),
            'shipping_price': safe_float(row.get('shipping_price')),
            'shipping_information': shipping_info,
            'order_items': order_items,
            'updated_at': datetime.utcnow()
        }
        
        
        order_data = {k: v for k, v in order_data.items() if v not in [None, ""]}
        
        
        purchase_order_id = safe_string(row.get('purchase_order_id'))
        if not purchase_order_id:
            orders_skipped += 1
            continue

        existing = Order.objects(purchase_order_id=purchase_order_id).first()
        
        if existing:
            
            if SKIP_EXISTING_WITH_CHANNEL and existing.channel and existing.geo:
                orders_already_complete += 1
                if orders_already_complete % 1000 == 0:
                    print(f"⏭️  Skipped {orders_already_complete} orders that already have channel/geo data")
                continue
            
            
            for field, value in order_data.items():
                if field != 'id' and value not in [None, "", [], {}, 0, False]:
                    try:
                        setattr(existing, field, value)
                    except:
                        pass
            
            existing.updated_at = datetime.utcnow()
            
            try:
                existing.save()
                orders_updated += 1
                if (orders_updated + orders_inserted) % 1000 == 0:
                    print(f"Orders Progress: {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated")
            except Exception as save_error:
                orders_failed += 1
        else:
            
            try:
                order = Order(**order_data)
                order.save()
                orders_inserted += 1
                if (orders_updated + orders_inserted) % 1000 == 0:
                    print(f"Orders Progress: {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated")
            except Exception as save_error:
                orders_failed += 1
                if orders_failed <= 3:
                    import traceback
                    traceback.print_exc()

    except Exception as e:
        orders_failed += 1
        if orders_failed <= 3:
            import traceback
            traceback.print_exc()

    
    if (i + 1) % 5000 == 0:
        print(f"Orders Processed {i+1}/{len(df_orders)} | ✅ {orders_inserted} inserted, 🔄 {orders_updated} updated, ❌ {orders_failed} failed")

print(f"\n✅ Finished importing Orders:")
print(f"   ✅ Inserted: {orders_inserted}")
print(f"   🔄 Updated: {orders_updated}")
print(f"   ❌ Failed: {orders_failed}")
print(f"   ⏭️  Skipped: {orders_skipped}")


print("\n" + "="*60)
print("🔍 FINAL DATABASE VERIFICATION")
print("="*60)

total_orders = Order.objects.count()
total_items = OrderItems.objects.count()
total_products = Product.objects.count()
total_marketplaces = Marketplace.objects.count()

print(f"📊 Final Counts:")
print(f"   Orders: {total_orders}")
print(f"   OrderItems: {total_items}")
print(f"   Products: {total_products}")
print(f"   Marketplaces: {total_marketplaces}")

if total_items > 0:
    sample_item = OrderItems.objects.first()
    print(f"\n✅ Sample OrderItem:")
    print(f"   ID: {str(sample_item.id)}")
    print(f"   OrderId: {sample_item.OrderId}")
    print(f"   SKU: {sample_item.ProductDetails.SKU}")

if total_orders > 0:
    sample_order = Order.objects.first()
    print(f"\n✅ Sample Order:")
    print(f"   Purchase Order ID: {sample_order.purchase_order_id}")
    print(f"   Order Date: {sample_order.order_date}")
    print(f"   Channel: {sample_order.channel}")
    print(f"   Geo: {sample_order.geo}")
    print(f"   Marketplace: {sample_order.marketplace_id.name if sample_order.marketplace_id else 'None'}")
    print(f"   Linked OrderItems: {len(sample_order.order_items)}")