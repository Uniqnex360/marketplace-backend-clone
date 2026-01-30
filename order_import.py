"""
Order Import Script
This script imports orders from Excel and creates/updates them in the database
Supports multi-marketplace orders with all order items
"""
import os
import sys

# Get the base directory - MUST BE BEFORE OTHER IMPORTS
base_dir = os.path.dirname(os.path.abspath(__file__))

# Add both paths
sys.path.insert(0, base_dir)
sys.path.insert(0, os.path.join(base_dir, 'ecommerce_tool'))
import pandas as pd
from datetime import datetime
from mongoengine import connect, DoesNotExist
from decimal import Decimal
import re

from omnisight.models import Marketplace, Order, Product

# Import your models
# from ecommerce_tool.models import Order, OrderItems, Product, Marketplace, ProductDetails, Pricing, Money, OrderStatus, Fulfillment

class OrderImporter:
    def __init__(self):
        """Initialize the order importer"""
        self.marketplaces = {}  # Cache marketplaces
        self.products = {}  # Cache products
        self.stats = {
            'total_rows': 0,
            'orders_created': 0,
            'orders_updated': 0,
            'order_items_created': 0,
            'skipped': 0,
            'errors': 0,
            'error_details': []
        }
    
    def connect_db(self, connection_string=None, db_name=None, host='localhost', port=27017):
        """
        Connect to MongoDB database
        
        Args:
            connection_string: Full MongoDB connection string (for Atlas or remote)
            db_name: Database name (used with host/port for local connections)
            host: MongoDB host (default: localhost)
            port: MongoDB port (default: 27017)
        """
        try:
            if connection_string:
                connect(host=connection_string)
                print(f"✓ Connected to MongoDB Atlas")
            else:
                connect(db_name, host=host, port=port)
                print(f"✓ Connected to database: {db_name}")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {str(e)}")
            return False
    
    def get_marketplace(self, marketplace_name):
        """Get marketplace by name"""
        
        if not marketplace_name or pd.isna(marketplace_name):
            return None
        
        marketplace_name = str(marketplace_name).strip()
        
        # Check cache first
        if marketplace_name in self.marketplaces:
            return self.marketplaces[marketplace_name]
        
        try:
            marketplace = Marketplace.objects(name=marketplace_name).first()
            if marketplace:
                self.marketplaces[marketplace_name] = marketplace
                return marketplace
            else:
                print(f"Warning: Marketplace '{marketplace_name}' not found in database")
                return None
        except Exception as e:
            print(f"Error getting marketplace: {str(e)}")
            return None
    def get_or_create_marketplace(self, marketplace_name):
        from omnisight.models import Marketplace
        
        if not marketplace_name or pd.isna(marketplace_name):
            return None
        
        marketplace_name = str(marketplace_name).strip()
        
        # Check cache first
        if marketplace_name in self.marketplaces:
            return self.marketplaces[marketplace_name]
        
        try:
            marketplace = Marketplace.objects(name=marketplace_name).first()
            if not marketplace:
                # Create new marketplace
                country_code = 'AE' if 'ae' in marketplace_name.lower() else 'US'
                marketplace = Marketplace(
                    name=marketplace_name,
                    url=f"https://www.{marketplace_name.lower()}",
                    created_at=datetime.now().isoformat(),
                    updated_at=datetime.now().isoformat(),
                    country=[country_code]
                )
                marketplace.save()
                print(f"✓ Created marketplace: {marketplace_name}")
            
            self.marketplaces[marketplace_name] = marketplace
            return marketplace
            
        except Exception as e:
            print(f"Error with marketplace: {str(e)}")
            return None
    def get_product(self, sku=None, asin=None, marketplace=None):
        from omnisight.models import Product
        
        cache_key = f"{sku}_{asin}"
        
        if cache_key in self.products:
            return self.products[cache_key]
        
        try:
            # Try to find by SKU first (ignore marketplace)
            if sku:
                product = Product.objects(sku=sku).first()
                if product:
                    self.products[cache_key] = product
                    return product
            
            # Try by ASIN
            if asin:
                product = Product.objects(asin=asin).first()
                if product:
                    self.products[cache_key] = product
                    return product
            
            return None
            
        except Exception as e:
            print(f"Error getting product: {str(e)}")
            return None
    
    def clean_price(self, value):
        """Clean price values"""
        if pd.isna(value) or value == '' or value is None:
            return 0.0
        
        value_str = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '').strip()
        
        try:
            return float(value_str)
        except ValueError:
            return 0.0
    
    def clean_int(self, value):
        """Clean integer values"""
        if pd.isna(value) or value == '' or value is None:
            return 0
        
        try:
            return int(float(value))
        except ValueError:
            return 0
    
    def parse_date(self, date_value):
        """Parse date from various formats"""
        if pd.isna(date_value) or date_value == '':
            return datetime.now()
        
        try:
            if isinstance(date_value, datetime):
                return date_value
            
            # Try different date formats
            date_str = str(date_value).strip()
            
            # Try pandas to_datetime
            return pd.to_datetime(date_str)
            
        except Exception as e:
            print(f"Warning: Could not parse date '{date_value}': {str(e)}")
            return datetime.now()
    
    def clean_string(self, value):
        """Clean string values"""
        if pd.isna(value) or value == '':
            return ''
        return str(value).strip()
    
    def parse_order_row(self, row):
        """Parse a row from Excel into order data"""
        try:
            data = {
                'order_id': self.clean_string(row.get('Order ID')),
                'customer_id': self.clean_string(row.get('Customer ID')),
                'order_date': self.parse_date(row.get('Order Date')),
                'sku': self.clean_string(row.get('SKU')),
                'product_name': self.clean_string(row.get('Product Name')),
                'quantity': self.clean_int(row.get('Quantity')),
                'status': self.clean_string(row.get('Status')),
                'product_cost': self.clean_price(row.get('Product Cost (Procurement Price)')),
                'ship_cost': self.clean_price(row.get('Ship Cost (Merchant Cost)')),
                'product_price': self.clean_price(row.get('Product Price (Customer Price)')),
                'shipping_cost': self.clean_price(row.get('Shipping Cost (Taken from ShipStation)')),
                'funding': self.clean_price(row.get('Funding')),
                'referral_fee': self.clean_price(row.get('Referral Fee')),
                'tax': self.clean_price(row.get('Tax')),
                'line_total': self.clean_price(row.get('Line Total')),
                'line_product_cost_total': self.clean_price(row.get('Line Product Cost Total')),
                'line_referral_fee_total': self.clean_price(row.get('Line Referral Fee Total')),
                'line_vendor_funding_total': self.clean_price(row.get('Line Vendor Funding Total')),
                'asin': self.clean_string(row.get('ASIN')),
                'brand_name': self.clean_string(row.get('Brand Name')),
                'marketplace_name': self.clean_string(row.get('Marketplace')),
                'fulfillment_channel': self.clean_string(row.get('Fulfillment Channel')),
                'customer_name': self.clean_string(row.get('Customer Name')),
                'customer_email': self.clean_string(row.get('Customer Email')),
                'promotion_discount': self.clean_price(row.get('Promotion Discount')),
                'ship_promotion_discount': self.clean_price(row.get('Ship-promotion Discount')),
            }
            
            # Validate required fields
            if not data['order_id']:
                return None, "Missing Order ID"
            
            if not data['sku']:
                return None, "Missing SKU"
            
            return data, None
            
        except Exception as e:
            return None, f"Error parsing row: {str(e)}"
    
    def create_order_item(self, order_data, product, marketplace):
        """Create an order item"""
        from omnisight.models import (
            OrderItems, ProductDetails, Pricing, Money, 
            OrderStatus, Fulfillment
        )
        
        try:
            # Create ProductDetails
            product_details = ProductDetails(
                product_id=product if product else None,
                Title=order_data['product_name'],
                SKU=order_data['sku'],
                ASIN=order_data['asin'] if order_data['asin'] else None,
                Condition="New",
                QuantityOrdered=order_data['quantity'],
                QuantityShipped=order_data['quantity'] if order_data['status'].lower() == 'shipped' else 0
            )
            
            # Create Pricing with Money objects
            item_price = Money(
                CurrencyCode="USD",
                Amount=order_data['product_price']
            )
            
            item_tax = Money(
                CurrencyCode="USD",
                Amount=order_data['tax']
            ) if order_data['tax'] > 0 else None
            
            promotion_discount = Money(
                CurrencyCode="USD",
                Amount=order_data['promotion_discount']
            ) if order_data['promotion_discount'] > 0 else None
            
            ship_promotion_discount = Money(
                CurrencyCode="USD",
                Amount=order_data['ship_promotion_discount']
            ) if order_data['ship_promotion_discount'] > 0 else None
            
            pricing = Pricing(
                ItemPrice=item_price,
                ItemTax=item_tax,
                PromotionDiscount=promotion_discount,
                ShipPromotionDiscount=ship_promotion_discount
            )
            
            # Create Fulfillment
            fulfillment = Fulfillment(
                FulfillmentOption=order_data['fulfillment_channel'],
                ShipMethod="Standard" if not order_data['fulfillment_channel'] else order_data['fulfillment_channel']
            )
            
            # Create OrderStatus
            order_status = OrderStatus(
                Status=order_data['status'] if order_data['status'] else "Pending",
                StatusDate=order_data['order_date']
            )
            
            # Calculate net profit
            net_profit = (
                order_data['line_total'] - 
                order_data['line_product_cost_total'] - 
                order_data['line_referral_fee_total'] - 
                order_data['shipping_cost'] +
                order_data['line_vendor_funding_total']
            )
            
            # Create OrderItem
            order_item = OrderItems(
                OrderId=order_data['order_id'],
                Platform=marketplace.name if marketplace else order_data['marketplace_name'],
                ProductDetails=product_details,
                Pricing=pricing,
                Fulfillment=fulfillment,
                OrderStatus=order_status,
                IsGift=False,
                created_date=order_data['order_date'],
                document_created_date=datetime.now(),
                PromotionDiscount=order_data['promotion_discount'],
                net_profit=net_profit,
                tax_checked=True,
                pricing_checked=True
            )
            
            order_item.save()
            return order_item
            
        except Exception as e:
            raise Exception(f"Error creating order item: {str(e)}")
    
    def create_or_update_order(self, order_data, order_items_list, marketplace):
        """Create or update an order"""
        
        try:
            # Check if order exists
            existing_order = Order.objects(
                purchase_order_id=order_data['order_id']
            ).first()
            
            # Calculate totals
            total_items = sum([item.ProductDetails.QuantityOrdered for item in order_items_list])
            total_amount = sum([
                item.Pricing.ItemPrice.Amount * item.ProductDetails.QuantityOrdered 
                for item in order_items_list
            ])
            
            order_dict = {
                'purchase_order_id': order_data['order_id'],
                'customer_order_id': order_data['customer_id'],
                'customer_email_id': order_data['customer_email'],
                'customer_name': order_data['customer_name'],
                'order_date': order_data['order_date'],
                'order_status': order_data['status'],
                'fulfillment_channel': order_data['fulfillment_channel'],
                'marketplace': marketplace.name if marketplace else order_data['marketplace_name'],
                'marketplace_id': marketplace if marketplace else None,
                'order_items': order_items_list,
                'items_order_quantity': total_items,
                'order_total': total_amount,
                'currency': 'USD',
                'number_of_items_shipped': total_items if order_data['status'].lower() == 'shipped' else 0,
                'number_of_items_unshipped': 0 if order_data['status'].lower() == 'shipped' else total_items,
                'last_update_date': datetime.now(),
                'shipping_price': order_data['shipping_cost'],
                'updated_at': datetime.now()
            }
            
            if existing_order:
                # Update existing order
                for key, value in order_dict.items():
                    setattr(existing_order, key, value)
                existing_order.save()
                return 'updated', existing_order
            else:
                # Create new order
                new_order = Order(**order_dict)
                new_order.save()
                return 'created', new_order
                
        except Exception as e:
            raise Exception(f"Error creating/updating order: {str(e)}")
    
    def import_from_excel(self, file_path, sheet_name=0):
        """
        Import orders from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index (default: 0 for first sheet)
        """
        print(f"\n{'='*70}")
        print(f"Starting Order Import from: {file_path}")
        print(f"{'='*70}\n")
        
        try:
            # Read Excel file
            print("Reading Excel file...")
            # df = pd.read_excel(file_path, sheet_name=sheet_name)
            df = pd.read_csv(file_path)
            self.stats['total_rows'] = len(df)
            print(f"✓ Found {len(df)} rows in Excel file\n")
            
            print(f"\nProcessing orders...")
            print("-" * 70)
            
            # Group by Order ID to handle multi-item orders
            orders_dict = {}
            
            # First pass: Parse all rows and group by order ID
            for index, row in df.iterrows():
                try:
                    # Parse order data
                    order_data, error = self.parse_order_row(row)
                    
                    if error:
                        self.stats['skipped'] += 1
                        self.stats['error_details'].append({
                            'row': index + 2,
                            'error': error
                        })
                        print(f"⚠ Row {index + 2}: SKIPPED - {error}")
                        continue
                    
                    order_id = order_data['order_id']
                    
                    # Initialize order if not exists
                    if order_id not in orders_dict:
                        orders_dict[order_id] = {
                            'order_data': order_data,
                            'items': [],
                            'rows': []
                        }
                    
                    # Add item to order
                    orders_dict[order_id]['items'].append(order_data)
                    orders_dict[order_id]['rows'].append(index + 2)
                    
                except Exception as e:
                    self.stats['errors'] += 1
                    error_msg = str(e)
                    self.stats['error_details'].append({
                        'row': index + 2,
                        'error': error_msg
                    })
                    print(f"✗ Row {index + 2}: ERROR - {error_msg}")
            
            # Second pass: Create orders and order items
            for order_id, order_info in orders_dict.items():
                try:
                    first_item = order_info['items'][0]
                    
                    # Get marketplace
                    marketplace = self.get_or_create_marketplace(first_item['marketplace_name'])
                    if not marketplace:
                        print(f"⚠ Warning: Creating order without marketplace reference for {order_id}")
                    
                    # Create order items
                    order_items_list = []
                    for item_data in order_info['items']:
                        # Get product
                        product = self.get_product(
                            sku=item_data['sku'],
                            asin=item_data['asin'],
                            marketplace=marketplace
                        )
                        
                        if not product:
                            print(f"  ⚠ Warning: Product not found for SKU {item_data['sku']}")
                        
                        # Create order item
                        order_item = self.create_order_item(item_data, product, marketplace)
                        order_items_list.append(order_item)
                        self.stats['order_items_created'] += 1
                    
                    # Create or update order
                    action, order = self.create_or_update_order(
                        first_item,
                        order_items_list,
                        marketplace
                    )
                    
                    if action == 'created':
                        self.stats['orders_created'] += 1
                        print(f"✓ Order {order_id}: CREATED with {len(order_items_list)} item(s)")
                    elif action == 'updated':
                        self.stats['orders_updated'] += 1
                        print(f"✓ Order {order_id}: UPDATED with {len(order_items_list)} item(s)")
                    
                except Exception as e:
                    self.stats['errors'] += 1
                    error_msg = str(e)
                    self.stats['error_details'].append({
                        'order_id': order_id,
                        'error': error_msg
                    })
                    print(f"✗ Order {order_id}: ERROR - {error_msg}")
            
            # Print summary
            self.print_summary()
            return True
            
        except FileNotFoundError:
            print(f"✗ Error: File not found - {file_path}")
            return False
        except Exception as e:
            print(f"✗ Error reading Excel file: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_summary(self):
        """Print import summary"""
        print(f"\n{'='*70}")
        print("ORDER IMPORT SUMMARY")
        print(f"{'='*70}")
        print(f"Total Rows:              {self.stats['total_rows']}")
        print(f"Orders Created:          {self.stats['orders_created']}")
        print(f"Orders Updated:          {self.stats['orders_updated']}")
        print(f"Order Items Created:     {self.stats['order_items_created']}")
        print(f"Skipped:                 {self.stats['skipped']}")
        print(f"Errors:                  {self.stats['errors']}")
        print(f"{'='*70}")
        
        if self.stats['error_details']:
            print(f"\nError Details:")
            print("-" * 70)
            for detail in self.stats['error_details'][:10]:
                if 'row' in detail:
                    print(f"Row {detail['row']}: {detail['error']}")
                elif 'order_id' in detail:
                    print(f"Order {detail['order_id']}: {detail['error']}")
            
            if len(self.stats['error_details']) > 10:
                print(f"\n... and {len(self.stats['error_details']) - 10} more errors")
        
        print()


def main():
    """Main function to run the import"""
    # Configuration
    CONNECTION_STRING = "mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0"
    EXCEL_FILE = "/home/lexicon/Downloads/BigTree-orders_converted.csv"
    
    # Create importer instance
    importer = OrderImporter()
    
    # Connect to database
    if not importer.connect_db(connection_string=CONNECTION_STRING):
        return
    
    # Import orders
    importer.import_from_excel(EXCEL_FILE)


if __name__ == "__main__":
    main()