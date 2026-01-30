"""
Product Import Script - Updated with Marketplace Column Support
This script imports products from Excel and creates/updates them in the database
Supports marketplace column to determine which marketplace each product belongs to
"""

import pandas as pd
from datetime import datetime
from mongoengine import connect, DoesNotExist
from decimal import Decimal
import re
import sys
import os

# Get the base directory
base_dir = os.path.dirname(os.path.abspath(__file__))

# Add both paths
sys.path.insert(0, base_dir)
sys.path.insert(0, os.path.join(base_dir, 'ecommerce_tool'))
# Import your models (adjust the import path based on your project structure)
# from ecommerce_tool.models import Product, Brand, Marketplace

class ProductImporter:
    def __init__(self):
        """Initialize the importer"""
        self.marketplaces = {}  # Cache marketplaces
        self.brands = {}  # Cache brands
        self.stats = {
            'total_rows': 0,
            'created': 0,
            'updated': 0,
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
                # Use connection string (MongoDB Atlas or remote)
                connect(host=connection_string)
                print(f"✓ Connected to MongoDB Atlas")
            else:
                # Use host/port for local connection
                connect(db_name, host=host, port=port)
                print(f"✓ Connected to database: {db_name}")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {str(e)}")
            return False
    
    def get_or_create_marketplace(self, marketplace_name):
        """Get or create the marketplace"""
        from omnisight.models import Marketplace
        
        if not marketplace_name or pd.isna(marketplace_name):
            marketplace_name = "Walmart"  # Default
        
        # Clean marketplace name
        marketplace_name = str(marketplace_name).strip()
        
        # Check cache first
        if marketplace_name in self.marketplaces:
            return self.marketplaces[marketplace_name]
        
        try:
            marketplace = Marketplace.objects(name=marketplace_name).first()
            if not marketplace:
                # Extract country from marketplace name (e.g., "Amazon.de" -> "DE")
                country = self._extract_country(marketplace_name)
                
                marketplace = Marketplace(
                    name=marketplace_name,
                    url=self._generate_marketplace_url(marketplace_name),
                    created_at=datetime.now().isoformat(),
                    updated_at=datetime.now().isoformat(),
                    country=[country] if country else []
                )
                marketplace.save()
                print(f"✓ Created marketplace: {marketplace_name}")
            else:
                print(f"✓ Found marketplace: {marketplace_name}")
            
            # Cache it
            self.marketplaces[marketplace_name] = marketplace
            return marketplace
            
        except Exception as e:
            print(f"✗ Error with marketplace: {str(e)}")
            return None
    
    def _extract_country(self, marketplace_name):
        """Extract country code from marketplace name"""
        # Map common marketplace patterns to countries
        country_mapping = {
            'amazon.ae': 'AE',  # UAE
            'amazon.de': 'DE',  # Germany
            'amazon.com': 'US',  # USA
            'amazon.co.uk': 'UK',  # United Kingdom
            'amazon.fr': 'FR',  # France
            'amazon.it': 'IT',  # Italy
            'amazon.es': 'ES',  # Spain
            'amazon.in': 'IN',  # India
            'amazon.ca': 'CA',  # Canada
            'walmart': 'US',
            'walmart.com': 'US'
        }
        
        marketplace_lower = marketplace_name.lower()
        for pattern, country in country_mapping.items():
            if pattern in marketplace_lower:
                return country
        
        # Try to extract from domain extension
        if '.' in marketplace_name:
            extension = marketplace_name.split('.')[-1].upper()
            if len(extension) == 2:
                return extension
        
        return 'US'  # Default
    
    def _generate_marketplace_url(self, marketplace_name):
        """Generate marketplace URL from name"""
        marketplace_lower = marketplace_name.lower()
        
        if 'amazon' in marketplace_lower:
            return f"https://www.{marketplace_name.lower()}"
        elif 'walmart' in marketplace_lower:
            return "https://www.walmart.com"
        else:
            return f"https://www.{marketplace_name.lower()}"
    
    def get_or_create_brand(self, brand_name, marketplace):
        """Get or create a brand"""
        from omnisight.models import Brand
        
        if not brand_name or pd.isna(brand_name):
            return None
        
        brand_name = str(brand_name).strip()
        cache_key = f"{brand_name}_{marketplace.name}"
        
        # Check cache first
        if cache_key in self.brands:
            return self.brands[cache_key]
        
        try:
            brand = Brand.objects(name=brand_name, marketplace_id=marketplace).first()
            if not brand:
                brand = Brand(
                    name=brand_name,
                    marketplace_id=marketplace,
                    marketplace_ids=[marketplace]
                )
                brand.save()
            
            # Cache it
            self.brands[cache_key] = brand
            return brand
            
        except Exception as e:
            print(f"Warning: Could not create/get brand '{brand_name}': {str(e)}")
            return None
    
    def clean_price(self, value):
        """Clean price values by removing currency symbols and converting to float"""
        if pd.isna(value) or value == '':
            return 0.0
        
        # Remove currency symbols and whitespace
        value_str = str(value).replace('$', '').replace('€', '').replace('£', '').replace(',', '').strip()
        
        try:
            return float(value_str)
        except ValueError:
            return 0.0
    
    def clean_int(self, value):
        """Clean integer values"""
        if pd.isna(value) or value == '':
            return 0
        
        try:
            return int(float(value))
        except ValueError:
            return 0
    
    def parse_product_row(self, row):
        try:
            # Clean and extract data
            data = {
                'master_sku': str(row.get('Master SKU', '')).strip() if pd.notna(row.get('Master SKU')) else '',
                'parent_sku': str(row.get('Parent', '')).strip() if pd.notna(row.get('Parent')) else '',
                'sku': str(row.get('SKU', '')).strip() if pd.notna(row.get('SKU')) else '',
                'pack_size': self.clean_int(row.get('Pack Size')),
                'wpid': str(row.get('Item ID', '')).strip() if pd.notna(row.get('Item ID')) else '',
                'product_title': str(row.get('Product Name', '')).strip() if pd.notna(row.get('Product Name')) else '',
                'brand_name': str(row.get('Brand', '')).strip() if pd.notna(row.get('Brand')) else '',
                'lifecycle_status': str(row.get('Lifecycle Status', '')).strip() if pd.notna(row.get('Lifecycle Status')) else '',
                'published_status': str(row.get('Publish Status', '')).strip() if pd.notna(row.get('Publish Status')) else '',
                'unpublished_reasons': str(row.get('Status Change Reason', '')).strip() if pd.notna(row.get('Status Change Reason')) else '',
                'price': self.clean_price(row.get('Price')),
                'upc': str(row.get('UPC', '')).strip() if pd.notna(row.get('UPC')) else '',
                'w_product_cost': self.clean_price(row.get('Cost')),
                'walmart_fee': self.clean_price(row.get('Walmart Fee')),
                'w_shiping_cost': self.clean_price(row.get('Shipping')),
                'w_total_cogs': self.clean_price(row.get('Total Cost')),
                'vendor_funding': self.clean_price(row.get('Funding')),
                'net_profit': self.clean_price(row.get('Net Profit')),
                'marketplace_name': str(row.get('Marketplace', 'Walmart')).strip() if pd.notna(row.get('Marketplace')) else 'Walmart',
            }
            
            # Validate required fields
            if not data['wpid']:
                return None, "Missing Item ID (WPID)"
            
            if not data['product_title']:
                return None, "Missing Product Name"
            
            return data, None
            
        except Exception as e:
            return None, f"Error parsing row: {str(e)}"
    
    def create_or_update_product(self, product_data):
        """Create a new product or update existing one"""
        from omnisight.models import Product
        
        try:
            # Get or create marketplace
            marketplace = self.get_or_create_marketplace(product_data['marketplace_name'])
            if not marketplace:
                raise Exception(f"Could not create marketplace: {product_data['marketplace_name']}")
            
            # Try to find existing product by WPID and marketplace
            existing_product = Product.objects(
                wpid=product_data['wpid'],
                marketplace_id=marketplace
            ).first()
            
            # Get or create brand
            brand = None
            if product_data['brand_name']:
                brand = self.get_or_create_brand(product_data['brand_name'], marketplace)
            
            # Prepare update data
            update_data = {
                'product_title': product_data['product_title'],
                'wpid': product_data['wpid'],
                'sku': product_data['sku'], 
                'master_sku': product_data['master_sku'],
                'parent_sku': product_data['parent_sku'],
                'pack_size': product_data['pack_size'],
                'price': product_data['price'],
                'upc': product_data['upc'],
                'lifecycle_status': product_data['lifecycle_status'],
                'published_status': product_data['published_status'],
                'unpublished_reasons': product_data['unpublished_reasons'],
                'w_product_cost': product_data['w_product_cost'],
                'walmart_fee': product_data['walmart_fee'],
                'w_shiping_cost': product_data['w_shiping_cost'],
                'w_total_cogs': product_data['w_total_cogs'],
                'vendor_funding': product_data['vendor_funding'],
                'marketplace_id': marketplace,
                'brand_name': product_data['brand_name'],
                'updated_at': datetime.now(),
                'currency': '$'
            }
            
            if brand:
                update_data['brand_id'] = brand
            
            # Calculate net profit if not provided
            if product_data['net_profit'] == 0:
                update_data['net_profit'] = (
                    product_data['price'] - 
                    product_data['w_total_cogs'] - 
                    product_data['walmart_fee']
                )
            else:
                update_data['net_profit'] = product_data['net_profit']
            
            if existing_product:
                # Update existing product
                for key, value in update_data.items():
                    setattr(existing_product, key, value)
                existing_product.save()
                return 'updated', existing_product
            else:
                # Create new product
                update_data['created_at'] = datetime.now()
                update_data['product_created_date'] = datetime.now()
                update_data['product_id_type'] = 'WPID'
                update_data['product_id'] = product_data['wpid']
                
                new_product = Product(**update_data)
                new_product.save()
                return 'created', new_product
                
        except Exception as e:
            raise Exception(f"Error creating/updating product: {str(e)}")
    
    def import_from_excel(self, file_path, sheet_name=0):
        """
        Import products from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index (default: 0 for first sheet)
        """
        print(f"\n{'='*60}")
        print(f"Starting Product Import from: {file_path}")
        print(f"{'='*60}\n")
        
        try:
            # Read Excel file
            print("Reading Excel file...")
            # df = pd.read_excel(file_path, sheet_name=sheet_name)
            df = pd.read_csv(file_path)
            self.stats['total_rows'] = len(df)
            print(f"✓ Found {len(df)} rows in Excel file\n")
            
            print(f"\nProcessing products...")
            print("-" * 60)
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    # Parse product data
                    product_data, error = self.parse_product_row(row)
                    
                    if error:
                        self.stats['skipped'] += 1
                        self.stats['error_details'].append({
                            'row': index + 2,  # +2 for Excel row (header + 0-index)
                            'error': error
                        })
                        continue
                    
                    # Create or update product
                    action, product = self.create_or_update_product(product_data)
                    
                    if action == 'created':
                        self.stats['created'] += 1
                        print(f"✓ Row {index + 2}: CREATED - {product.product_title[:50]} ({product_data['marketplace_name']})")
                    elif action == 'updated':
                        self.stats['updated'] += 1
                        print(f"✓ Row {index + 2}: UPDATED - {product.product_title[:50]} ({product_data['marketplace_name']})")
                    
                except Exception as e:
                    self.stats['errors'] += 1
                    error_msg = str(e)
                    self.stats['error_details'].append({
                        'row': index + 2,
                        'error': error_msg
                    })
                    print(f"✗ Row {index + 2}: ERROR - {error_msg}")
            
            # Print summary
            self.print_summary()
            return True
            
        except FileNotFoundError:
            print(f"✗ Error: File not found - {file_path}")
            return False
        except Exception as e:
            print(f"✗ Error reading Excel file: {str(e)}")
            return False
    
    def print_summary(self):
        """Print import summary"""
        print(f"\n{'='*60}")
        print("IMPORT SUMMARY")
        print(f"{'='*60}")
        print(f"Total Rows:        {self.stats['total_rows']}")
        print(f"Created:           {self.stats['created']}")
        print(f"Updated:           {self.stats['updated']}")
        print(f"Skipped:           {self.stats['skipped']}")
        print(f"Errors:            {self.stats['errors']}")
        print(f"{'='*60}")
        
        if self.stats['error_details']:
            print(f"\nError Details:")
            print("-" * 60)
            for detail in self.stats['error_details'][:10]:  # Show first 10 errors
                print(f"Row {detail['row']}: {detail['error']}")
            
            if len(self.stats['error_details']) > 10:
                print(f"\n... and {len(self.stats['error_details']) - 10} more errors")
        
        print()


def main():
    """Main function to run the import"""
    # Configuration
    CONNECTION_STRING = "mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0"
    EXCEL_FILE = "/home/lexicon/Downloads/BigTree-Marketlynxe Product info-updated(in).csv"

    
    # Create importer instance
    importer = ProductImporter()
    
    # Connect to database using connection string
    if not importer.connect_db(connection_string=CONNECTION_STRING):
        return
    
    # Import products
    importer.import_from_excel(EXCEL_FILE)


if __name__ == "__main__":
    main()