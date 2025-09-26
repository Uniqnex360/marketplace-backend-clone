import pymongo
from datetime import datetime, timedelta
import pandas as pd
from bson import ObjectId
import json
import os
from typing import Dict, List, Any

class MongoDataExtractor:
    def __init__(self, connection_string: str):
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client['ecommerce_db']
        
        # Calculate date range for last 3 months
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=60)
        
        print(f"Extracting data from {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
    
    def convert_objectid_to_string(self, obj):
        
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {key: self.convert_objectid_to_string(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_objectid_to_string(item) for item in obj]
        else:
            return obj
    
    def extract_orders(self) -> List[Dict]:
        print("Extracting orders...")
        collection = self.db['order']
        
        query = {
            "$or": [
                {"order_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"purchase_order_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"created_at": {"$gte": self.start_date, "$lte": self.end_date}},
                {"updated_at": {"$gte": self.start_date, "$lte": self.end_date}}
            ]
        }
        
        orders = list(collection.find(query))
        print(f"Found {len(orders)} orders")
        return orders
    
    def extract_custom_orders(self) -> List[Dict]:
        print("Extracting custom orders...")
        collection = self.db['custom_order']
        
        query = {
            "$or": [
                {"purchase_order_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"created_at": {"$gte": self.start_date, "$lte": self.end_date}},
                {"updated_at": {"$gte": self.start_date, "$lte": self.end_date}}
            ]
        }
        
        custom_orders = list(collection.find(query))
        print(f"Found {len(custom_orders)} custom orders")
        return custom_orders
    
    def extract_order_items(self) -> List[Dict]:
        print("Extracting order items...")
        collection = self.db['order_items']
        
        query = {
            "$or": [
                {"created_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"document_created_date": {"$gte": self.start_date, "$lte": self.end_date}}
            ]
        }
        
        order_items = list(collection.find(query))
        print(f"Found {len(order_items)} order items")
        return order_items
    
    def extract_products(self) -> List[Dict]:
        print("Extracting products...")
        collection = self.db['product']
        
        query = {
            "$or": [
                {"created_at": {"$gte": self.start_date, "$lte": self.end_date}},
                {"updated_at": {"$gte": self.start_date, "$lte": self.end_date}},
                {"product_created_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"producted_last_updated_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"open_date": {"$gte": self.start_date, "$lte": self.end_date}}
            ]
        }
        
        products = list(collection.find(query))
        print(f"Found {len(products)} products")
        return products
    
    def extract_pageview_sessions(self) -> List[Dict]:
        print("Extracting pageview sessions...")
        collection = self.db['pageview_session_count']
        
        query = {
            "date": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        pageview_sessions = list(collection.find(query))
        print(f"Found {len(pageview_sessions)} pageview session records")
        return pageview_sessions
    
    def extract_inventory_logs(self) -> List[Dict]:
        print("Extracting inventory logs...")
        collection = self.db['inventry_log']
        
        query = {
            "date": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        inventory_logs = list(collection.find(query))
        print(f"Found {len(inventory_logs)} inventory log records")
        return inventory_logs
    
    def extract_product_price_changes(self) -> List[Dict]:
    
        print("Extracting product price changes...")
        collection = self.db['productPriceChange']
        
        query = {
            "change_date": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        price_changes = list(collection.find(query))
        print(f"Found {len(price_changes)} price change records")
        return price_changes
    
    def extract_notes(self) -> List[Dict]:
        """
        Extract notes from the last 3 months
        """
        print("Extracting notes...")
        collection = self.db['notes_data']
        
        query = {
            "date_f": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        notes = list(collection.find(query))
        print(f"Found {len(notes)} notes")
        return notes
    
    def extract_fees(self) -> List[Dict]:
        """
        Extract fees from the last 3 months
        """
        print("Extracting fees...")
        collection = self.db['fee']
        
        query = {
            "date": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        fees = list(collection.find(query))
        print(f"Found {len(fees)} fee records")
        return fees
    
    def extract_refunds(self) -> List[Dict]:
        """
        Extract refunds from the last 3 months
        """
        print("Extracting refunds...")
        collection = self.db['refund']
        
        query = {
            "date": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        refunds = list(collection.find(query))
        print(f"Found {len(refunds)} refund records")
        return refunds
    
    def extract_cached_metrics(self) -> List[Dict]:
        """
        Extract cached metrics from the last 3 months
        """
        print("Extracting cached metrics...")
        collection = self.db['cached_metrics']
        
        query = {
            "$or": [
                {"from_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"to_date": {"$gte": self.start_date, "$lte": self.end_date}},
                {"last_updated": {"$gte": self.start_date, "$lte": self.end_date}}
            ]
        }
        
        cached_metrics = list(collection.find(query))
        print(f"Found {len(cached_metrics)} cached metrics records")
        return cached_metrics
    
    def extract_shipping_rates(self) -> List[Dict]:
        """
        Extract shipping rates from the last 3 months
        """
        print("Extracting shipping rates...")
        collection = self.db['shipping_rate']
        
        query = {
            "created_at": {"$gte": self.start_date, "$lte": self.end_date}
        }
        
        shipping_rates = list(collection.find(query))
        print(f"Found {len(shipping_rates)} shipping rate records")
        return shipping_rates
    
    def save_to_json(self, data: List[Dict], filename: str):
        """
        Save data to JSON file
        """
        if data:
            # Convert ObjectIds to strings
            converted_data = self.convert_objectid_to_string(data)
            
            with open(f"{filename}.json", 'w', encoding='utf-8') as f:
                json.dump(converted_data, f, indent=2, default=str, ensure_ascii=False)
            print(f"Saved {len(data)} records to {filename}.json")
    
    def save_to_csv(self, data: List[Dict], filename: str):
        """
        Save data to CSV file (flattened structure)
        """
        if data:
            # Convert ObjectIds to strings
            converted_data = self.convert_objectid_to_string(data)
            
            # Flatten nested dictionaries for CSV
            flattened_data = []
            for record in converted_data:
                flattened_record = self.flatten_dict(record)
                flattened_data.append(flattened_record)
            
            df = pd.DataFrame(flattened_data)
            df.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
            print(f"Saved {len(data)} records to {filename}.csv")
    
    def flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """
        Flatten nested dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to strings for CSV compatibility
                items.append((new_key, str(v) if v else ''))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def extract_all_data(self, export_format='json'):
        """
        Extract all data from the last 3 months
        """
        print(f"Starting data extraction for the last 3 months...")
        print(f"Export format: {export_format}")
        
        # Create output directory
        output_dir = f"mongodb_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(output_dir, exist_ok=True)
        os.chdir(output_dir)
        
        # Extract data from each collection
        extraction_methods = {
            'orders': self.extract_orders,
            'custom_orders': self.extract_custom_orders,
            'order_items': self.extract_order_items,
            'products': self.extract_products,
            'pageview_sessions': self.extract_pageview_sessions,
            'inventory_logs': self.extract_inventory_logs,
            'product_price_changes': self.extract_product_price_changes,
            'notes': self.extract_notes,
            'fees': self.extract_fees,
            'refunds': self.extract_refunds,
            'cached_metrics': self.extract_cached_metrics,
            'shipping_rates': self.extract_shipping_rates
        }
        
        total_records = 0
        
        for collection_name, extraction_method in extraction_methods.items():
            try:
                data = extraction_method()
                if data:
                    total_records += len(data)
                    
                    if export_format.lower() == 'json':
                        self.save_to_json(data, collection_name)
                    elif export_format.lower() == 'csv':
                        self.save_to_csv(data, collection_name)
                    elif export_format.lower() == 'both':
                        self.save_to_json(data, collection_name)
                        self.save_to_csv(data, collection_name)
                else:
                    print(f"No data found for {collection_name}")
                    
            except Exception as e:
                print(f"Error extracting {collection_name}: {str(e)}")
        
        print(f"\nExtraction completed!")
        print(f"Total records extracted: {total_records}")
        print(f"Files saved in directory: {output_dir}")
        
        # Go back to original directory
        os.chdir('..')
        
    def close_connection(self):
        """
        Close MongoDB connection
        """
        self.client.close()
        print("MongoDB connection closed")

# Usage example
if __name__ == "__main__":
    # MongoDB connection string
    CONNECTION_STRING = "mongodb://plmp_admin:admin%401234@54.86.75.104:27017/ecommerce_db?authSource=admin"
    
    # Initialize extractor
    extractor = MongoDataExtractor(CONNECTION_STRING)
    
    try:
        # Extract all data (options: 'json', 'csv', 'both')
        extractor.extract_all_data(export_format='both')
        
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
    
    finally:
        # Close connection
        extractor.close_connection()