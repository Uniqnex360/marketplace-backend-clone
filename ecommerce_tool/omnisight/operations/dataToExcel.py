# import pandas as pd
# from mongoengine import connect, disconnect
# from datetime import datetime
# import os
# from typing import List, Dict, Any
# import logging

# # Import your models (assuming they're in a file called models.py)
# from ..models import Order, OrderItems, Product, Marketplace, Brand, Manufacturer

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class MongoToExcelExporter:
#     def __init__(self, mongo_uri: str, database_name: str):
#         self.mongo_uri = mongo_uri
#         self.database_name = database_name
#         self.connection = None
    
#     def connect_to_mongodb(self):
#         """Establish connection to MongoDB"""
#         try:
#             self.connection = connect(db=self.database_name, host=self.mongo_uri)
#             logger.info(f"Successfully connected to MongoDB: {self.database_name}")
#         except Exception as e:
#             logger.error(f"Failed to connect to MongoDB: {str(e)}")
#             raise
    
#     def disconnect_from_mongodb(self):
#         """Close MongoDB connection"""
#         if self.connection:
#             disconnect()
#             logger.info("Disconnected from MongoDB")
    
#     def flatten_embedded_document(self, embedded_doc, prefix: str = "") -> Dict[str, Any]:
#         """
#         Flatten embedded documents for Excel export
        
#         Args:
#             embedded_doc: The embedded document to flatten
#             prefix: Prefix for field names
            
#         Returns:
#             Dictionary with flattened fields
#         """
#         flattened = {}
        
#         if embedded_doc is None:
#             return flattened
        
#         if hasattr(embedded_doc, '_data'):
#             data = embedded_doc._data
#         else:
#             data = embedded_doc if isinstance(embedded_doc, dict) else {}
        
#         for key, value in data.items():
#             field_name = f"{prefix}_{key}" if prefix else key
            
#             if hasattr(value, '_data'):  # Another embedded document
#                 flattened.update(self.flatten_embedded_document(value, field_name))
#             elif isinstance(value, dict):
#                 for sub_key, sub_value in value.items():
#                     flattened[f"{field_name}_{sub_key}"] = sub_value
#             elif isinstance(value, list):
#                 # Handle lists by converting to string or processing each item
#                 if value and hasattr(value[0], '_data'):
#                     for i, item in enumerate(value):
#                         flattened.update(self.flatten_embedded_document(item, f"{field_name}_{i}"))
#                 else:
#                     flattened[field_name] = str(value)
#             else:
#                 flattened[field_name] = value
        
#         return flattened
    
#     def get_order_items_data(self, batch_size: int = 100) -> List[Dict[str, Any]]:
#         """
#         Retrieve and flatten OrderItems data using small batch processing
#         Compatible with MongoDB Atlas free/shared tiers
        
#         Args:
#             batch_size: Number of documents to process in each batch
        
#         Returns:
#             List of dictionaries containing flattened order items data
#         """
#         logger.info("Fetching OrderItems data...")
        
#         order_items_data = []
        
#         try:
#             total_items = OrderItems.objects.count()
#             logger.info(f"Total order items to process: {total_items}")
            
#             # Process in small batches to avoid cursor timeout
#             for skip in range(0, total_items, batch_size):
#                 batch_end = min(skip + batch_size, total_items)
                
#                 # Create a fresh query for each batch (no persistent cursor)
#                 order_items = OrderItems.objects.skip(skip).limit(batch_size).no_cache()
                
#                 batch_count = 0
#                 for item in order_items:
#                     # Start with basic fields
#                     item_data = {
#                         'order_item_id': str(item.id),
#                         'order_id': item.OrderId,
#                         'platform': item.Platform,
#                         'is_gift': item.IsGift,
#                         'promotion_discount': item.PromotionDiscount,
#                         'net_profit': item.net_profit,
#                         'tax_checked': item.tax_checked,
#                         'pricing_checked': item.pricing_checked,
#                         'created_date': item.created_date,
#                         'document_created_date': item.document_created_date,
#                         'updated_at': item.updated_at
#                     }
                    
#                     # Flatten ProductDetails
#                     if item.ProductDetails:
#                         product_details = self.flatten_embedded_document(item.ProductDetails, 'product_details')
#                         item_data.update(product_details)
                        
#                         # Get related product info if available
#                         if item.ProductDetails.product_id:
#                             try:
#                                 product = item.ProductDetails.product_id
#                                 item_data.update({
#                                     'product_title': product.product_title,
#                                     'product_brand': product.brand_name,
#                                     'product_manufacturer': product.manufacturer_name,
#                                     'product_price': product.price,
#                                     'product_currency': product.currency
#                                 })
#                             except Exception as e:
#                                 logger.warning(f"Could not fetch product details: {str(e)}")
                    
#                     # Flatten Pricing
#                     if item.Pricing:
#                         pricing_data = self.flatten_embedded_document(item.Pricing, 'pricing')
#                         item_data.update(pricing_data)
                    
#                     # Flatten Fulfillment
#                     if item.Fulfillment:
#                         fulfillment_data = self.flatten_embedded_document(item.Fulfillment, 'fulfillment')
#                         item_data.update(fulfillment_data)
                    
#                     # Flatten OrderStatus
#                     if item.OrderStatus:
#                         status_data = self.flatten_embedded_document(item.OrderStatus, 'order_status')
#                         item_data.update(status_data)
                    
#                     # Flatten TaxCollection
#                     if item.TaxCollection:
#                         tax_data = self.flatten_embedded_document(item.TaxCollection, 'tax_collection')
#                         item_data.update(tax_data)
                    
#                     # Flatten BuyerInfo
#                     if item.BuyerInfo:
#                         buyer_data = self.flatten_embedded_document(item.BuyerInfo, 'buyer_info')
#                         item_data.update(buyer_data)
                    
#                     order_items_data.append(item_data)
#                     batch_count += 1
                
#                 # Log progress every 1000 items
#                 if (skip + batch_size) % 1000 == 0 or batch_end == total_items:
#                     logger.info(f"Processed {len(order_items_data)}/{total_items} order items...")
                
#         except Exception as e:
#             logger.error(f"Error fetching OrderItems data: {str(e)}")
#             logger.error(f"Successfully processed {len(order_items_data)} items before error")
#             raise
        
#         logger.info(f"Successfully fetched {len(order_items_data)} order items")
#         return order_items_data
    
#     def get_orders_data(self, batch_size: int = 100) -> List[Dict[str, Any]]:
#         """
#         Retrieve and flatten Orders data using small batch processing
#         Compatible with MongoDB Atlas free/shared tiers
        
#         Args:
#             batch_size: Number of documents to process in each batch
        
#         Returns:
#             List of dictionaries containing flattened orders data
#         """
#         logger.info("Fetching Orders data...")
        
#         orders_data = []
        
#         try:
#             total_orders = Order.objects.count()
#             logger.info(f"Total orders to process: {total_orders}")
            
#             # Process in small batches to avoid cursor timeout
#             for skip in range(0, total_orders, batch_size):
#                 batch_end = min(skip + batch_size, total_orders)
                
#                 # Create a fresh query for each batch (no persistent cursor)
#                 orders = Order.objects.skip(skip).limit(batch_size).no_cache()
                
#                 batch_count = 0
#                 for order in orders:
#                     # Start with basic fields
#                     order_data = {
#                         'order_id': str(order.id),
#                         'purchase_order_id': order.purchase_order_id,
#                         'customer_order_id': order.customer_order_id,
#                         'seller_order_id': order.seller_order_id,
#                         'merchant_order_id': order.merchant_order_id,
#                         'shipstation_id': order.shipstation_id,
#                         'shipstation_synced': order.shipstation_synced,
#                         'shipstation_sync_date': order.shipstation_sync_date,
#                         'shipping_rates_fetched': order.shipping_rates_fetched,
#                         'shipping_rates_date': order.shipping_rates_date,
#                         'shipping_cost': order.shipping_cost,
#                         'tracking_number': order.tracking_number,
#                         'merchant_shipment_cost': order.merchant_shipment_cost,
#                         'customer_email_id': order.customer_email_id,
#                         'order_date': order.order_date,
#                         'pacific_date': order.pacific_date,
#                         'earliest_ship_date': order.earliest_ship_date,
#                         'latest_ship_date': order.latest_ship_date,
#                         'last_update_date': order.last_update_date,
#                         'ship_service_level': order.ship_service_level,
#                         'shipment_service_level_category': order.shipment_service_level_category,
#                         'order_status': order.order_status,
#                         'number_of_items_shipped': order.number_of_items_shipped,
#                         'number_of_items_unshipped': order.number_of_items_unshipped,
#                         'fulfillment_channel': order.fulfillment_channel,
#                         'sales_channel': order.sales_channel,
#                         'order_type': order.order_type,
#                         'is_premium_order': order.is_premium_order,
#                         'is_prime': order.is_prime,
#                         'has_regulated_items': order.has_regulated_items,
#                         'is_replacement_order': order.is_replacement_order,
#                         'is_sold_by_ab': order.is_sold_by_ab,
#                         'is_ispu': order.is_ispu,
#                         'is_access_point_order': order.is_access_point_order,
#                         'is_business_order': order.is_business_order,
#                         'marketplace': order.marketplace,
#                         'payment_method': order.payment_method,
#                         'payment_method_details': order.payment_method_details,
#                         'order_total': order.order_total,
#                         'currency': order.currency,
#                         'is_global_express_enabled': order.is_global_express_enabled,
#                         'customer_name': order.customer_name,
#                         'order_channel': order.order_channel,
#                         'items_order_quantity': order.items_order_quantity,
#                         'shipping_price': order.shipping_price,
#                         'updated_at': order.updated_at
#                     }
                    
#                     # Add marketplace info if available
#                     if order.marketplace_id:
#                         try:
#                             marketplace = order.marketplace_id
#                             order_data.update({
#                                 'marketplace_name': marketplace.name,
#                                 'marketplace_url': marketplace.url,
#                                 'marketplace_image_url': marketplace.image_url
#                             })
#                         except Exception as e:
#                             logger.warning(f"Could not fetch marketplace details: {str(e)}")
                    
#                     # Handle shipping information (DictField)
#                     if order.shipping_information:
#                         for key, value in order.shipping_information.items():
#                             order_data[f'shipping_info_{key}'] = value
                    
#                     # Handle automated shipping settings (DictField)
#                     if order.automated_shipping_settings:
#                         for key, value in order.automated_shipping_settings.items():
#                             order_data[f'auto_shipping_{key}'] = value
                    
#                     # Handle order details (ListField of DictField)
#                     if order.order_details:
#                         for i, detail in enumerate(order.order_details):
#                             for key, value in detail.items():
#                                 order_data[f'order_detail_{i}_{key}'] = value
                    
#                     # Add count of order items
#                     order_data['order_items_count'] = len(order.order_items) if order.order_items else 0
                    
#                     # Add order item IDs as comma-separated string
#                     if order.order_items:
#                         order_data['order_item_ids'] = ','.join([str(item.id) for item in order.order_items])
                    
#                     orders_data.append(order_data)
#                     batch_count += 1
                
#                 # Log progress every 1000 items
#                 if (skip + batch_size) % 1000 == 0 or batch_end == total_orders:
#                     logger.info(f"Processed {len(orders_data)}/{total_orders} orders...")
                
#         except Exception as e:
#             logger.error(f"Error fetching Orders data: {str(e)}")
#             logger.error(f"Successfully processed {len(orders_data)} orders before error")
#             raise
        
#         logger.info(f"Successfully fetched {len(orders_data)} orders")
#         return orders_data
    
#     def export_to_excel(self, output_filename: str = None, chunk_to_csv: bool = False):
#         """
#         Export Orders and OrderItems data to Excel file
        
#         Args:
#             output_filename: Name of the output Excel file
#             chunk_to_csv: If True, export to CSV instead for large datasets
#         """
#         if not output_filename:
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             if chunk_to_csv:
#                 output_filename = f"orders_export_{timestamp}.csv"
#             else:
#                 output_filename = f"orders_and_items_export_{timestamp}.xlsx"
        
#         try:
#             # Get data from both collections
#             logger.info("Starting data export...")
            
#             orders_data = self.get_orders_data()
#             order_items_data = self.get_order_items_data()
            
#             # Verify counts
#             logger.info(f"Orders collected: {len(orders_data)}")
#             logger.info(f"Order items collected: {len(order_items_data)}")
            
#             # Create DataFrames
#             orders_df = pd.DataFrame(orders_data)
#             order_items_df = pd.DataFrame(order_items_data)
            
#             if chunk_to_csv:
#                 # Export to CSV (better for very large datasets)
#                 orders_csv = output_filename.replace('.csv', '_orders.csv')
#                 items_csv = output_filename.replace('.csv', '_order_items.csv')
                
#                 if not orders_df.empty:
#                     orders_df.to_csv(orders_csv, index=False)
#                     logger.info(f"Exported {len(orders_df)} orders to '{orders_csv}'")
                
#                 if not order_items_df.empty:
#                     order_items_df.to_csv(items_csv, index=False)
#                     logger.info(f"Exported {len(order_items_df)} order items to '{items_csv}'")
                
#                 print(f"\n{'='*60}")
#                 print(f"Export completed successfully!")
#                 print(f"Orders file: {orders_csv}")
#                 print(f"Items file: {items_csv}")
#                 print(f"Total Orders: {len(orders_df)}")
#                 print(f"Total Order Items: {len(order_items_df)}")
#                 print(f"{'='*60}\n")
#             else:
#                 # Create Excel writer object
#                 with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
#                     # Write Orders data
#                     if not orders_df.empty:
#                         orders_df.to_excel(writer, sheet_name='Orders', index=False)
#                         logger.info(f"Exported {len(orders_df)} orders to 'Orders' sheet")
#                     else:
#                         logger.warning("No orders data to export")
                    
#                     # Write OrderItems data
#                     if not order_items_df.empty:
#                         order_items_df.to_excel(writer, sheet_name='OrderItems', index=False)
#                         logger.info(f"Exported {len(order_items_df)} order items to 'OrderItems' sheet")
#                     else:
#                         logger.warning("No order items data to export")
                    
#                     # Create a summary sheet
#                     summary_data = {
#                         'Metric': ['Total Orders', 'Total Order Items', 'Export Date'],
#                         'Value': [len(orders_df), len(order_items_df), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
#                     }
#                     summary_df = pd.DataFrame(summary_data)
#                     summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
#                 logger.info(f"Successfully exported data to {output_filename}")
#                 print(f"\n{'='*60}")
#                 print(f"Export completed successfully!")
#                 print(f"File saved as: {output_filename}")
#                 print(f"Total Orders: {len(orders_df)}")
#                 print(f"Total Order Items: {len(order_items_df)}")
#                 print(f"{'='*60}\n")
            
#         except Exception as e:
#             logger.error(f"Error during export: {str(e)}")
#             raise

# def main():
#     """
#     Main function to run the export process
#     """
#     # MongoDB connection settings
#     MONGO_URI = "mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0"
#     DATABASE_NAME = 'ecommerce_db'
    
#     # Create exporter instance
#     exporter = MongoToExcelExporter(MONGO_URI, DATABASE_NAME)
    
#     try:
#         # Connect to MongoDB
#         exporter.connect_to_mongodb()
        
#         # Export data to Excel (or CSV for large datasets)
#         # Set chunk_to_csv=True if you have memory issues with Excel
#         exporter.export_to_excel(chunk_to_csv=False)
        
#     except Exception as e:
#         logger.error(f"Export failed: {str(e)}")
#         print(f"\nExport failed: {str(e)}\n")
    
#     finally:
#         # Always disconnect
#         exporter.disconnect_from_mongodb()

# if __name__ == "__main__":
#     main()