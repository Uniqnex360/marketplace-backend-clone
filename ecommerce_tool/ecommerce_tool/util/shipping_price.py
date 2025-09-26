from __future__ import annotations
import requests
import json
import logging
from ecommerce_tool.settings import SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET
import sys
from datetime import datetime, time, timedelta
import time as t
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any, Union
import random
import threading
from collections import deque
import re

logger = logging.getLogger(__name__)


class EmailAwareShipStationClient:
    """
    Enhanced ShipStation API client that handles email mismatches between 
    database and ShipStation, particularly for Walmart relay emails.
    """
    
    # Global rate limiting across all instances
    _request_times = deque(maxlen=200)
    _lock = threading.Lock()
    _global_backoff_until = 0
    
    # Email mapping cache
    _email_mapping_cache = {}
    _cache_lock = threading.Lock()
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.api_key = api_key or SHIPSTATION_API_KEY
        self.api_secret = api_secret or SHIPSTATION_API_SECRET
        self.base_url = "https://ssapi.shipstation.com"
        self.auth = (self.api_key, self.api_secret)
        
        self.max_retries = 7
        self.base_delay = 2.0
        self.max_delay = 120.0
        
        # Marketplace-specific email patterns
        self.walmart_relay_pattern = re.compile(r'^[A-F0-9]+@relay\.walmart\.com$', re.IGNORECASE)
    
    def _check_rate_limit(self) -> float:
        """Check if we should delay before making a request."""
        with self._lock:
            now = t.time()
            
            if now < self._global_backoff_until:
                return self._global_backoff_until - now
            
            cutoff_time = now - 60
            while self._request_times and self._request_times[0] < cutoff_time:
                self._request_times.popleft()
            
            if len(self._request_times) >= 30:
                oldest_request = self._request_times[0]
                delay_needed = 60 - (now - oldest_request)
                if delay_needed > 0:
                    return delay_needed
            
            return 0
    
    def _record_request(self):
        """Record that a request was made."""
        with self._lock:
            self._request_times.append(t.time())
    
    def _set_global_backoff(self, backoff_seconds: float):
        """Set global backoff for all instances."""
        with self._lock:
            self._global_backoff_until = max(
                self._global_backoff_until, 
                t.time() + backoff_seconds
            )

    def safe_request(self, url: str, **kwargs) -> Dict[str, Any]:
        """Make a safe API request with comprehensive rate limiting."""
        for attempt in range(self.max_retries):
            try:
                delay = self._check_rate_limit()
                if delay > 0:
                    t.sleep(delay)
                
                self._record_request()
                
                resp = requests.get(url, auth=self.auth, timeout=45, **kwargs)
                
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    base_wait = min(self.max_delay, self.base_delay * (2 ** attempt))
                    jitter = random.uniform(0.8, 1.2)
                    api_wait = retry_after * jitter
                    wait_time = max(base_wait, api_wait)
                    
                    if retry_after > 30 or attempt >= 3:
                        self._set_global_backoff(wait_time)
                    
                    logger.warning(f"Rate limited. Waiting {wait_time:.1f}s (attempt {attempt+1})")
                    t.sleep(wait_time)
                    continue
                
                elif resp.status_code in [502, 503, 504]:
                    wait_time = min(self.max_delay, self.base_delay * (2 ** attempt)) * random.uniform(0.8, 1.2)
                    logger.warning(f"Server error {resp.status_code}. Retrying in {wait_time:.1f}s")
                    t.sleep(wait_time)
                    continue
                
                resp.raise_for_status()
                return resp.json()
                
            except requests.exceptions.Timeout:
                wait_time = min(self.max_delay, self.base_delay * (2 ** attempt))
                logger.warning(f"Request timeout. Retrying in {wait_time:.1f}s")
                t.sleep(wait_time)
                continue
                
            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Final attempt failed for {url}: {e}")
                    raise
                wait_time = min(self.max_delay, self.base_delay * (2 ** attempt))
                logger.warning(f"Request failed: {e}. Retrying in {wait_time:.1f}s")
                t.sleep(wait_time)
                continue
        
        raise Exception(f"Failed after {self.max_retries} retries for {url}")

    def find_order_by_po_and_date(self, 
                                 purchase_order_id: str, 
                                 order_date_utc_iso: Union[str, datetime],
                                 local_tz: str = 'US/Pacific') -> Optional[Dict[str, Any]]:
        """
        Find order by PO ID and date without relying on email.
        This is more reliable when emails don't match.
        """
        if not purchase_order_id or purchase_order_id.strip() == "":
            logger.warning("Empty purchase order ID provided")
            return None
        
        start_date, end_date = self.get_utc_range_for_order_date(order_date_utc_iso, local_tz)
        
        try:
            # Search by date range only, then filter by PO ID
            base_url = f"{self.base_url}/orders"
            params = {
                "pageSize": 500,  # Larger page size to reduce API calls
                "page": 1,
                "orderDateStart": start_date,
                "orderDateEnd": end_date
            }
            
            url = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            data = self.safe_request(url)
            
            total_pages = data.get("pages", 1)
            orders_all = data.get("orders", [])
            
            logger.info(f"Searching {total_pages} pages for PO {purchase_order_id}")
            
            # Search additional pages if needed
            if total_pages > 1:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = []
                    for page in range(2, min(total_pages + 1, 6)):  # Limit to 5 pages max
                        if futures and len(futures) >= 2:
                            # Process completed futures
                            for future in as_completed(futures[:1]):
                                try:
                                    page_data = future.result()
                                    orders_all.extend(page_data.get("orders", []))
                                except Exception as e:
                                    logger.error(f"Error fetching page: {e}")
                                futures.remove(future)
                        
                        t.sleep(1.0)  # Delay between submissions
                        future = executor.submit(self.fetch_page, base_url, {**params, "page": page})
                        futures.append(future)
                    
                    # Process remaining futures
                    for future in as_completed(futures):
                        try:
                            page_data = future.result()
                            orders_all.extend(page_data.get("orders", []))
                        except Exception as e:
                            logger.error(f"Error fetching page: {e}")
            
            # Find matching order by PO ID
            for order in orders_all:
                order_po_id = order.get("advancedOptions", {}).get("customField2")
                if order_po_id == purchase_order_id:
                    logger.info(f"Found order {order.get('orderNumber')} for PO {purchase_order_id}")
                    return order
            
            logger.warning(f"No order found for PO {purchase_order_id} in date range")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for PO {purchase_order_id}: {e}")
            return None

    def get_full_order_and_shipping_details_by_po(self, 
                                                 purchase_order_id: str,
                                                 order_date_utc_iso: Union[str, datetime],
                                                 local_tz: str = 'US/Pacific') -> Optional[Dict[str, Any]]:
        """
        Get complete order details by PO ID, bypassing email issues.
        """
        try:
            # First find the order
            order = self.find_order_by_po_and_date(purchase_order_id, order_date_utc_iso, local_tz)
            if not order:
                return None
            
            order_number = order.get("orderNumber")
            if not order_number:
                logger.error("Found order but no order number")
                return order
            
            # Get shipment details
            t.sleep(0.5)  # Small delay between requests
            shipment_url = f"{self.base_url}/shipments?orderNumber={order_number}"
            shipment_data = self.safe_request(shipment_url)
            shipments = shipment_data.get("shipments", [])
            
            # Add shipment details to order
            order["shipments"] = [
                {
                    "shipmentId": shipment.get("shipmentId"),
                    "carrierCode": shipment.get("carrierCode"),
                    "serviceCode": shipment.get("serviceCode"),
                    "shipmentCost": shipment.get("shipmentCost", 0),
                    "shipDate": shipment.get("shipDate"),
                    "trackingNumber": shipment.get("trackingNumber"),
                }
                for shipment in shipments
            ]
            
            # Calculate total shipping cost
            total_shipping_cost = sum(
                float(shipment.get("shipmentCost", 0)) 
                for shipment in order["shipments"]
            )
            order["totalShippingCost"] = total_shipping_cost
            
            logger.info(f"Order {order_number} has {len(shipments)} shipments, total cost: ${total_shipping_cost}")
            
            return order
            
        except Exception as e:
            logger.error(f"Error fetching full details for PO {purchase_order_id}: {e}")
            return None

    def get_utc_range_for_order_date(self, 
                                   order_date_utc_iso: Union[str, datetime], 
                                   local_tz_str: str = 'US/Pacific') -> tuple[str, str]:
        """Convert order date to UTC range for API queries."""
        local_tz = pytz.timezone(local_tz_str)
        
        if isinstance(order_date_utc_iso, str):
            # Handle different datetime formats
            for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    utc_dt = datetime.strptime(order_date_utc_iso, fmt).replace(tzinfo=pytz.utc)
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Unable to parse date: {order_date_utc_iso}")
        elif isinstance(order_date_utc_iso, datetime):
            utc_dt = order_date_utc_iso
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        else:
            raise ValueError("order_date_utc_iso must be a str or datetime.datetime")
        
        # Create a wider date range to catch edge cases
        local_dt = utc_dt.astimezone(local_tz)
        center_date = local_dt.date()
        start_date = center_date - timedelta(days=2)  # Wider range
        end_date = center_date + timedelta(days=1)
        
        local_start_dt = local_tz.localize(datetime.combine(start_date, time.min))
        local_end_dt = local_tz.localize(datetime.combine(end_date, time.max))
        
        utc_start = local_start_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        utc_end = local_end_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info(f"Searching orders between UTC {utc_start} and {utc_end}")
        return utc_start, utc_end

    def fetch_page(self, base_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch a single page with additional safety measures."""
        url = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        return self.safe_request(url)

    def get_shipping_cost_by_po(self, 
                               purchase_order_id: str,
                               order_date_utc_iso: Union[str, datetime],
                               local_tz: str = 'US/Pacific') -> Optional[float]:
        """
        Get just the shipping cost for a PO, optimized for database updates.
        """
        try:
            order_details = self.get_full_order_and_shipping_details_by_po(
                purchase_order_id, order_date_utc_iso, local_tz
            )
            
            if not order_details:
                logger.warning(f"No order found for PO {purchase_order_id}")
                return None
            
            total_cost = order_details.get("totalShippingCost", 0)
            logger.info(f"PO {purchase_order_id} shipping cost: ${total_cost}")
            return float(total_cost)
            
        except Exception as e:
            logger.error(f"Error getting shipping cost for PO {purchase_order_id}: {e}")
            return None

    # Legacy methods for backward compatibility
    def get_orders_by_customer_and_date(self, 
                                      customer_email: str,
                                      order_date_utc_iso: Union[str, datetime] = None, 
                                      purchase_order_id: str = None,
                                      local_tz: str = 'US/Pacific') -> Optional[List[Dict[str, Any]]]:
        """
        Legacy method - now uses PO-based search as fallback.
        """
        # Try the new method first
        if purchase_order_id and order_date_utc_iso:
            logger.info(f"Using PO-based search for {purchase_order_id}")
            order = self.get_full_order_and_shipping_details_by_po(
                purchase_order_id, order_date_utc_iso, local_tz
            )
            
            if order:
                shipments = order.get("shipments", [])
                return [
                    {
                        "orderNumber": order.get("orderNumber"),
                        "purchaseOrderId": purchase_order_id,
                        "shipmentId": shipment.get("shipmentId"),
                        "carrierCode": shipment.get("carrierCode"),
                        "serviceCode": shipment.get("serviceCode"),
                        "shipmentCost": shipment.get("shipmentCost", 0),
                        "shipDate": shipment.get("shipDate"),
                        "trackingNumber": shipment.get("trackingNumber"),
                    }
                    for shipment in shipments
                ]
        
        logger.warning(f"PO-based search failed, no results for {purchase_order_id}")
        return []

    def get_full_order_and_shipping_details(self, order_number: str) -> Optional[Dict[str, Any]]:
        """Get order details by order number (legacy method)."""
        if not order_number or order_number.strip() == "":
            return None
        
        order_url = f"{self.base_url}/orders?orderNumber={order_number}"
        shipment_url = f"{self.base_url}/shipments?orderNumber={order_number}"
        
        try:
            order_data = self.safe_request(order_url)
            orders = order_data.get("orders", [])
            
            if not orders:
                return None
            
            order = orders[0]
            
            t.sleep(0.5)
            shipment_data = self.safe_request(shipment_url)
            shipments = shipment_data.get("shipments", [])
            
            order["shipments"] = [
                {
                    "shipmentId": shipment.get("shipmentId"),
                    "carrierCode": shipment.get("carrierCode"),
                    "serviceCode": shipment.get("serviceCode"),
                    "shipmentCost": shipment.get("shipmentCost", 0),
                    "shipDate": shipment.get("shipDate"),
                    "trackingNumber": shipment.get("trackingNumber"),
                }
                for shipment in shipments
            ]
            
            return order
            
        except Exception as e:
            logger.error(f"Error fetching order {order_number}: {e}")
            return None


# Updated convenience functions
def get_shipping_cost_by_po(purchase_order_id: str, order_date_utc_iso: str, local_tz: str = 'US/Pacific') -> Optional[float]:
    """Convenience function to get shipping cost by PO ID."""
    client = EmailAwareShipStationClient()
    return client.get_shipping_cost_by_po(purchase_order_id, order_date_utc_iso, local_tz)

def get_full_order_by_po(purchase_order_id: str, order_date_utc_iso: str, local_tz: str = 'US/Pacific') -> Optional[Dict[str, Any]]:
    """Convenience function to get full order details by PO ID."""
    client = EmailAwareShipStationClient()
    return client.get_full_order_and_shipping_details_by_po(purchase_order_id, order_date_utc_iso, local_tz)

# Legacy functions for backward compatibility
def safe_request(url, auth, retries=5):
    client = EmailAwareShipStationClient()
    return client.safe_request(url)

def get_full_order_and_shipping_details(order_number):
    client = EmailAwareShipStationClient()
    return client.get_full_order_and_shipping_details(order_number)

def get_orders_by_customer_and_date(customer_email, order_date_utc_iso=None, purchase_order_id=None, local_tz='US/Pacific'):
    client = EmailAwareShipStationClient()
    return client.get_orders_by_customer_and_date(customer_email, order_date_utc_iso, purchase_order_id, local_tz)


# Example usage for your specific case
# if __name__ == "__main__":
#     client = EmailAwareShipStationClient()
    
#     # Your example data
#     po_id = "129025542327956"
#     order_date = "2025-09-11T06:56:11.366Z"
    
#     # Method 1: Get just shipping cost (for database updates)
#     shipping_cost = client.get_shipping_cost_by_po(po_id, order_date)
#     print(f"Shipping cost: ${shipping_cost}")
    
#     # Method 2: Get full order details
#     order_details = client.get_full_order_and_shipping_details_by_po(po_id, order_date)
#     if order_details:
#         print(f"Order {order_details.get('orderNumber')} found")
#         print(f"Customer Email in ShipStation: {order_details.get('customerEmail')}")
#         print(f"Total Shipping Cost: ${order_details.get('totalShippingCost')}")
#     else:
#         print("Order not found")