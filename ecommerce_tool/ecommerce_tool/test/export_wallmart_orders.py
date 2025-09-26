import requests
import uuid
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from omnisight.operations.walmart_utils import oauthFunction
import os
from ecommerce_tool.util.shipping_price import get_full_order_and_shipping_details,get_orders_by_customer_and_date

def getWalmartOrderDetails(purchase_order_id):
    try:
        access_token = oauthFunction()
        if not access_token:
            print("❌ Failed to get access token")
            return None

        url = f"https://marketplace.walmartapis.com/v3/orders/{purchase_order_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "WM_SEC.ACCESS_TOKEN": access_token,
            "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
            "WM_SVC.NAME": "Walmart Marketplace",
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200:
            order = response.json().get('order', {})
            print(f"✅ Order Details for: {purchase_order_id}\n")
            print(json.dumps(order, indent=2))  

            total_shipping = 0
            for line in order.get('orderLines', {}).get('orderLine', []):
                charges = line.get('charges', {}).get('charge', [])
                for charge in charges:  
                    charge_type = charge.get('chargeType')
                    amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    if charge_type == 'SHIPPING':
                        total_shipping += amount
                        print(f"🚚 Shipping Charge (Line): ${amount}")

            if total_shipping > 0:
                print(f"💰 Total Shipping Charge: ${total_shipping}")
            else:
                print("ℹ️ No shipping charges found")

            return order
        else:
            print(f"❌ Error fetching order: [HTTP {response.status_code}] {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
        return None
    except ValueError as e:
        print(f"❌ JSON parsing error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def fetchTodayWalmartOrdersCount(exclude_cancelled=True, min_order_value=0):
    access_token = oauthFunction()
    if not access_token:
        print(" Failed to get access token")
        return None

    
    pacific = pytz.timezone("US/Pacific")
    now_pacific = datetime.now(pacific)
    start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)

    
    start_date_utc = start_of_day.astimezone(pytz.utc)
    end_date_utc = end_of_day.astimezone(pytz.utc)
    start_date = start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}&limit=100"
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"Fetching Walmart orders for today")
    print(f" Pacific Time: {start_of_day.strftime('%Y-%m-%d %H:%M:%S')} → {end_of_day.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" UTC Time: {start_date} → {end_date}")

    while True:
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        print(f"📡 Fetching page {page}...")
        response = requests.get(paged_url, headers=headers)

        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            if not new_orders:
                print(" No more orders found")
                break

            fetched_orders.extend(new_orders)
            print(f" Found {len(new_orders)} orders on page {page}")
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                print("Reached last page")
                break

            page += 1
        else:
            print(f" Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    print(f"\n Initial orders fetched: {len(fetched_orders)}")

    
    filtered_orders = []
    cancelled_count = 0
    zero_value_count = 0

    for order in fetched_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        if not order_lines:
            continue

        all_cancelled = True
        order_total = 0

        for line in order_lines:
            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            current_status = ''
            if statuses:
                first_status = statuses[0]
                status_val = first_status.get('status', '') if isinstance(first_status, dict) else str(first_status)
                current_status = status_val if isinstance(status_val, str) else status_val.get('status', '')

            if current_status.upper() not in ['CANCELLED', 'CANCELED']:
                all_cancelled = False

            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    price = float(charge.get('chargeAmount', {}).get('amount', 0))
                    qty = int(line.get('orderLineQuantity', {}).get('amount', 0))
                    order_total += price * qty

        if exclude_cancelled and all_cancelled:
            cancelled_count += 1
            continue
        if order_total <= min_order_value:
            zero_value_count += 1
            continue

        filtered_orders.append(order)

    
    total_orders = len(filtered_orders)
    total_order_items = 0
    total_revenue = 0
    rows = []

    for order in filtered_orders:
        order_id = order.get("purchaseOrderId")
        customer_id = order.get("customerOrderId")
        order_date_ms = int(order.get("orderDate", 0))
        order_date = datetime.fromtimestamp(order_date_ms / 1000).astimezone(pacific).strftime("%Y-%m-%d %H:%M:%S")

        for line in order.get("orderLines", {}).get("orderLine", []):
            sku = line.get("item", {}).get("sku")
            name = line.get("item", {}).get("productName")
            qty = int(line.get("orderLineQuantity", {}).get("amount", 0))
            total_order_items += qty

            product_price = 0
            shipping_price = 0
            charges = line.get("charges", {}).get("charge", [])
            for charge in charges:
                if charge.get("chargeType") == "PRODUCT":
                    product_price = float(charge.get("chargeAmount", {}).get("amount", 0))
                elif charge.get("chargeType") == "SHIPPING":
                    shipping_price = float(charge.get("chargeAmount", {}).get("amount", 0))

            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            line_status = 'Unknown'
            if statuses:
                status_val = statuses[0].get('status', '') if isinstance(statuses[0], dict) else str(statuses[0])
                line_status = status_val if isinstance(status_val, str) else status_val.get('status', '')

            total_revenue += product_price * qty

            rows.append({
                "Order ID": order_id,
                "Customer ID": customer_id,
                "Order Date (Pacific)": order_date,
                "SKU": sku,
                "Product Name": name,
                "Quantity": qty,
                "Unit Price": product_price,
                "Shipping Price": shipping_price,
                "Line Total": qty * product_price,
                "Line Status": line_status
            })

    
    df = pd.DataFrame(rows)
    filename = f"walmart_orders_{now_pacific.strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(filename, index=False)
    print(f"\n Exported to Excel: {filename}")

    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

    print(f"\n Total Orders: {total_orders}")
    print(f"Total Order Items: {total_order_items}")
    print(f" Total Revenue: ${total_revenue:,.2f}")
    print(f" Average Order Value: ${avg_order_value:,.2f}")

    return {
        "total_orders": total_orders,
        "total_order_items": total_order_items,
        "total_revenue": total_revenue,
        "average_order_value": avg_order_value,
        "filtered_orders": filtered_orders,
        "excel_file": filename
    }


def countWalmartOrdersByDateRange(start_date, end_date, timezone="US/Pacific", exclude_cancelled=True, min_order_value=0):
    from mongoengine import connect
    from omnisight.models import Order, Product
    import pandas as pd
    import pytz
    import uuid
    import requests
    from datetime import datetime

    
    connect(
        db='ecommerce_db',
        host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
        port=27017
    )

    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    tz = pytz.timezone(timezone)
    start_dt = tz.localize(datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0))
    end_dt = tz.localize(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59))

    start_date_utc = start_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_utc = end_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    url = f"{base_url}?createdStartDate={start_date_utc}&createdEndDate={end_date_utc}&limit=100"

    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📦 Fetching Walmart orders from {start_date} to {end_date} ({timezone})...")

    while True:
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        response = requests.get(paged_url, headers=headers)

        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            if not new_orders:
                break

            fetched_orders.extend(new_orders)
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break
            page += 1
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    
    filtered_orders = []
    cancelled_count = 0
    zero_value_count = 0

    for order in fetched_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        if not order_lines:
            continue

        all_cancelled = True
        order_total = 0

        for line in order_lines:
            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            current_status = ''
            if statuses:
                first_status = statuses[0]
                if isinstance(first_status, dict):
                    status_value = first_status.get('status', '')
                    current_status = status_value.get('status', '') if isinstance(status_value, dict) else str(status_value)
                else:
                    current_status = str(first_status)

            if current_status.upper() not in ['CANCELLED', 'CANCELED']:
                all_cancelled = False

            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    qty = int(line.get('orderLineQuantity', {}).get('amount', 0))
                    order_total += amount * qty

        if exclude_cancelled and all_cancelled:
            cancelled_count += 1
            continue

        if order_total <= min_order_value:
            zero_value_count += 1
            continue

        filtered_orders.append(order)

    
    def get_merchant_shipment_cost(walmart_order):
        from datetime import datetime
        from omnisight.models import Order

        order_id = walmart_order.get("purchaseOrderId")
        if not order_id:
            return 0.0

        try:
            order_doc = Order.objects(purchase_order_id=order_id).first()
            if order_doc:
                merchant_cost = getattr(order_doc, 'merchant_shipment_cost', 0.0)
                if merchant_cost:
                    return merchant_cost
        except Exception as e:
            print(f"❌ Error fetching from DB for Order ID '{order_id}':", e)

        return 0.0
    def get_product_details_from_db(sku):
        try:
            product = Product.objects(sku=sku).first()
            if product:
                return {
                    'product_cost': getattr(product, 'w_product_cost', 0.0) or getattr(product, 'product_cost', 0.0) or getattr(product, 'cogs', 0.0),
                    'walmart_fee': getattr(product, 'walmart_fee', 0.0) or getattr(product, 'referral_fee', 0.0),
                    'vendor_funding': getattr(product, 'vendor_funding', 0.0),
                    'vendor_discount': getattr(product, 'vendor_discount', 0.0)
                }
        except Exception as e:
            print(f"❌ Error fetching product details for SKU {sku}: {e}")
        
        return {
            'product_cost': 0.0,
            'walmart_fee': 0.0,
            'vendor_funding': 0.0,
            'vendor_discount': 0.0
        }

    # Process orders and build detailed rows
    total_orders = len(filtered_orders)
    total_order_items = 0
    total_revenue = 0.0
    total_merchant_shipping = 0.0
    total_product_cost = 0.0
    total_walmart_fees = 0.0
    total_vendor_funding = 0.0
    rows = []

    
    for order in filtered_orders:
        order_id = order.get("purchaseOrderId")
        customer_id = order.get("customerOrderId")
        order_date_ms = int(order.get("orderDate", 0))
        order_date_local = datetime.fromtimestamp(order_date_ms / 1000).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")

        merchant_cost = get_merchant_shipment_cost(order)
        total_merchant_shipping += merchant_cost

        for line in order.get("orderLines", {}).get("orderLine", []):
            sku = line.get("item", {}).get("sku")
            name = line.get("item", {}).get("productName")
            qty = int(line.get("orderLineQuantity", {}).get("amount", 0))
            total_order_items += qty

            # Get product details from database
            product_details = get_product_details_from_db(sku)

            product_price = 0
            shipping_price = 0
            tax_amount = 0

            for charge in line.get("charges", {}).get("charge", []):
                charge_type = charge.get("chargeType", "").upper()
                amount = float(charge.get("chargeAmount", {}).get("amount", 0))

                if charge_type == "PRODUCT":
                    product_price = amount
                elif charge_type == "SHIPPING":
                    shipping_price = amount
                
                # Calculate tax from charge
                tax_info = charge.get('tax', {})
                if tax_info:
                    tax_amount += float(tax_info.get('taxAmount', {}).get('amount', 0) or 0)

            # Get line status
            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            line_status = 'Unknown'
            if statuses:
                status_val = statuses[0].get('status', '') if isinstance(statuses[0], dict) else str(statuses[0])
                line_status = status_val if isinstance(status_val, str) else status_val.get('status', '')

            # Calculate totals
            line_total = (product_price * qty) + tax_amount
            total_revenue += line_total

            # Calculate costs and fees for this line
            line_product_cost = (product_details['product_cost'] + merchant_cost) * qty

            line_walmart_fee = (product_price * qty) * (product_details['walmart_fee'] / 100) if product_details['walmart_fee'] > 0 else 0
            line_vendor_funding = product_details['vendor_funding'] * qty

            total_product_cost += line_product_cost
            total_walmart_fees += line_walmart_fee
            total_vendor_funding += line_vendor_funding
            total_expenses = total_product_cost + total_walmart_fees


            rows.append({
                "Order ID": order_id,
                "Customer ID": customer_id,
                "Order Date": order_date_local,
                "SKU": sku,
                "Product Name": name,
                "Quantity": qty,
                "Status": line_status,
                "Product Cost (Procurement Price)": product_details['product_cost'],
                "Ship Cost (Merchant Cost)": merchant_cost / len(order.get("orderLines", {}).get("orderLine", [])) if len(order.get("orderLines", {}).get("orderLine", [])) > 0 else 0,  # Distribute merchant cost across items
                "Product Price (Customer Price)": product_price,
                "Shipping Cost (Taken from ShipStation)": shipping_price,
                "Funding": product_details['vendor_funding'],
                "Walmart Fee": line_walmart_fee / qty if qty > 0 else 0,  # Per unit fee
                "Tax": tax_amount,

                "Line Total": line_total,
                "Line Product Cost Total": line_product_cost,
                "Line Walmart Fee Total": line_walmart_fee,
                "Line Vendor Funding Total": line_vendor_funding
            })

    # Create DataFrame and export
    df = pd.DataFrame(rows)
    filename = f"walmart_orders_detailed_{start_date}_to_{end_date}.xlsx"
    df.to_excel(filename, index=False)
    print(f"\n📂 Exported to Excel: {filename}")

    # Calculate enhanced metrics
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    net_revenue = total_revenue - total_merchant_shipping
    gross_profit = total_revenue - total_product_cost - total_walmart_fees - total_merchant_shipping
    profit_margin = (gross_profit / total_revenue * 100) if total_revenue > 0 else 0

    print(f"\n📊 ENHANCED SUMMARY:")
    print(f"📦 Total Orders: {total_orders}")
    print(f"📦 Total Order Items: {total_order_items}")
    print(f"💰 Total Revenue: ${total_revenue:,.2f}")
    print(f"🏷️ Total Product Costs: ${total_product_cost:,.2f}")
    print(f"🚚 Total Merchant Shipping Costs: ${total_merchant_shipping:,.2f}")
    print(f"💳 Total Walmart Fees: ${total_walmart_fees:,.2f}")
    print(f"💎 Total Vendor Funding: ${total_vendor_funding:,.2f}")
    print(f"💰 Net Revenue (after shipping): ${net_revenue:,.2f}")
    print(f"📈 Gross Profit: ${gross_profit:,.2f}")
    print(f"📊 Profit Margin: {profit_margin:.2f}%")
    print(f"📊 Average Order Value: ${avg_order_value:,.2f}")

    return {
        "total_orders": total_orders,
        "total_order_items": total_order_items,
        "total_revenue": total_revenue,
        "total_product_cost": total_product_cost,
        "total_merchant_shipping": total_merchant_shipping,
        "total_walmart_fees": total_walmart_fees,
        "total_vendor_funding": total_vendor_funding,
        "net_revenue": net_revenue,
        "gross_profit": gross_profit,
        "profit_margin": profit_margin,
        "average_order_value": avg_order_value,
        "expenses":total_expenses,
        "filtered_orders": filtered_orders,
        "excel_file": filename,
        "detailed_rows": rows
    }

if __name__ == "__main__":
    result = countWalmartOrdersByDateRange(
        start_date="2025-08-11",
        end_date="2025-08-11",
        timezone="US/Pacific",
        exclude_cancelled=True,
        min_order_value=0
    )