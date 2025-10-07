from pymongo import MongoClient

client = MongoClient('mongodb://plmp_admin:admin%401234@54.86.75.104:27017/')
db = client['ecommerce_db']
orders = db['order']

query = {
    "fulfillment_channel": "SellerFulfilled",
    "order_total": 0
}

batch_size = 2000  
skip = 0
updated_count = 0

while True:
    orders_batch = list(orders.find(query).sort("_id",-1).skip(skip).limit(batch_size))
    if not orders_batch:
        break

    for order in orders_batch:
        try:
            order_id = order['_id']
            order_details = order.get('order_details', [])
            if not isinstance(order_details, list):
                order_details = []
            total = 0.0
            for line in order_details:
                charges = line.get('charges', {}).get('charge', [])
                for charge in charges:
                    item_price = charge.get('chargeAmount', {}).get('amount', 0) or 0
                    tax = charge.get('tax') or {}
                    tax_amount = tax.get('taxAmount', {}).get('amount', 0) or 0
                    total += item_price + tax_amount
            shipping_price = order.get('shipping_price', 0) or 0
            total += shipping_price

            total = round(total, 2)

            orders.update_one(
                {'_id': order_id},
                {'$set': {'order_total': total}}
            )
            updated_count += 1
            if updated_count % 100 == 0:
                print(f"Updated {updated_count} orders so far...")
        except Exception as e:
            print(f"Error updating order {order.get('_id')}: {e}")

    skip += batch_size

print(f"Done. Updated total {updated_count} orders.")
