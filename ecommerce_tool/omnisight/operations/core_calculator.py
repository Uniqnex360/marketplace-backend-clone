from __future__ import annotations
import math

from    omnisight.models import OrderItems
class EcommerceCalculator:
    @staticmethod
    def safe_float(value,default=0.0):
        if value is None or math.isnan(value):
            return default
        return float(value)
    
    @staticmethod
    def calculate_order_metrics(orders,item_details_map,include_breakdown=False):
        gross_revenue=total_cogs=channel_fee=net_profit=total_units=vendor_funding=vendor_discount=temp_price=refund=tax_price=shipping_cost=promotion_discount=ship_promotion_discount=total_product_cost=0
        sku_set=set()
        p_id=set()
        unique_order_ids=set()
        product_categories={}
        product_completeness={'complete':0,'incomplete':0}
        base_result = {
        "gross_revenue_with_tax": 0,
        'expenses': 0,
        'referral_fee': 0,
        'net_profit': 0,
        "roi": 0,
        "total_units": 0,
        "refund": 0,
        "skuCount": 0,
        'margin': 0,
        'total_orders': 0,
        'total_tax': 0,
        'total_cogs': 0,
        'product_cost': 0,
        "shipping_cost": 0,
        "base_price": 0,
        'promotion_discount': 0,
        "ship_promotion_discount": 0,
        'vendor_funding': 0,
        'vendor_discount': 0
    }
        for order in orders:
            po_id=order.get('purchase_order_id')
            if po_id:
                unique_order_ids.add(po_id)
            gross_revenue+=EcommerceCalculator.safe_float(order.get('original_order_total',0))
            shipping_cost+=EcommerceCalculator.safe_float(order.get('shipping_price',0))
            for item_id in order.get('order_items',[]):
                item_data=item_details_map.get(str(item_id)) or item_details_map.get(item_id)
                if not item_data:
                    continue
                price=EcommerceCalculator.safe_float(item_data.get('price',0))
                if price==0 and 'charges' in item_data:
                    price=sum(EcommerceCalculator.safe_float(charge.get('chargeAmount',0)) for charge in item_data['charges'])
                temp_price+=price
                tax_price+=EcommerceCalculator.safe_float(item_data.get('tax_price',0))
                product_cost=EcommerceCalculator.safe_float(item_data.get('product_cost',0))
                quantity=int(item_data.get('QuantityOrdered',1)or 1)
                total_units+=quantity
                total_cogs+=product_cost*quantity
                channel_fee+=EcommerceCalculator.safe_float(item_data.get('referral_fee',0))*quantity
                promotion_discount+=EcommerceCalculator.safe_float(item_data.get('promotion_discount',0))
                ship_promotion_discount+=EcommerceCalculator.safe_float(item_data.get('ship_promotion_discount',0))
                vendor_funding+=EcommerceCalculator.safe_float(item_data.get('vendor_funding',0))*quantity
                total_product_cost+=product_cost*quantity
                if item_data.get('sku'):
                    sku_set.add(item_data['sku'])
                category=item_data.get('category',"Unknown")
                product_categories[category]=product_categories.get(category,0)+1
                if item_data.get('price') and item_data.get('product_cost') and item_data.get('sku'):
                    product_completeness['complete']+=1
                else:
                    product_completeness['incomplete']+=1
                try:
                    p_id.add(item_data['p_id'])
                except:
                    pass
            fulfillment_channel=order.get('fulfillment_channel','')
            merchant_shipment_cost=EcommerceCalculator.safe_float(order.get('merchant_shipment_cost',0))
            if merchant_shipment_cost is None:
                if fulfillment_channel=='AFN':
                    merchant_shipment_cost=EcommerceCalculator.safe_float(order.get('shipping_price',0))
                elif fulfillment_channel in ['MFN','SellerFulfilled']:
                    merchant_shipment_cost=EcommerceCalculator.safe_float(order.get('merchant_shipment_cost',0))
                else:
                    merchant_shipment_cost=EcommerceCalculator.safe_float(order.get('merchant_shipment_cost',0))
            total_cogs+=merchant_shipment_cost
            net_profit=(temp_price+shipping_cost+promotion_discount+vendor_funding-(channel_fee+total_cogs+vendor_discount+ship_promotion_discount+refund))
            margin=(net_profit/gross_revenue)*100 if gross_revenue>0 and not math.isnan(gross_revenue) else 0
            base_result={
                        "gross_revenue_with_tax":round(gross_revenue,2),
                        'expenses':round(total_cogs+channel_fee,2),
                        'referral_fee':round(channel_fee,2),
                        'net_profit':round(net_profit,2),
                        "roi":round((net_profit/total_cogs)*100,2) if total_cogs!=0 else 0,
                        "total_units":total_units,
                        "refund":refund,
                        "skuCount":len(sku_set),
                        'margin':round(margin,2),
                        'total_orders':len(unique_order_ids),
                        'total_tax':round(tax_price,2),
                        'total_cogs':round(total_cogs,2),
                        'product_cost':round(total_product_cost,2),
                        "shipping_cost":round(shipping_cost,2),
                        "base_price":round(temp_price,2),
                        'promotion_discount':round(promotion_discount,2),
                        "ship_promotion_discount":round(ship_promotion_discount,2),
                        'vendor_funding':round(vendor_funding,2),
                        'vendor_discount':round(vendor_discount,2)
                    }
        if include_breakdown:
            base_result.update({'productCategories':product_categories,'productCompleteness':product_completeness})
                        
        return base_result

    @staticmethod
    def get_item_details_bulk(all_item_ids):
        pipeline = [
                {"$match": {"_id": {"$in": all_item_ids}}},
                {"$lookup": {
                    "from": "product",
                    "localField": "ProductDetails.product_id",
                    "foreignField": "_id",
                    "as": "product_ins"
                }},
                {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                {"$project": {
                    "p_id": "$product_ins._id",
                    "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                    "product_cost": {"$round":[{"$ifNull": ["$product_ins.product_cost", 0]},2]},
                    "promotion_discount": {"$ifNull": ["$Pricing.PromotionDiscount.Amount", 0]},
                    "ship_promotion_discount": {"$ifNull": ["$Pricing.ShipPromotionDiscount.Amount", 0]},
                    "vendor_discount": {"$ifNull": ["$product_ins.vendor_discount", 0]},
                    "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    "referral_fee": {"$round":[{"$ifNull": ["$product_ins.referral_fee", 0]},2]},
                    "walmart_fee": {"$ifNull": ["$product_ins.walmart_fee", 0]},
                    "QuantityOrdered": {"$ifNull": ["$ProductDetails.QuantityOrdered", 1]},
                }}
            ]
        item_results=list(OrderItems.objects.aggregate(*pipeline))
        return{str(item['_id']):item for item in item_results}