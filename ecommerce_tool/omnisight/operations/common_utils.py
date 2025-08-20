import re
import threading
import emoji



def calculate_listing_score(product):
    total_rules = 13
    score_per_rule = 10 / total_rules
    passed_rules = 0
    def check_title_strange_symbols(p):
        # Check if the product title contains any emoji
        title = p.get("product_title", "")
        contains_emoji = any(char in emoji.EMOJI_DATA for char in title)
        return not contains_emoji

    def check_title_length(p):
        return len(p.get("product_title", "")) >= 150

    def check_qty_bullets(p):
        return len(p.get("features", [])) >= 5

    def check_length_bullets(p):
        return all(len(bullet) >= 150 for bullet in p.get("features", []))

    def check_capitalized_bullets(p):
        return all(bullet[:1].isupper() for bullet in p.get("features", []) if bullet)

    def check_all_caps_bullets(p):
        return all(not bullet.isupper() for bullet in p.get("features", []))

    def check_ebc_description(p):
        return len(p.get("product_description", "")) >= 1000

    def check_image_resolution(p):
        # Placeholder - Assume pass if image_url is present
        return bool(p.get("image_url"))

    def check_image_background(p):
        # Placeholder - Assume pass if image_url is present
        return bool(p.get("image_url"))

    def check_images_qty(p):
        return len(p.get("image_urls", [])) >= 7

    def check_videos_qty(p):
        # Placeholder - No video data available
        return len(p.get("videos", [])) >= 7

    def check_review_qty(p):
        # Placeholder - No review data available
        return False

    def check_review_rating(p):
        # Placeholder - No review rating available
        return False
    final_checks = []
    checks = [
        check_title_strange_symbols,
        check_title_length,
        check_qty_bullets,
        check_length_bullets,
        check_capitalized_bullets,
        check_all_caps_bullets,
        check_ebc_description,
        check_image_resolution,
        check_image_background,
        check_images_qty,
        check_videos_qty,
        check_review_qty,
        check_review_rating
    ]

    for check in checks:
        c_rule = check(product)
        final_checks.append(c_rule)
        if c_rule:
            passed_rules += 1
    data = {
    "final_score" : round(passed_rules * score_per_rule, 2),
    "rules_checks" : final_checks
    }


    return data

# Example usage:
def assign_listing_score_to_product(product_doc):
    product_dict = product_doc.to_mongo().to_dict()
    score = calculate_listing_score(product_dict)
    product_doc.listing_quality_score = score['final_score']
    product_doc.save()
    return score


# def process_products_in_batches(start_index, end_index, product_list):
#     for i in range(start_index, end_index):
#         if i >= len(product_list):
#             break
#         product = product_list[i]
#         score = assign_listing_score_to_product(product)
#         print(f"Product count {i+1} Product ID: {product.id}, Listing Quality Score: {score}")

# product_list = DatabaseModel.list_documents(Product.objects)
# batch_size = 100
# threads = []

# for start_index in range(0, len(product_list), batch_size):
#     end_index = start_index + batch_size
#     thread = threading.Thread(target=process_products_in_batches, args=(start_index, end_index, product_list))
#     threads.append(thread)
#     thread.start()

# # Wait for all threads to complete
# for thread in threads:
#     thread.join()




