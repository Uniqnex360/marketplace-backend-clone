from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser

from clickhouse.config import client
from clickhouse.helpers import migrate_product_list_to_clickhouse_task


@csrf_exempt
def create_product_list_schema_clickhouse(request):
    if request.method != "POST":
        return {"error": "Only POST allowed"}

    try:
        query = """
        CREATE TABLE IF NOT EXISTS product_list
        (
            -- Mongo ID
            id String,

            -- Product Identity
            product_id String,
            product_title String,

            sku String,
            master_sku String,
            parent_sku String,

            asin String,

            -- Classification
            category String,
            product_type String,

            brand_id String,
            brand_name String,

            manufacturer_name String,

            -- Inventory & Pricing
            price Float64,
            quantity Int32,

            -- Media
            image_url String,

            -- Marketplace
            marketplace_ids Array(String),
            marketplace_names Array(String),

            -- Audit Fields
            created_at Nullable(DateTime),
            updated_at Nullable(DateTime)
        )
        ENGINE = MergeTree
        ORDER BY (
            product_id,
            sku
        );
        """

        client.command(query)

        return {"success": True, "message": "product_list schema created successfully"}

    except Exception as e:
        return {"success": False, "error": str(e)}


@csrf_exempt
def migrate_product_list_to_clickhouse_view(request):
    """migrate data required for order list to Mongo DB to Clickhouse"""

    data = migrate_product_list_to_clickhouse_task()
    return data


@csrf_exempt
def getProductList_clickhouse(request):

    json_request = JSONParser().parse(request)

    marketplace_id = json_request.get("marketplace_id")
    skip = int(json_request.get("skip") or 0)
    limit = int(json_request.get("limit") or 10)
    search_query = json_request.get("search_query")
    category_name = json_request.get("category_name")
    brand_id_list = json_request.get("brand_id_list")
    sort_by = json_request.get("sort_by")
    sort_by_value = json_request.get("sort_by_value")

    where_conditions = []
    params = {}

    # ---------------------------------
    # MARKETPLACE FILTER
    # ---------------------------------
    if marketplace_id:
        where_conditions.append("has(marketplace_ids, %(marketplace_id)s)")
        params["marketplace_id"] = marketplace_id

    # ---------------------------------
    # CATEGORY FILTER
    # ---------------------------------
    if category_name:

        if isinstance(category_name, list):
            category_values = "', '".join(category_name)

            where_conditions.append(f"category IN ('{category_values}')")

        else:
            where_conditions.append("category = %(category)s")
            params["category"] = category_name

    # ---------------------------------
    # BRAND FILTER
    # ---------------------------------
    if brand_id_list:

        brand_values = "', '".join(brand_id_list)

        where_conditions.append(f"brand_id IN ('{brand_values}')")

    # ---------------------------------
    # SEARCH
    # ---------------------------------
    if search_query:

        search_query = search_query.strip()

        where_conditions.append("""
            (
                lower(product_title) LIKE lower(%(search)s)
                OR
                lower(sku) LIKE lower(%(search)s)
            )
            """)

        params["search"] = f"%{search_query}%"

    # ---------------------------------
    # WHERE
    # ---------------------------------
    where_sql = ""

    if where_conditions:
        where_sql = "WHERE " + " AND ".join(where_conditions)

    # ---------------------------------
    # SORT
    # ---------------------------------
    allowed_sorts = {
        "product_title",
        "price",
        "quantity",
        "created_at",
        "updated_at",
        "brand_name",
        "category",
    }

    order_sql = ""

    if sort_by in allowed_sorts:

        direction = "DESC"

        if str(sort_by_value) == "1":
            direction = "ASC"

        order_sql = f"""
            ORDER BY {sort_by} {direction}
        """

    else:
        order_sql = """
            ORDER BY product_title DESC
        """

    # ---------------------------------
    # PRODUCT QUERY
    # ---------------------------------
    query = f"""
    SELECT
        id,
        product_title,
        product_id,
        sku,
        asin,
        price,
        quantity,
        category,
        image_url,
        marketplace_ids,
        marketplace_names
    FROM product_list
    {where_sql}
    {order_sql}
    LIMIT %(limit)s
    OFFSET %(skip)s
    """

    params["limit"] = limit
    params["skip"] = skip

    result = client.query(query, parameters=params)

    columns = result.column_names

    product_list = [dict(zip(columns, row)) for row in result.result_rows]

    # ---------------------------------
    # MATCH OLD RESPONSE
    # ---------------------------------
    for product in product_list:

        product["marketplace_ins"] = product.get(
            "marketplace_names",
            [],
        )

        product["marketplace_image_url"] = []

        product.pop("marketplace_names", None)
        product.pop("marketplace_ids", None)

    # ---------------------------------
    # COUNT QUERY
    # ---------------------------------
    count_query = f"""
    SELECT count()
    FROM product_list
    {where_sql}
    """

    total_count = client.query(
        count_query,
        parameters=params,
    ).first_row[0]

    return {
        "total_count": total_count,
        "product_list": product_list,
    }
