base_path = 'data/source'
bronze_path = 'data/bronze'
silver_path = 'data/silver'
gold_path = 'data/gold'

dataset = [
    {
        "path": f"{base_path}/olist_geolocation_dataset.csv",
        "table": "geolocation"
    },
    {
        "path": f"{base_path}/olist_order_items_dataset.csv",
        "table": "order_items",
        "primary_key": ["order_id", "order_item_id"]
    },
    {
        "path": f"{base_path}/olist_order_payments_dataset.csv",
        "table": "order_payments",
        "primary_key": ["order_id", "payment_sequential"]
    },
    {
        "path": f"{base_path}/olist_order_reviews_dataset.csv",
        "table": "order_reviews",
        "primary_key": ["review_id"]
    },
    {
        "path": f"{base_path}/olist_orders_dataset.csv",
        "table": "orders",
        "primary_key": ["order_id", "customer_id"]
    },
    {
        "path": f"{base_path}/olist_products_dataset.csv",
        "table": "products",
        "primary_key": ["product_id"]
    },
    {
        "path": f"{base_path}/olist_sellers_dataset.csv",
        "table": "sellers",
        "primary_key": ["seller_id"]
    },
    {
        "path": f"{base_path}/product_category_name_translation.csv",
        "table": "category_name_translate"
    },
    {
        "path": f"{base_path}/olist_customers_dataset.csv",
        "table": "customers",
        "primary_key": ["customer_id", "customer_unique_id"]
    },
]