base_path = 'data/source'
bronze_path = 'data/bronze'

dataset = [
    {
        "path": f"{base_path}/olist_geolocation_dataset.csv",
        "table": "geolocation"
    },
    {
        "path": f"{base_path}/olist_order_items_dataset.csv",
        "table": "order_items"
    },
    {
        "path": f"{base_path}/olist_order_payments_dataset.csv",
        "table": "order_payments"
    },
    {
        "path": f"{base_path}/olist_order_reviews_dataset.csv",
        "table": "order_reviews"
    },
    {
        "path": f"{base_path}/olist_orders_dataset.csv",
        "table": "orders"
    },
    {
        "path": f"{base_path}/olist_products_dataset.csv",
        "table": "products"
    },
    {
        "path": f"{base_path}/olist_sellers_dataset.csv",
        "table": "sellers"
    },
    {
        "path": f"{base_path}/product_category_name_translation.csv",
        "table": "category_name_translate"
    },
    {
        "path": f"{base_path}/olist_customers_dataset.csv",
        "table": "customers"
    },
]