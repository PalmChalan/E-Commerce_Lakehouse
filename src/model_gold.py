import logging
from pyspark.sql.functions import col, first, to_date, year, month, dayofweek
from utils.config import silver_path, gold_path

logger = logging.getLogger(__name__)

# --- DIMENSIONS ---

def create_dim_products(spark):
    '''Function to create dim_products table'''
    logger.info("Creating Dim Products...")
    df = spark.read.format("delta").load(f"{silver_path}/products")
    
    df.select(
        "product_id", "category", "product_name_lenght",
        "product_description_lenght", "product_photos_qty", 
        "product_weight_g", "product_length_cm", 
        "product_height_cm", "product_width_cm"
    ).write.format("delta").mode("overwrite").save(f"{gold_path}/dim_products")

    logger.info("Dim_products created")

def create_dim_customers(spark):
    '''Function to create dim_customers table'''
    logger.info("Creating Dim Customers...")
    cust = spark.read.format("delta").load(f"{silver_path}/customers")
    geo = spark.read.format("delta").load(f"{silver_path}/geolocation")

    # Aggregate with geolocation table to get latitude / longitude
    geo_agg = geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"),
        first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )

    # Join and keep zip code
    cust.join(geo_agg, cust.customer_zip_code_prefix == geo_agg.geolocation_zip_code_prefix, "left") \
        .select(
            col("customer_id"), 
            col("customer_unique_id"), 
            col("customer_zip_code_prefix").alias("zip_code"),
            col("city"), 
            col("state"), 
            col("lat"), 
            col("lng")
        ) \
        .write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")
    
    logger.info("Dim_customers created")

def create_dim_sellers(spark):
    '''Function to create dim_sellers table'''
    logger.info("Creating Dim Sellers...")
    sellers = spark.read.format("delta").load(f"{silver_path}/sellers")
    geo = spark.read.format("delta").load(f"{silver_path}/geolocation")

    # Aggregate with geolocation table to get latitude / longitude
    geo_agg = geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"),
        first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )

    # Join and keep zip code
    sellers.join(geo_agg, sellers.seller_zip_code_prefix == geo_agg.geolocation_zip_code_prefix, "left") \
        .select(
            col("seller_id"), 
            col("seller_zip_code_prefix").alias("zip_code"), 
            col("city"), 
            col("state"), 
            col("lat"), 
            col("lng")
        ) \
        .write.format("delta").mode("overwrite").save(f"{gold_path}/dim_sellers")
    
    logger.info("Dim_sellers created")

# --- FACTS ---

def create_fact_sales(spark):
    '''Function to create fact_sales table'''
    logger.info("Creating Fact Sales...")
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    items = spark.read.format("delta").load(f"{silver_path}/order_items")
    
    # Inner Join: We only want items that belong to valid orders
    df = items.join(orders, "order_id", "inner")
    
    # Add Time-based logic for filtering
    df = df.withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
           .withColumn("year", year(col("order_purchase_timestamp"))) \
           .withColumn("month", month(col("order_purchase_timestamp"))) \
           .withColumn("weekday", dayofweek(col("order_purchase_timestamp")))

    # Select all lifecycle timestamps
    df.select(
        "order_id", "order_item_id", "product_id", "seller_id", "customer_id",
        "price", "freight_value", "order_status", 
        "order_purchase_timestamp", 
        "order_approved_at",
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date",
        "order_date", "year", "month", "weekday"
    ).write.format("delta").mode("overwrite").save(f"{gold_path}/fact_sales")

    logger.info("Fact_sales created")

def create_fact_reviews(spark):
    '''Function to create fact_reviews table'''
    logger.info("Creating Fact Reviews...")
    reviews = spark.read.format("delta").load(f"{silver_path}/order_reviews")
    
    reviews.select(
        "review_id", "order_id", "review_score", 
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp"
    ).write.format("delta").mode("overwrite").save(f"{gold_path}/fact_reviews")

    logger.info("Fact_reviews created")

def create_fact_payments(spark):
    '''Function to create fact_payments table'''
    logger.info("Creating Fact Payments...")
    payments = spark.read.format("delta").load(f"{silver_path}/order_payments")
    
    payments.select(
        "order_id", "payment_sequential", "payment_type", 
        "payment_installments", "payment_value"
    ).write.format("delta").mode("overwrite").save(f"{gold_path}/fact_payments")

    logger.info("Fact_payments created")

def CreateModel(spark): 
    logger.info("Starting Gold layer...")
    try:
        create_dim_products(spark)
        create_dim_customers(spark)
        create_dim_sellers(spark)
    
        create_fact_sales(spark)
        create_fact_reviews(spark)
        create_fact_payments(spark)
    except Exception as e:
        logger.exception(f"Gold layer error: {e}")
        raise e
    logger.info("Gold layer completed")