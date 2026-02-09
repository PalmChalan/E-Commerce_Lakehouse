from pyspark.sql.functions import col, first, to_date, year, month, dayofweek
from utils.sparksession import GetSparkSession
from utils.config import silver_path, gold_path

# --- DIMENSIONS ---

def create_dim_products(spark):
    print("Creating Dim Products...")
    df = spark.read.parquet(f"{silver_path}/products")
    
    df.select(
        "product_id", "category", "product_name_lenght",
        "product_description_lenght", "product_photos_qty", 
        "product_weight_g", "product_length_cm", 
        "product_height_cm", "product_width_cm"
    ).write.mode("overwrite").parquet(f"{gold_path}/dim_products")

def create_dim_customers(spark):
    print("Creating Dim Customers...")
    cust = spark.read.parquet(f"{silver_path}/customers")
    geo = spark.read.parquet(f"{silver_path}/geolocation")

    
    geo_agg = geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"),
        first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )

    
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
        .write.mode("overwrite").parquet(f"{gold_path}/dim_customers")

def create_dim_sellers(spark):
    print("Creating Dim Sellers...")
    sellers = spark.read.parquet(f"{silver_path}/sellers")
    geo = spark.read.parquet(f"{silver_path}/geolocation")

    geo_agg = geo.groupBy("geolocation_zip_code_prefix").agg(
        first("geolocation_lat").alias("lat"),
        first("geolocation_lng").alias("lng"),
        first("geolocation_city").alias("city"),
        first("geolocation_state").alias("state")
    )

    sellers.join(geo_agg, sellers.seller_zip_code_prefix == geo_agg.geolocation_zip_code_prefix, "left") \
        .select(
            col("seller_id"), 
            col("seller_zip_code_prefix").alias("zip_code"), 
            col("city"), 
            col("state"), 
            col("lat"), 
            col("lng")
        ) \
        .write.mode("overwrite").parquet(f"{gold_path}/dim_sellers")

# --- FACTS ---

def create_fact_sales(spark):
    print("Creating Fact Sales...")
    orders = spark.read.parquet(f"{silver_path}/orders")
    items = spark.read.parquet(f"{silver_path}/order_items")
    
   
    df = items.join(orders, "order_id", "inner")
    
    
    df = df.withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
           .withColumn("year", year(col("order_purchase_timestamp"))) \
           .withColumn("month", month(col("order_purchase_timestamp"))) \
           .withColumn("weekday", dayofweek(col("order_purchase_timestamp")))

    
    df.select(
        "order_id", "order_item_id", "product_id", "seller_id", "customer_id",
        "price", "freight_value", "order_status", 
        "order_purchase_timestamp", 
        "order_approved_at",
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date",
        "order_date", "year", "month", "weekday"
    ).write.mode("overwrite").parquet(f"{gold_path}/fact_sales")

def create_fact_reviews(spark):
    print("Creating Fact Reviews...")
    reviews = spark.read.parquet(f"{silver_path}/order_reviews")
    
    reviews.select(
        "review_id", "order_id", "review_score", 
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp"
    ).write.mode("overwrite").parquet(f"{gold_path}/fact_reviews")

def create_fact_payments(spark):
    print("Creating Fact Payments...")
    payments = spark.read.parquet(f"{silver_path}/order_payments")
    
    
    payments.select(
        "order_id", "payment_sequential", "payment_type", 
        "payment_installments", "payment_value"
    ).write.mode("overwrite").parquet(f"{gold_path}/fact_payments")

def main():
    spark = GetSparkSession("GoldLayer")
    
    create_dim_products(spark)
    create_dim_customers(spark)
    create_dim_sellers(spark)
    
    create_fact_sales(spark)
    create_fact_reviews(spark)
    create_fact_payments(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()