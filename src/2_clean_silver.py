from pyspark.sql.functions import col, lower, to_timestamp, translate, trim
from pyspark.sql.types import IntegerType
from utils.sparksession import GetSparkSession
from utils.config import bronze_path, silver_path, dataset


def clean_orders(spark):
    """
    Specific function for orders data, it does the following:
    1. Fix timestamp string
    2. Remove null and duplicates (considering on primary key)
    3. Save to silver folder
    """
    print("Processing Orders...")
    
    # Read from Bronze
    input_path = f"{bronze_path}/orders"
    df = spark.read.parquet(input_path)

    # TRANSFORMATION: Convert String -> Timestamp
    # The raw format is "yyyy-MM-dd HH:mm:ss"
    timestamp_cols = [
        "order_purchase_timestamp", 
        "order_approved_at", 
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]

    for column in timestamp_cols:
        df = df.withColumn(column, to_timestamp(col(column)))

    # Save to Silver
    output_path = f"{silver_path}/orders"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved Cleaned Orders to {output_path}")

def clean_products(spark):
    """
    Specific function for products data, it does the following:
    1. Join products with translation to get English product category name
    2. Remove Portuguese column
    3. Remove null and duplicates (considering on primary key)
    4. Save to silver folder
    """
    print("Processing Products...")
    
    # Read Products and Translations dataset
    products_df = spark.read.parquet(f"{bronze_path}/products")
    translate_df = spark.read.parquet(f"{bronze_path}/category_name_translate")

    # TRANSFORMATION: Join to get English Name
    joined_df = products_df.join(
        translate_df, 
        products_df.product_category_name == translate_df.product_category_name, 
        "left"
    )

    # TRANSFORMATION: Select only clean columns (Drop Portuguese name)
    final_df = joined_df.select(
        col("product_id"),
        col("product_category_name_english").alias("category"), # Rename for clarity
        col("product_name_lenght"),
        col("product_description_lenght"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )

    number_cols = [
        "product_name_lenght", 
        "product_description_lenght", 
        "product_photos_qty", 
        "product_weight_g", 
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    # Convert string -> integer
    for column in number_cols:
        final_df = final_df.withColumn(column, col(column).cast(IntegerType()))

    # Save to Silver
    output_path = f"{silver_path}/products"
    final_df.write.mode("overwrite").parquet(output_path)
    print(f"Saved Cleaned Products to {output_path}")

def clean_order_items(spark):

    input_path = f"{bronze_path}/order_items"
    df = spark.read.parquet(input_path)

    number_cols = ["price", "freight_value"]
    timestamp_col = "shipping_limit_date"

    for column in number_cols:
        df = df.withColumn(column, col(column).cast("double"))
    
    df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col)))

    df = df.filter(col("price") >= 0)

    output_path = f"{silver_path}/order_items"
    df.write.mode("overwrite").parquet(output_path)

def clean_order_payments(spark):

    input_path = f"{bronze_path}/order_payments"
    df = spark.read.parquet(input_path)

    decimal_col = "payment_value"
    int_cols = ["payment_sequential", "payment_installments"]

    df = df.withColumn(decimal_col, col(decimal_col).cast("double"))
    for column in int_cols:
        df = df.withColumn(column, col(column).cast(IntegerType()))

    df = df.filter(col("payment_value") >= 0)

    output_path = f"{silver_path}/order_payments"
    df.write.mode("overwrite").parquet(output_path)

def clean_order_reviews(spark):

    input_path = f"{bronze_path}/order_reviews"
    df = spark.read.parquet(input_path)

    number_col = "review_score"
    timestamp_cols = ["review_creation_date", "review_answer_timestamp"]

    df = df.withColumn(number_col, col(number_col).cast(IntegerType()))
    for column in timestamp_cols:
        df = df.withColumn(column, to_timestamp(col(column)))

    df = df.filter(col("review_id").rlike("^[a-f0-9]{32}$"))

    output_path = f"{silver_path}/order_reviews"
    df.write.mode("overwrite").parquet(output_path)

def removeAccent(spark):
    folder = ["customers", "geolocation", "sellers"]
    columns = ["customer_city", "geolocation_city", "seller_city"]
    accents = "áéíóúÁÉÍÓÚãõÃÕâêîôûÂÊÎÔÛçÇ"
    no_accents = "aeiouAEIOUaoAOaeiouAEIOUcC"

    for i in range(len(folder)):
        input_path = f"{bronze_path}/{folder[i]}"
        df = spark.read.parquet(input_path)
        df = df.withColumn(columns[i], translate(col(columns[i]), accents, no_accents))
        df = df.withColumn(columns[i], lower(trim(col(columns[i]))))

        output_path = f"{silver_path}/{folder[i]}"
        df.write.mode("overwrite").parquet(output_path)


def remove_DuplicateAndNull(spark):
    '''
    Function to remove duplicate and null data (based on primary key) for the remaining dataset that had not been cleaned yet
    '''

    not_cleaned = ["category_name_translate"]
    for item in dataset:
        if item["table"] not in not_cleaned:           
            print(f"Cleaning table: {item['table']}")
            df = spark.read.parquet(f"{silver_path}/{item['table']}")
            original = df.count()
            if item.get('primary_key'):
                print(item['primary_key'])
                df = df.dropDuplicates(item['primary_key'])
                df = df.dropna(subset=item['primary_key'])
            else:
                df = df.dropDuplicates()
                df = df.dropna()

            final = df.count()
            dropped_count = original - final
            print(f"dropped {dropped_count} row(s)")
            output_path = f"{silver_path}/{item['table']}"
            df.write.mode("overwrite").parquet(output_path)
        else:
            print(f"Cleaning table: {item['table']}")
            df = spark.read.parquet(f"{bronze_path}/{item['table']}")
            df = df.dropDuplicates()
            df = df.dropna()
            output_path = f"{silver_path}/{item['table']}"
            df.write.mode("overwrite").parquet(output_path)
        


def main():
    spark = GetSparkSession("Silver")
    
    clean_orders(spark)
    clean_products(spark)
    clean_order_items(spark)
    clean_order_payments(spark)
    clean_order_reviews(spark)
    removeAccent(spark)
    remove_DuplicateAndNull(spark)
    
    
    spark.stop()

if __name__ == "__main__":
    main()