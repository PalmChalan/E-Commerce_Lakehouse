import logging
from pyspark.sql.functions import col, lower, to_timestamp, translate, trim
from pyspark.sql.types import IntegerType
from utils.config import bronze_path, silver_path, dataset

logger = logging.getLogger(__name__)

def standardSave(df, table, primary_key=None):
    '''
    Helper function to save dataset into silver layer. This code does the following:
    1. Drop duplicate (based on primary key)
    2. Drop null (based on primary key)
    3. Saves parquet file into silver folder
    '''
    original = df.count()
    logger.info(f"Saving {table}...")

    if primary_key:
        df = df.dropDuplicates(primary_key)
        df = df.dropna(subset=primary_key)
    else:
        df = df.dropDuplicates()
        df = df.dropna()
    final = df.count()

    out_path = f"{silver_path}/{table}"
    df.write.format("delta").mode("overwrite").save(out_path)
    logger.info(f"Saved cleaned {table} to {out_path}. Total dropped rows: {original - final}")

def getPrimaryKey(table):
    '''Helper function to get Primary Key of the table'''
    for item in dataset:
        if item['table'] == table:
            return item['primary_key']

def removeAccent(df, column):
    '''Helper function to remove Spanish Accent from the column'''
    accents = "áéíóúÁÉÍÓÚãõÃÕâêîôûÂÊÎÔÛçÇ"
    no_accents = "aeiouAEIOUaoAOaeiouAEIOUcC"
    df = df.withColumn(column, translate(col(column), accents, no_accents))
    df = df.withColumn(column, lower(trim(col(column))))
    return df
        
def clean_orders(spark):
    """
    Specific function for orders data. This code does the following:
    1. Fix timestamp string
    2. Remove null and duplicates
    3. Save to silver folder
    """
    
    # Read from Bronze
    input_path = f"{bronze_path}/orders"
    df = spark.read.format("delta").load(input_path)

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
    standardSave(df, "orders", getPrimaryKey("orders"))

def clean_products(spark):
    """
    Specific function for products data. This code does the following:
    1. Join products with translation to get English product category name
    2. Remove Portuguese column
    3. Remove null and duplicates
    4. Save to silver folder
    """
    
    # Read Products and Translations dataset
    products_df = spark.read.format("delta").load(f"{bronze_path}/products")
    translate_df = spark.read.format("delta").load(f"{bronze_path}/category_name_translate")

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
    standardSave(final_df, "products", getPrimaryKey("products"))

def clean_order_items(spark):
    '''
    Specific function for order_items data. This code does the following:
    1. Cast number column into decimal
    2. Cast timestamp column into timestamp
    3. Remove columns with negative prices (not possible)
    4. Remove null and duplicates
    5. Save to silver folder
    '''
    input_path = f"{bronze_path}/order_items"
    df = spark.read.format("delta").load(input_path)

    number_cols = ["price", "freight_value"]
    timestamp_col = "shipping_limit_date"

    for column in number_cols:
        df = df.withColumn(column, col(column).cast("double"))
    
    df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col)))

    df = df.filter(col("price") >= 0)

    standardSave(df, "order_items", getPrimaryKey("order_items"))

def clean_order_payments(spark):
    '''
    Specific function for order_payments data. This code does the following:
    1. Cast number column into decimal and integer based on the data
    2. Remove columns with negative payment value (not possible)
    3. Remove null and duplicates
    4. Save to silver folder
    '''
    input_path = f"{bronze_path}/order_payments"
    df = spark.read.format("delta").load(input_path)

    decimal_col = "payment_value"
    int_cols = ["payment_sequential", "payment_installments"]

    df = df.withColumn(decimal_col, col(decimal_col).cast("double"))
    for column in int_cols:
        df = df.withColumn(column, col(column).cast(IntegerType()))

    df = df.filter(col("payment_value") >= 0)

    standardSave(df, "order_payments", getPrimaryKey("order_payments"))

def clean_order_reviews(spark):
    '''
    Specific function for order_payments data. This code does the following:
    1. Cast number column into integer
    2. Cast timestamp column into timestamp
    3. Remove column with wrong review id format
    4. Remove null and duplicates
    5. Save to silver folder
    '''
    input_path = f"{bronze_path}/order_reviews"
    df = spark.read.format("delta").load(input_path)

    number_col = "review_score"
    timestamp_cols = ["review_creation_date", "review_answer_timestamp"]

    df = df.withColumn(number_col, col(number_col).cast(IntegerType()))
    for column in timestamp_cols:
        df = df.withColumn(column, to_timestamp(col(column)))

    df = df.filter(col("review_id").rlike("^[a-f0-9]{32}$"))

    standardSave(df, "order_reviews", getPrimaryKey("order_reviews"))

def clean_location(spark):
    '''
    Function to clean dataset containing location column. This code does the following:
    1. Get the table and column name of the data containing city
    2. Remove accent from the data (E.g. changing from ã to a)
    3. Remove null and duplicates
    4. Save to silver folder
    '''
    tables = [
        ("customers", "customer_city"),
        ("geolocation", "geolocation_city"),
        ("sellers", "seller_city")
    ]

    for table, column in tables:
        input_path = f"{bronze_path}/{table}"
        df = spark.read.format("delta").load(input_path)
        df = removeAccent(df, column)

        standardSave(df, table, getPrimaryKey(table))

def clean_translation(spark):
    '''
    Specific function for translation data. This code does the following:
    1. Remove null and duplicates
    2. Save to silver folder
    '''
    input_path = f"{bronze_path}/category_name_translate"
    df = spark.read.format("delta").load(input_path)
    standardSave(df, "category_name_translate", getPrimaryKey("category_name_translate"))

def CleanData(spark): 
    logger.info("Starting Silver layer...")
    try:
        clean_orders(spark)
        clean_products(spark)
        clean_order_items(spark)
        clean_order_payments(spark)
        clean_order_reviews(spark)
        clean_location(spark)
        clean_translation(spark)
    except Exception as e:
        logger.exception(f"Silver layer error: {e}")
        raise e
    logger.info("Silver layer compeleted")