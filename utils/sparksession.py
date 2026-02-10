from pyspark.sql import SparkSession

def GetSparkSession(name='ETLJob'):
    delta_version = "3.0.0"
    spark = (SparkSession.builder.master("local[*]")
             .appName(name)
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{delta_version}")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate()
             )
    return spark