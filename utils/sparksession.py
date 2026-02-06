from pyspark.sql import SparkSession

def GetSparkSession(name='ETLJob'):
    spark = (SparkSession.builder.master("local[*]")
             .appName(name)
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate()
             )
    return spark