from utils.sparksession import GetSparkSession
from utils.config import bronze_path, dataset

spark = GetSparkSession("Ingest")

for item in dataset:
    print(f"Ingesting {item['table']}")
    print(item['path'])
    out_path = f"{bronze_path}/{item['table']}"
    print(out_path)
    df = spark.read.csv(item['path'], inferSchema=True, header=True)
    df.write.mode("overwrite").parquet(out_path)

    print(f"Saved {item['table']} to {out_path}")