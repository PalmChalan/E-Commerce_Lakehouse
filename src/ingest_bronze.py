from utils.config import bronze_path, dataset


def IngestData(spark):
    for item in dataset:
        print(f"Ingesting {item['table']}")
        out_path = f"{bronze_path}/{item['table']}"
        # Read data from source folder
        df = spark.read.csv(item['path'], header=True)
        # Write into parquet and save to bronze folder
        df.write.format("delta").mode("overwrite").save(out_path)

        print(f"Saved {item['table']} to {out_path}")