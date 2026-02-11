import logging
from utils.config import bronze_path, dataset

logger = logging.getLogger(__name__)

def IngestData(spark):
    logger.info("Starting Bronze layer...")
    for item in dataset:
        logger.info(f"Ingesting {item['table']}")
        out_path = f"{bronze_path}/{item['table']}"
        try:
            # Read data from source folder
            df = spark.read.csv(item['path'], header=True)
            # Write into parquet and save to bronze folder
            df.write.format("delta").mode("overwrite").save(out_path)
        except Exception as e:
            logger.exception(f"Bronze layer error: {e}")
            raise e

        logger.info(f"Saved {item['table']} to {out_path}")

    logger.info(f"Bronze layer completed")