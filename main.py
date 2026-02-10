import logging
from utils.logger import SetLogger
from utils.sparksession import GetSparkSession
from src.download_data import DownloadDataset
from src.ingest_bronze import IngestData
from src.clean_silver import CleanData
from src.model_gold import CreateModel

SetLogger("pipeline.log")
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Data Pipeline")
    spark = GetSparkSession("Medallion Architecture")
    DownloadDataset()
    IngestData(spark)
    CleanData(spark)
    CreateModel(spark)
    logger.info("Data pipeline successfully executed. Closing spark session")
    spark.stop()

if __name__ == "__main__":
    main()