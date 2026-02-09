from utils.sparksession import GetSparkSession
from src.download_data import DownloadDataset
from src.ingest_bronze import IngestData
from src.clean_silver import CleanData
from src.model_gold import CreateModel

def main():
    spark = GetSparkSession("Medallion Architecture")
    DownloadDataset()
    IngestData(spark)
    CleanData(spark)
    CreateModel(spark)

if __name__ == "__main__":
    main()