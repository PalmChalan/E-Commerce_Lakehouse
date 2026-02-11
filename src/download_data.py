import kagglehub
import logging
from utils.config import base_path

logger = logging.getLogger(__name__)

def DownloadDataset():
    logger.info("Downloading CSV Dataset...")
    try:
        kagglehub.dataset_download("olistbr/brazilian-ecommerce", output_dir=f"{base_path}", force_download=True)
    except Exception as e:
        logger.exception(f"Downloading CSV error: {e}")
        raise e
    logger.info(f"Dataset successfully saved to {base_path}")