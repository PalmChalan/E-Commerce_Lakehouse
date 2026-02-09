import kagglehub
from utils.config import base_path

kagglehub.dataset_download("olistbr/brazilian-ecommerce", output_dir=f"{base_path}", force_download=True)