import logging
import sys
import os

def SetLogger(log_file_path="pipeline.log"):
    """
    Configures the global logging settings.
    Saves logs to a file AND prints them to the console.
    """
    
    # Create the directory for logs if it doesn't exist
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define the handlers
    file_handler = logging.FileHandler(log_file_path, mode='w')
    console_handler = logging.StreamHandler(sys.stdout)

    # Apply configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', # Nice readable date format
        handlers=[file_handler, console_handler],
        force=True # ⚠️ Important: Overrides any default settings 
    )