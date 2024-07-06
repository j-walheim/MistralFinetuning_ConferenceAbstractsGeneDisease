import os

def ensure_dir(file_path):
    """Ensure that a directory exists."""
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def log_progress(ti, message):
    """Log progress message using Airflow's TaskInstance."""
    ti.log.info(message)

# Add more utility functions as needed