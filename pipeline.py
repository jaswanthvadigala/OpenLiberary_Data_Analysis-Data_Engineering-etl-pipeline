import os
import logging
from datetime import datetime

# importing layer files
from extract.extract import extract_data
from transform.transform import transform_data
from quality.quality import run_quality_checks   

# creating log folder
logging_path = "C:/Users/vadig/OneDrive/Documents/DE_project/project_10_openlibrary/data/logs"
os.makedirs(logging_path, exist_ok=True)

# batch id for unique log file
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# logging configuration
logging.basicConfig(
    filename=f"{logging_path}/pipeline_{batch_id}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)



logging.info("PIPELINE STARTED")


logging.info("BRONZE LAYER STARTED")

try:
    extract_data()
    logging.info("EXTRACTION COMPLETED SUCCESSFULLY")

except Exception as e:
    logging.error("EXTRACT FAILED")
    logging.error(str(e))
    logging.error("PIPELINE STOPPED AT EXTRACTION PHASE") 
    exit()


logging.info("SILVER LAYER STARTED")

try:

    df_silver_work, df_silver_author, df_silver_work_author, df_silver_work_subject = transform_data()

    logging.info("TRANSFORM COMPLETED SUCCESSFULLY")

except Exception as e:
    logging.error("TRANSFORM FAILED")
    logging.error(str(e))
    logging.error("PIPELINE STOPPED AT TRANSFORM PHASE")
    exit()


logging.info("DATA QUALITY CHECKS STARTED")

try:
    run_quality_checks(
        df_silver_work,
        df_silver_author,
        df_silver_work_author,
        df_silver_work_subject
    )

    logging.info("DATA QUALITY CHECKS COMPLETED SUCCESSFULLY")

except Exception as e:
    logging.error("DATA QUALITY CHECKS FAILED")
    logging.error(str(e))
    logging.error("PIPELINE STOPPED AT QUALITY CHECK PHASE")
    exit()

logging.info("PIPELINE COMPLETED SUCCESSFULLY")