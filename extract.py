import logging
import requests
import yaml
import json
import os
from datetime import datetime
from tqdm import tqdm
import threading


#  Load config
with open("C:/Users/vadig/OneDrive/Documents/DE_project/project_10_openlibrary/config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

subjects = config["subjects"]
base_url = config["API"]["base"]
search_endpoint = config["API"]["search_endpoint"]
page_limit = config["page_limit"]
bronze_path = config["paths"]["bronze_path"]

batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

#  Create folders
os.makedirs(f"{bronze_path}/{batch_id}/subjects", exist_ok=True)
os.makedirs(f"{bronze_path}/{batch_id}/author", exist_ok=True)
os.makedirs(f"{bronze_path}/{batch_id}/author-works", exist_ok=True)
os.makedirs(f"{bronze_path}/{batch_id}/work-details", exist_ok=True)


def extract_data():

    all_author_keys = []
    all_work_keys = []

    #  subject data
    def sub_extract():
        for subject in subjects:
            logging.info(f"\nFetching Subject: {subject}")

            for page in tqdm(range(1, page_limit + 1), leave=True):

                url = f"{base_url}{search_endpoint}?subject={subject}&page={page}"

                try:
                    response = requests.get(url,timeout=20)

                    if response.status_code == 200:
                        data = response.json()

                        for doc in data.get("docs", []):

                            if "author_key" in doc:
                                all_author_keys.extend(doc["author_key"])

                            if "key" in doc:
                                work_key = doc["key"].split("/")[-1]
                                all_work_keys.append(work_key)

                        file_path = f"{bronze_path}/{batch_id}/subjects/{subject.replace(' ', '_')}_page_{page}.json"

                        with open(file_path, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=4)

                except Exception as e:
                    logging.info(f"Error: {e}")

    sub_thread = threading.Thread(target=sub_extract)
    sub_thread.start()
    sub_thread.join()
    
    #  Remove duplicates
    
    all_author_keys = list(set(all_author_keys))
    all_work_keys = list(set(all_work_keys))

    logging.info(f"\nTotal Authors: {len(all_author_keys)}")
    logging.info(f"Total Works: {len(all_work_keys)}")

    # author details
    def aut_extract():
        for key in tqdm(all_author_keys, desc="Author Details"):
            url = f"{base_url}/authors/{key}.json"
            file_path = f"{bronze_path}/{batch_id}/author/{key}.json"

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)

            except Exception as e:
                logging.info(f"Error: {e}")

    aut_thread = threading.Thread(target=aut_extract)
    aut_thread.start()

    #  author works
    def autWork_extract():
        for key in tqdm(all_author_keys, desc="Author Works"):
            url = f"{base_url}/authors/{key}/works.json"
            file_path = f"{bronze_path}/{batch_id}/author-works/{key}_works.json"

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)

            except Exception as e:
                logging.info(f"Error: {e}")

    autWork_thread = threading.Thread(target=autWork_extract)
    autWork_thread.start()

    #  works details
    def work_extract():
        for key in tqdm(all_work_keys, desc="Work Details"):
            url = f"{base_url}/works/{key}.json"
            file_path = f"{bronze_path}/{batch_id}/work-details/{key}.json"

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)

            except Exception as e:
                logging.info(f"Error: {e}")

        logging.info("\n Bronze Layer Completed Successfully!")
    work_thread = threading.Thread(target=work_extract)
    work_thread.start()
    work_thread.join()
    autWork_thread.join()
    aut_thread.join()


if __name__ == "__main__":
    extract_data()