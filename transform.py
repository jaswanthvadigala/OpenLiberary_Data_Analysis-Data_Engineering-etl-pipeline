import logging
import json
import os
import pandas as pd
from datetime import datetime
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="***************",
    database="openlibrary_db",
    auth_plugin='mysql_native_password'
)

cursor = conn.cursor()

def transform_data():

    # bronze path

    bronze_path = "C:/Users/vadig/OneDrive/Documents/DE_project/project_10_openlibrary/data"

    batch_id = sorted(os.listdir(bronze_path))[-2]

    subjects_path = f"{bronze_path}/{batch_id}/subjects"
    author_path = f"{bronze_path}/{batch_id}/author"


    # silver paths 

    silver_path = ("C:/Users/vadig/OneDrive/Documents/DE_project/project_10_openlibrary/sql/silver")
    os.makedirs(silver_path, exist_ok=True)


    # silver tables

    silver_ingestion_batch = []
    silver_subject = []
    silver_work = []
    silver_author = []
    silver_work_author = []
    silver_work_subject = []
    silver_work_search_snapshot = []


    # injection batch starts
    start_time = datetime.now()

    # creating 1st table
    silver_ingestion_batch.append({
        "batch_id"        : batch_id,
        "source_name"     : "open_liberary",
        "load_start_ts"   : start_time,
        "load_end_ts"     : None,
        "status"          : None,
        "records_fetched" : None,
        "records_loaded"  : None,
    })


    subject_id = 0

    # loading the subjects folders, files in it
    for subject in os.listdir(subjects_path):

        # increment subject_id once per subject file (not per doc)
        subject_id += 11111

        with open(f"{subjects_path}/{subject}", 'r', encoding="utf-8") as f:
            data = json.load(f)

            # looping for every subject
            for docs in data.get("docs", []):

                subject_name = subject.split("_page_")[0]

                # creating table-2 (silver_subject)
                silver_subject.append({
                    "subject_id"       : subject_id,
                    "subject_name"     : subject_name,
                    "subject_slug"     : subject_name.lower(),
                    "first_seen_batch_id" : batch_id
                })

                
                silver_work.append({
                    "work_key"           : docs.get("key", "no_key").split("/")[-1],
                    "title"              : docs.get("title", "not found"),
                    "first_publish_year" : docs.get("first_publish_year", None),
                    "edition_count"      : docs.get("edition_count", None),
                    "has_fulltext"       : docs.get("has_fulltext", False),
                    "language_count"     : len(docs.get("language", "NAN")),
                    "source_batch_id"    : batch_id
                })

                # creating table-6 (silver_work_subject)
                silver_work_subject.append({
                    "work_key"       : docs.get("key", "no_key").split("/")[-1],
                    "subject_name"   : subject_name,
                    "source_batch_id": batch_id
                })

                # creating table-7 (silver_work_search_snapshot)
                raw_authors = docs.get("author_name")
                raw_author_str = ", ".join(raw_authors) if isinstance(raw_authors, list) else (raw_authors or None)

                silver_work_search_snapshot.append({
                    "batch_id"        : batch_id,
                    "subject_name"    : subject_name,
                    "work_key"        : docs.get("key", "no_key").split("/")[-1],
                    "search_rank"     : None,
                    "api_page"        : "1",
                    "raw_title"       : docs.get("title", "info_lost"),
                    "raw_author_text" : raw_author_str
                })

                # creating table-5 (silver_work_author)
                work_key = docs.get("key", "").split("/")[-1]
                for aut_key in docs.get("author_key", []):
                    silver_work_author.append({
                        "work_key": work_key,
                        "author_key": aut_key,
                        "source_batch_id": batch_id
                    })


    # creating tables related to authors
    for author in os.listdir(author_path):
        with open(f"{author_path}/{author}", 'r', encoding="utf-8") as f:
            aut_data = json.load(f)

            # creating table-4 (silver_author)
            silver_author.append({
                "author_key"      : aut_data.get("key", "not found").split("/")[-1],
                "author_name"     : aut_data.get("name", "not found"),
                "birth_date"      : aut_data.get("birth_date", "not found"),   
                "top_work"        : aut_data.get("top_work", "not found"),
                "work_count"      : aut_data.get("revision", "not found"),
                "source_batch_id" : batch_id
            })

        


    # converting the tables into dataframes
    df_silver_ingestion_batch    = pd.DataFrame(silver_ingestion_batch)
    df_silver_subject            = pd.DataFrame(silver_subject)
    df_silver_work               = pd.DataFrame(silver_work)
    df_silver_author             = pd.DataFrame(silver_author)
    df_silver_work_author        = pd.DataFrame(silver_work_author)
    df_silver_work_subject       = pd.DataFrame(silver_work_subject)
    df_silver_work_search_snapshot = pd.DataFrame(silver_work_search_snapshot)


    # removing duplicates
    df_silver_ingestion_batch    = df_silver_ingestion_batch.drop_duplicates()
    df_silver_subject            = df_silver_subject.drop_duplicates()
    df_silver_work               = df_silver_work.drop_duplicates()
    df_silver_author             = df_silver_author.drop_duplicates()
    df_silver_work_author        = df_silver_work_author.drop_duplicates()
    df_silver_work_subject       = df_silver_work_subject.drop_duplicates()
    df_silver_work_search_snapshot = df_silver_work_search_snapshot.drop_duplicates()


    # updating the values in table-1 (silver_ingestion_batch)
    load_end_ts     = datetime.now()
    status          = "SUCCESS"
    records_fetched = len(silver_work_subject)
    records_loaded  = len(silver_work_subject)

    df_silver_ingestion_batch["load_end_ts"]     = load_end_ts
    df_silver_ingestion_batch["status"]          = status
    df_silver_ingestion_batch["records_fetched"] = records_fetched
    df_silver_ingestion_batch["records_loaded"]  = records_loaded

    # convert datetime → string
    df_silver_ingestion_batch["load_start_ts"] = df_silver_ingestion_batch["load_start_ts"].astype(str)
    df_silver_ingestion_batch["load_end_ts"]   = df_silver_ingestion_batch["load_end_ts"].astype(str)


    def clean_df(df):
        return df.where(pd.notnull(df), None)

    df_silver_ingestion_batch      = clean_df(df_silver_ingestion_batch)
    df_silver_subject              = clean_df(df_silver_subject)
    df_silver_work                 = clean_df(df_silver_work)
    df_silver_author               = clean_df(df_silver_author)
    df_silver_work_author          = clean_df(df_silver_work_author)
    df_silver_work_subject         = clean_df(df_silver_work_subject)
    df_silver_work_search_snapshot = clean_df(df_silver_work_search_snapshot)


    # logging.infoing final output
    logging.info("\n Table1  : silver_ingestion_batch")
    logging.info(df_silver_ingestion_batch.head())

    logging.info("\n Table2  : silver_subject")
    logging.info(df_silver_subject.head())

    logging.info("\n Table3  : silver_work")
    logging.info(df_silver_work.head())

    logging.info("\n Table4  : silver_author")
    logging.info(df_silver_author.head())

    logging.info("\n Table5 df_silver_work_author")
    logging.info(df_silver_work_author.head())

    logging.info("\n Table6 df_silver_work_subject")
    logging.info(df_silver_work_subject.head())

    logging.info("\n Table7 df_silver_work_search_snapshot")
    logging.info(df_silver_work_search_snapshot.head())

    logging.info("silver layer runned successfully")


    # inserting the dataFrames data into MySql

    # 1
    sql_silver_ingestion_batch = """
    INSERT IGNORE INTO silver_ingestion_batch(
        batch_id, source_name, load_start_ts, load_end_ts, status, records_fetched, records_loaded
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # 2
    sql_silver_subject = """
    INSERT IGNORE INTO silver_subject(
        subject_id,
        subject_name,
        subject_slug,
        first_seen_batch_id
    )
    VALUES (%s, %s, %s, %s)
    """

    # 3
    sql_silver_work = """
    INSERT IGNORE INTO silver_work(
        work_key,
        title,
        first_publish_year,
        edition_count,
        has_fulltext,
        language_count,
        source_batch_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # 4
    sql_silver_author = """
    INSERT IGNORE INTO silver_author(
        author_key,
        author_name,
        birth_date,
        top_work,
        work_count,
        source_batch_id
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # 5
    sql_silver_work_author = """
    INSERT IGNORE INTO silver_work_author(
        work_key,
        author_key,
        source_batch_id
    )
    VALUES (%s, %s, %s)
    """

    # 6
    sql_silver_work_subject = """
    INSERT IGNORE INTO silver_work_subject(
        work_key,
        subject_name,
        source_batch_id
    )
    VALUES (%s, %s, %s)
    """

    # 7
    sql_silver_work_search_snapshot = """
    INSERT IGNORE INTO silver_work_search_snapshot(
        batch_id,
        subject_name,
        work_key,
        search_rank,
        api_page,
        raw_title,
        raw_author_text
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """



    # Execution Order


    # parent table first
    cursor.executemany(sql_silver_ingestion_batch, df_silver_ingestion_batch.values.tolist())

    # then dependent tables
    cursor.executemany(sql_silver_subject, df_silver_subject.values.tolist())
    cursor.executemany(sql_silver_work, df_silver_work.values.tolist())
    cursor.executemany(sql_silver_author, df_silver_author.values.tolist())

    cursor.executemany(
        sql_silver_work_author,
        df_silver_work_author[["work_key", "author_key", "source_batch_id"]].values.tolist()
    )

    cursor.executemany(sql_silver_work_subject, df_silver_work_subject.values.tolist())

    cursor.executemany(
        sql_silver_work_search_snapshot,
        df_silver_work_search_snapshot.values.tolist()
    )

    conn.commit()


    # checking data
    cursor.execute("SELECT * FROM silver_ingestion_batch")
    logging.info(cursor.fetchall())

    cursor.execute("SELECT * FROM silver_work_author " \
    "limit 10")

    logging.info(cursor.fetchall())

    logging.info("data inserted successfully")

    return (
    df_silver_work,
    df_silver_author,
    df_silver_work_author,
    df_silver_work_subject
)

if __name__ == "__main__":
    transform_data()