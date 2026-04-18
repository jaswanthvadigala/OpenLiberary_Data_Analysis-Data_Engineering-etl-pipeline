import pandas as pd
from datetime import datetime

def run_quality_checks(
    df_silver_work,
    df_silver_author,
    df_silver_work_author,
    df_silver_work_subject
):

    print("\nRunning Data Quality Checks...\n")

    # 1. work_key must not be null
    if df_silver_work["work_key"].isnull().any():
        raise Exception("ERROR: Null work_key found in silver_work")
    else:
        print("PASS: work_key is valid")

    # 2. author_key must not be null
    if df_silver_author["author_key"].isnull().any():
        raise Exception("ERROR: Null author_key found in silver_author")
    else:
        print("PASS: author_key is valid")

    # 3. No duplicate work-author pairs
    if df_silver_work_author.duplicated(["work_key", "author_key"]).any():
        raise Exception("ERROR: Duplicate rows in silver_work_author")
    else:
        print("PASS: No duplicates in work_author")

    # 4. first_publish_year range check
    current_year = datetime.now().year

    invalid_years = df_silver_work[
        (df_silver_work["first_publish_year"].notnull()) &
        (
            (df_silver_work["first_publish_year"] < 1000) |
            (df_silver_work["first_publish_year"] > current_year)
        )
    ]

    if len(invalid_years) > 0:
        print("WARNING: Invalid first_publish_year detected")
    else:
        print("PASS: first_publish_year valid")

    # 5. work_key in work_subject must exist in work
    if not set(df_silver_work_subject["work_key"]).issubset(set(df_silver_work["work_key"])):
        raise Exception("ERROR: Invalid work_key in silver_work_subject")
    else:
        print("PASS: work_subject FK valid")

    # 6. author_key in work_author must exist in author
    if not set(df_silver_work_author["author_key"]).issubset(set(df_silver_author["author_key"])):
        raise Exception("ERROR: Invalid author_key in silver_work_author")
    else:
        print("PASS: work_author FK valid")

    print("\nAll Quality Checks Passed Successfully\n")