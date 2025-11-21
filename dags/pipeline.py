"""
Variant 3 — API (comments) → SQLite → “suspicious” flags + CSV

DAG id: v3_comments_pipeline
Tasks:
  1) fetch_comments              — load comments from API into SQLite
  2) export_suspicious_comments  — flag suspicious comments and export CSV
"""

from pathlib import Path
import sqlite3

import requests
import pandas as pd


FILE_DIR = Path(__file__).resolve().parent
DB_PATH = FILE_DIR / "v3_demo.db"
CSV_PATH = FILE_DIR / "suspicious_comments.csv"


def fetch_comments():
    """
    Task 1: Fetch comments from JSONPlaceholder API and save to SQLite.

    API: https://jsonplaceholder.typicode.com/comments
    """
    url = "https://jsonplaceholder.typicode.com/comments"
    print(f"Requesting data from {url} ...")
    response = requests.get(url)
    data = response.json()
    # Keep only needed columns "id", "postId", "name", "email", "body"
    # rename postId to post_id
    df = pd.DataFrame(data)[["id","postId","name","email","body"]]
    df = df.rename(columns ={"postId":"post_id"})
    # Save to SQLite database located in DB_PATH
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('comments',conn,if_exists="replace",index=False)
    conn.close()
    print(f"Saved {len(df)} comments to {DB_PATH} table 'comments'.")


def export_suspicious_comments():
    """
    Task 2: Flag 'suspicious' comments and export to CSV.

    Definition of suspicious:
    - body length < 20 characters OR
    - email does not contain "@"
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM comments",conn  )
    conn.close()
    print(f"Loaded {len(df)} comments from DB.")

    # filter out by body length
    # filter emails which don't contain "@"
    # Combine conditions with OR
    suspicious = df[(df["body"].str.len()<20) | (~df["email"].str.contains("@"))]
    
    # export to csv located in CSV_PATH
    suspicious.to_csv(CSV_PATH,index=False)
    print(
        f"Marked {len(suspicious)} comments as suspicious "
        f"and exported {len(suspicious)} rows to {CSV_PATH}."
    )
