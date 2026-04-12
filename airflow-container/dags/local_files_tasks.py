import json
import os
from pathlib import Path
from airflow import DAG, Dataset
from airflow.sdk import task
from airflow.sdk import get_current_context


@task
def list_local_files(path):
    # List all files in the specified directory
    files = os.listdir(path)
    print(f"Files in {path}: {files}")
    return files


@task
def filter_files_to_remove(local_files, blob_files, table_name):
    files_to_remove = []
    for f in blob_files:
        if f.replace(f"{table_name}/",'') not in local_files:
            files_to_remove.append(f)
    return files_to_remove


@task
def filter_new_files(local_files, blob_files, table_name):
    new_files = []
    for f in local_files:
        if f"{table_name}/{f}" not in blob_files:
            new_files.append(f)
    return new_files