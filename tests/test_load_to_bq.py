
import sys
import os
# Adding the root directory of the project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()
from tasks.data_loading import load_data_to_bigquery


def test_load_data_to_bq(): 
    test_cases = [ 
        {
            "description": "Loading json formatted data to BigQuery table", 
            "input": 
                { 
                    "data": [
                        {"name": "Ali", "age": 25, "create_date": "2024-08-30"}, 
                        {"name": "Ayse", "age": 26, "create_date": "2024-08-20"}

                    ],                       
                    "table_dict": {
                        "project_id": os.getenv("GCP_PROJECT_ID"),
                        "dataset_id": os.getenv("GCP_DATASET_ID"),
                        "table_id": os.getenv("GCP_TABLE_ID_TEST")
                    }
                },  
            "expected_output": True
        }
    ]  

    for test in test_cases: 
        is_job_completed, message = load_data_to_bigquery(**test["input"]) 
        if is_job_completed == test["expected_output"]:
            print(f"{test['description']}: PASSED, Info: {message}")
        else:
            print(f"{test['description']}: FAILED, Info: {message}")


if __name__ == "__main__": 
    test_load_data_to_bq()