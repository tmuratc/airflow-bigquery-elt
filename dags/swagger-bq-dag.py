from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, date
import requests 
from google.cloud import bigquery
import os 


default_args = {
    'owner': 'your_name',
    'start_date': datetime(2024, 9, 4),
    'retries': 1,
}


def fetch_from_api(api_url, params=None, **kwargs):
    """
    Fetch data from an API endpoint.
    Args:
        api_url (str): The URL of the API endpoint.
        params (dict, optional): A dictionary of parameters to be sent with the request.
        
    Outputs:
        kwargs["success"]: True or False 
        kwargs["data"]: response.json() or None
    """
    success = False
    data = None
    try:
        response = requests.get(api_url, params=params, timeout=10)  # Added timeout for better reliability
        data = response.json()
        success = True
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} - Status Code: {response.status_code}"
        print(error_message)

    except requests.exceptions.RequestException as req_err:
        error_message = f"Request exception occurred: {req_err}"
        print(error_message)

    except Exception as e:
        error_message = f"Unexpected error occured while extracting data: {e}"
        print(error_message)
    
    task_id = kwargs['task_instance'].task_id
    kwargs['ti'].xcom_push(key=f"{task_id}_data", value={"success":success, "data": data})



def load_data_to_bigquery(**kwargs):
    upstream_task_id = kwargs["upstream_extract_task_id"]
    upstream_task_xcom_key = f"{upstream_task_id}_data" 
    value = kwargs["ti"].xcom_pull(key=upstream_task_xcom_key, task_ids = upstream_task_id)
    success, data = value["success"], value["data"]
    table_dict = kwargs["destination_table"] 
   
    load_success = False 
    if not success:  
        return 
    try : 
        client = bigquery.Client()

        # Constructing a TableReference object
        table_ref = bigquery.TableReference.from_string(
            f"{table_dict["project_id"]}.{table_dict["dataset_id"]}.{table_dict["table_id"]}") 

        # Constructing a LoadJobConfig object and set attributes
        job_config = bigquery.LoadJobConfig()
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.write_disposition = "WRITE_APPEND" 
        job_config.source_format = "NEWLINE_DELIMITED_JSON"
        job_config.autodetect = True 


        load_job = client.load_table_from_json(json_rows = data, destination=table_ref, job_config=job_config)
        load_job.result()  # Waits for the job to complete.
        loaded_rows = load_job.output_rows
        loaded_table = load_job.destination
        message = f"{loaded_rows} rows were added to {loaded_table}"
        print(message)

        if load_job.done(): 
            print(f"Load job is completed.")
            load_success = True

        else: 
            print("Load job is not completed.")

    except Exception as e: 
        print(f"An error occured: {e}")

    task_id = kwargs['task_instance'].task_id
    kwargs['ti'].xcom_push(key=f"{task_id}_status", value={"success": load_success})


with DAG('swagger_bq_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

   
    extract_installs = PythonOperator(
        task_id = "extract_installs",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": os.getenv("URL_1")}, 
        provide_context = True
    )
    load_installs = PythonOperator(
        task_id = "load_installs", 
        python_callable = load_data_to_bigquery, 
        op_kwargs = {"upstream_extract_task_id": "extract_installs", 
                    "destination_table": {"project_id": os.getenv("PROJECT_ID"), 
                                        "dataset_id": os.getenv("DATASET_ID"),
                                        "table_id": os.getenv("STAGING_TABLE_1")
                                        }
                    },
        provide_context = True
    )
    
    extract_events = PythonOperator(
        task_id = "extract_events",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": os.getenv("URL_2")}, 
        provide_context = True
    )
    load_events = PythonOperator(
        task_id = "load_events", 
        python_callable = load_data_to_bigquery, 
        provide_context = True,
        op_kwargs = {"upstream_extract_task_id": "extract_events", 
                    "destination_table": {
                        "project_id": os.getenv("PROJECT_ID"), 
                        "dataset_id": os.getenv("DATASET_ID"),
                        "table_id": os.getenv("STAGING_TABLE_2")
                                            }
                    }
    )
    extract_network_costs = PythonOperator(
        task_id = "extract_network_costs",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": os.getenv("URL_3"), "params":{"cost_date": date.today()} }, 
        provide_context = True
    )
    load_network_costs = PythonOperator(
        task_id = "load_network_costs", 
        python_callable = load_data_to_bigquery, 
        provide_context = True,
        op_kwargs = {"upstream_extract_task_id": "extract_network_costs", 
                    "destination_table": {
                        "project_id": os.getenv("PROJECT_ID"), 
                        "dataset_id": os.getenv("DATASET_ID"),
                        "table_id": os.getenv("STAGING_TABLE_3")
                                            }
                    }
    )

    extract_installs>>load_installs
    extract_events>>load_events
    extract_network_costs>>load_network_costs