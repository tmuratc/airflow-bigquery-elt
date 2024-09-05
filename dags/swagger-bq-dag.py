from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import requests 
from datetime import datetime, date, timedelta
import os 

default_args = {
    'owner': os.getenv("OWNER"),
    'start_date': datetime(2024, 9, 5),
    'retries': 1
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
        job_config.write_disposition = "WRITE_TRUNCATE" 
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

def run_bq_query(**kwargs): 
    success = False
    try : 
        query = kwargs.get("query")
        client = bigquery.Client() 
        query_job = client.query(query)
        query_job.result() 
        print("Query job is finished.")
        success = True

    except Exception as e: 
        print(f"Query job is failed. Info: {e}") 

    task_id = kwargs['task_instance'].task_id
    kwargs['ti'].xcom_push(key=f"{task_id}_status", value={"success": success}) 
##########################################################################
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
FINAL_TABLE = os.getenv("FINAL_TABLE")

URL_1 = os.getenv("URL_1")
URL_2 = os.getenv("URL_2")
URL_3 = os.getenv("URL_3") 

STAGING_TABLE_1 = os.getenv("STAGING_TABLE_1")
STAGING_TABLE_2 = os.getenv("STAGING_TABLE_2")
STAGING_TABLE_3 = os.getenv("STAGING_TABLE_3")
##########################################################################
with DAG('swagger_bq_pipeline',
         default_args=default_args,
         schedule_interval='0 12 * * *', 
         catchup=False) as dag:

    extract_installs = PythonOperator(
        task_id = "extract_installs",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": URL_1}, 
        provide_context = True
    )
    load_installs = PythonOperator(
        task_id = "load_installs", 
        python_callable = load_data_to_bigquery, 
        op_kwargs = {"upstream_extract_task_id": "extract_installs", 
                    "destination_table": {"project_id": PROJECT_ID, 
                                          "dataset_id": DATASET_ID,
                                          "table_id": STAGING_TABLE_1
                                        }
                    },
        provide_context = True
    )
    
    extract_events = PythonOperator(
        task_id = "extract_events",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": URL_2}, 
        provide_context = True
    )
    load_events = PythonOperator(
        task_id = "load_events", 
        python_callable = load_data_to_bigquery, 
        provide_context = True,
        op_kwargs = {"upstream_extract_task_id": "extract_events", 
                    "destination_table": {
                        "project_id": PROJECT_ID, 
                        "dataset_id": DATASET_ID,
                        "table_id": STAGING_TABLE_2
                                            }
                    }
    )
    extract_network_costs = PythonOperator(
        task_id = "extract_network_costs",
        python_callable = fetch_from_api, 
        op_kwargs = {"api_url": URL_3, "params":{"cost_date": date.today()-timedelta(days=1)} }, 
        provide_context = True
    )
    load_network_costs = PythonOperator(
        task_id = "load_network_costs", 
        python_callable = load_data_to_bigquery, 
        provide_context = True,
        op_kwargs = {"upstream_extract_task_id": "extract_network_costs", 
                    "destination_table": {
                        "project_id": PROJECT_ID, 
                        "dataset_id": DATASET_ID,
                        "table_id": STAGING_TABLE_3
                                        }
                    }
    )
    
    query = """
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE}` AS

with modified_events as (
select user_id, date(event_ts) as event_day, event_name,
       case when event_name = "GameStart" THEN 1 else 0 
       end game_start_count,

       case when ad_type = "interstitial" THEN ad_revenue else 0 
       end interstitial_revenue, 

       case when ad_type = "banner" THEN ad_revenue else 0 
       end banner_revenue, 

       case when ad_type = "rewarded" THEN ad_revenue else 0 
       end rewarded_revenue,  
from `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_2}` 
), 
user_facts as ( 
  select me.*, date(ins.installed_at) as install_day, ins.network_name, ins.campaign_name, 
  from modified_events me
  left join `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_1}` ins on ins.user_id = me.user_id 
),
day_summary as (
select install_day, network_name, campaign_name, 
      cast(count(distinct user_id) as integer) as install_count, 
      cast(sum(game_start_count) as integer) as game_start_count, 
      cast(sum(interstitial_revenue) as integer) as interstitial_revenue, 
      cast(sum(banner_revenue) as integer) as banner_revenue, 
      cast(sum(rewarded_revenue) as integer) as rewarded_revenue,
from user_facts
group by 1,2,3 
),
costs as ( 
  select date(nc.date) as cost_day, nc.network_name, nc.campaign_name, 
  cast(sum(cost) as integer) as cost_sum
  from `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_3}` nc
  group by 1,2,3
), 
final as (
  select ds.*, c.cost_sum as cost
  from day_summary ds
  left join costs c on c.cost_day = ds.install_day and 
                      c.network_name = ds.network_name and 
                      c.campaign_name = ds.campaign_name
) 
select * from final 
""".format(PROJECT_ID=PROJECT_ID, 
           DATASET_ID=DATASET_ID, 
           FINAL_TABLE=FINAL_TABLE, 
           STAGING_TABLE_1 = STAGING_TABLE_1, 
           STAGING_TABLE_2 = STAGING_TABLE_2, 
           STAGING_TABLE_3 = STAGING_TABLE_3)
    
    bq_transform_data = PythonOperator(
        task_id = "bq_transform_data", 
        python_callable = run_bq_query,
        op_kwargs = {"query": query}, 
        provide_context = True
    )

    extract_installs>>load_installs
    extract_events>>load_events
    extract_network_costs>>load_network_costs 
    [load_installs, load_events, load_network_costs]>>bq_transform_data
