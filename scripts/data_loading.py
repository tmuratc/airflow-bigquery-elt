from google.cloud import bigquery

def load_data_to_bigquery(data, table_dict):
    # The bigquery.Client() automatically uses the credentials from the environment variable
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
        
        if load_job.done(): 
            print(f"Load job is completed.")
            return True, message

        else: 
            print("Load job is not completed.")
            return False, message 

    except Exception as e: 
        print(f"An error occured: {e}")
        return False, e