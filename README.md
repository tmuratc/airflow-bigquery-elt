## SWAGGER API-BIGQUERY DATA PIPELINE ORCHESTRATION
- A daily pipeline that extracts data from 3 endpoints and save final results to BigQuery table.
- DAG diagram is specified in jpeg file.
- Project aims to reusability on staging process for any given 3 endpoints.


## Prerequisites
A GCP Project with followings; 
- A BigQuery Dataset
- A service account with BigQuery Admin Role
- Key file for service account


## Installation with Docker-Compose 
It is used pre-built docker image for Airflow 2.9.1 version. 
To check apache/airflow:2.9.1 in Docker Hub [click here.](https://hub.docker.com/layers/apache/airflow/2.9.1/images/sha256-4b494609394706cc866431cfed10701c38c383c97e435cb2462a77acc70bb150)

To clone this repository, use the following command:

```bash
git clone https://github.com/tmuratcamli/Airflow-Api-BigQuery.git
````

Update first path oh this line in docker-compose file by replacing with path you store GCP key. 

```bash
absolute/path/to/your/gcp/key:/opt/airflow/gcp_key.json
````

Change .envexample extension as .env and update variables according to yours. Followings are essential to run smoothly; 
```bash
PROJECT_ID = "your_gcp_project_id"
DATASET_ID = "your_dataset_id"
````

To initialize Airflow inside docker container, run following command:

```bash
docker-compose -f docker-compose-dev.yaml up airflow-init

````
To start Airflow components services such as webserver, scheduler, worker, triggerer, run following command: 

```bash
docker-compose -f docker-compose-dev.yaml up -d
```

To see Airflow UI on your [localhost.](http://localhost:8080) Sign in with password and user in .env file;

 ```bash
_AIRFLOW_WWW_USER_USERNAME='your_web_ui_username'
_AIRFLOW_WWW_USER_PASSWORD='your_web_ui_password'
```
