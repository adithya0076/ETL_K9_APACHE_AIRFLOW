# ETL_K9_APACHE_AIRFLOW

ETL pipeline for K9 care using apache airflow and python

## Installation

```bash
docker-compose build
docker-compose up
```

## Usage
### Step 1
1. Log into pg-admin
username = admin@admin.com & password = root
2. Register a server with ip address of the docker container of postgres
3. Create a database called k9_db 
with username=airflow & password=airflow & port = 5432

### Step 2
1. Log into airflow
username = airflow & password = airflow

2. Go to Admin > Connections
3. Create new connection and with host of the posgtres and db_name = k9_db & username=airflow & password=airflow & port = 5432
4. After saving go to the dags page and execute the dag
