import datetime
import pandas as pd
import pendulum 

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# --------------------------------------------------------------------------------
# Scheduling
# --------------------------------------------------------------------------------
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")

DAG_NAME = "productfeed_import_transform_gcs" # DAG name (proposed format: lowercase underscore). Should be unique.
DAG_DESCRIPTION = "Import the Kwantum productfeed to Big Query"
DAG_START_DATE = datetime.datetime(2023, 4, 1, tzinfo=LOCAL_TZ) # Startdate. When setting the "catchup" parameter to True, you can perform a backfill when you insert a specific date here like datetime(2021, 6, 20)
DAG_SCHEDULE_INTERVAL = "0 7 * * *" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True # Defaults to False. When set to True, uploading a DAG for the first time, the DAG doesn't start directly 
DAG_MAX_ACTIVE_RUNS = 5 # Configure efficiency: Max. number of active runs for this DAG. Scheduler will not create new active DAG runs once this limit is hit. 


# --------------------------------------------------------------------------------
# Default DAG arguments
# --------------------------------------------------------------------------------

default_dag_args = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=30)
}

# --------------------------------------------------------------------------------
# Workflow configuration
# --------------------------------------------------------------------------------

# Import

GCP_GCS_CONNECTION_ID = 'xxxx'
GCP_GCS_BUCKET = 'xxxx'
GCP_GCS_BUCKET_SOURCE = 'xxxx
GCP_BQ_CONNECTION_ID = 'xxxx'
GCP_BQ_PROJECT = 'xxxx'

GCP_GCS_BUCKET_CDN = 'xxxx'

PRODUCT_FEEDS = [{
		'name': 'productfeed_source',
		'source_gcs': 'file.csv',
		'destination_gcs_cdn': 'file.csv',
		'destination_bq': 'project.dataset.table',
		'destination_bq_master_view': 'project.dataset.table',
		'destination_bq_master': 'project.dataset.table'
	},{
		'name': 'productfeed_source',
		'source_gcs': 'file.csv',
		'destination_gcs_cdn': 'file.csv',
		'destination_bq': 'project.dataset.table',
		'destination_bq_master_view': 'project.dataset.table',
		'destination_bq_master': 'project.dataset.table'
	}
]

# BQ Schema
GCP_BQ_SCHEMA = [
  {
    "mode": "NULLABLE",
    "name": "productlink",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "productid",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "mainproductid",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "title",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "description",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "main_category",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "sub_category",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "sub_sub_category",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "selling_price",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "old_price",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "color",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "material",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "product_height",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "product_length",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "product_width",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "product_diameter",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "v_groef",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "verduisterend",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "unit_pricing_base_measure",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "unit_pricing_measure",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "product_in_stock",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ProductImageURL",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ProductImageURL_L",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "additional_images",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "delivery_period",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "PackageHeight",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "PackageLength",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "PackagWidth",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "Weight",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "topCategoryId",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "categoryId",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "subCategoryId",
    "type": "STRING"
  },
]



# --------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------


def gcs_to_bigquery(i, **kwargs):
    return GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_' + PRODUCT_FEEDS[i]['name'],
        bucket=GCP_GCS_BUCKET_SOURCE,
        source_objects=[
            PRODUCT_FEEDS[i]['source_gcs']
        ],
        destination_project_dataset_table=PRODUCT_FEEDS[i]['destination_bq'],
        source_format='CSV',
        # autodetect=True,
        schema_fields=GCP_BQ_SCHEMA,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=';',
        max_bad_records=25,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        gcp_conn_id=GCP_GCS_CONNECTION_ID,
        #google_cloud_storage_conn_id=GCP_GCS_CONNECTION_ID,
        #bigquery_conn_id=GCP_BQ_CONNECTION_ID,
        task_concurrency=1
        #schema_update_options=['ALLOW_FIELD_ADDITION']
    )

def bq_table_master(i, **kwargs):
    return BigQueryExecuteQueryOperator(
        task_id='bq_table_master_' + PRODUCT_FEEDS[i]['name'],
        sql='SELECT * FROM `' + PRODUCT_FEEDS[i]['destination_bq_master_view'] + '`',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        destination_dataset_table=PRODUCT_FEEDS[i]['destination_bq_master'],
        gcp_conn_id=GCP_BQ_CONNECTION_ID
    )

def bq_to_gcs_cdn(i, **kwargs):
    return BigQueryToGCSOperator(
        task_id='bq_to_gcs_cdn_' + PRODUCT_FEEDS[i]['name'],
        source_project_dataset_table=PRODUCT_FEEDS[i]['destination_bq_master'],
        destination_cloud_storage_uris=[
            'gs://' + GCP_GCS_BUCKET_CDN + '/' + PRODUCT_FEEDS[i]['destination_gcs_cdn']
        ],
        export_format='CSV',
        print_header=True,
        field_delimiter=',',
        gcp_conn_id=GCP_BQ_CONNECTION_ID
    )
# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

with models.DAG(
    DAG_NAME,
    description = DAG_DESCRIPTION,
    start_date=DAG_START_DATE,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
	  max_active_runs=DAG_MAX_ACTIVE_RUNS,
    is_paused_upon_creation=DAG_PAUSED_UPON_CREATION,
    default_args=default_dag_args) as dag:

    start = DummyOperator(
        task_id="start",
        trigger_rule="all_success"
    )

    complete = DummyOperator(
        task_id="complete",
        trigger_rule="all_success"
    )

    for i, val in enumerate(PRODUCT_FEEDS):
        start >> gcs_to_bigquery(i) >> bq_table_master(i) >> bq_to_gcs_cdn(i) >> complete
