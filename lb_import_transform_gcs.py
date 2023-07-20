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

DAG_NAME = "merklenl_leenbakker_productfeed_import_transform_gcs" # DAG name (proposed format: lowercase underscore). Should be unique.
DAG_DESCRIPTION = "Import the Leenbakker productfeed to Big Query"
DAG_START_DATE = datetime.datetime(2021, 5, 31, tzinfo=LOCAL_TZ) # Startdate. When setting the "catchup" parameter to True, you can perform a backfill when you insert a specific date here like datetime(2021, 6, 20)
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
    'email': models.Variable.get('email_monitoring'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=30)
}

# --------------------------------------------------------------------------------
# Workflow configuration
# --------------------------------------------------------------------------------

# Import

GCP_GCS_CONNECTION_ID = 'gcp_merkle_airflow'
GCP_GCS_BUCKET = 'lb-gcs-import'
GCP_GCS_BUCKET_SOURCE = 'feeds_prod'
GCP_BQ_CONNECTION_ID = 'gcp_merkle_airflow'
GCP_BQ_PROJECT = 'future-cabinet-206910'
SFTP_CONNECTION_ID = 'sftp_client_leenbakker_ibm'

GCP_GCS_BUCKET_CDN = 'oogst-cdn-static'

PRODUCT_FEEDS = [{
		'name': 'productfeed_nl_source',
		'source_gcs': 'priceFeed/priceFeed_lbnl.csv',
		'destination_gcs_cdn': 'external/leenbakker/productfeed_nl_master.csv',
		'destination_bq': 'future-cabinet-206910.lb_business_data.productfeed_nl_source',
		'destination_bq_master_view': 'future-cabinet-206910.lb_business_data.view_productfeed_nl_master',
		'destination_bq_master': 'future-cabinet-206910.lb_business_data.productfeed_nl_master'
	},{
		'name': 'productfeed_be_nl_source',
		'source_gcs': 'priceFeed/priceFeed_lbbe_nl.csv',
		'destination_gcs_cdn': 'external/leenbakker/productfeed_be_nl_master.csv',
		'destination_bq': 'future-cabinet-206910.lb_business_data.productfeed_be_nl_source',
		'destination_bq_master_view': 'future-cabinet-206910.lb_business_data.view_productfeed_be_nl_master',
		'destination_bq_master': 'future-cabinet-206910.lb_business_data.productfeed_be_nl_master'
	},{
		'name': 'productfeed_be_fr_source',
		'source_gcs': 'priceFeed/priceFeed_lbbe_fr.csv',
		'destination_gcs_cdn': 'external/leenbakker/productfeed_be_fr_master.csv',
		'destination_bq': 'future-cabinet-206910.lb_business_data.productfeed_be_fr_source',
		'destination_bq_master_view': 'future-cabinet-206910.lb_business_data.view_productfeed_be_fr_master',
		'destination_bq_master': 'future-cabinet-206910.lb_business_data.productfeed_be_fr_master'
	}
]

# BQ Schema

# BQ Schema
GCP_BQ_SCHEMA = [
  {
    "mode": "NULLABLE",
    "name": "productid",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "title",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "brand_1",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "brand_serie",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ean",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "mpn",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "additional_image_link",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "short_description",
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
    "name": "discount",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "delivery_period",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "delivery_costs",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "deeplink_l1",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "deeplink_l2",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "deeplink_l3",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "deeplink_pdp",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "image_url_s",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "image_url_m",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "image_url",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "image_url_l",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_id_l1",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_id_l2",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_id_l3",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_id_l4",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_id_l5",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "master_category_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "promo_price_lable",
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
  {
    "mode": "NULLABLE",
    "name": "categoryPath",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "stock_status",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "products_in_stock",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "colour",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "material",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "last_change_date",
    "type": "TIMESTAMP"
  },
  {
    "mode": "NULLABLE",
    "name": "product_height",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "product_length",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "product_width",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "product_diameter",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "distributionmethod",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "parceltype",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "averageRating",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "reviewCount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "seller",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "msrp",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "figure",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "BV_FE_FAMILY",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "BV_FE_EXPAND",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "BSR_STYLE_GROUP",
    "type": "STRING"
  }
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