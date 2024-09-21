import logging
import os

import functions_framework
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery, storage

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_query_from_gcs(storage_client: storage.Client, bucket_name: str, file_name: str) -> str:
    """Load SQL query from GCS bucket.

    Args:
        storage_client (storage.Client): GCS client.
        bucket_name (str): Bucket name.
        file_name (str): File name.

    Returns:
        str: SQL query.
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
    except NotFound as e:
        logger.error(f"Bucket or file not found: {e}")
        raise e
    except Forbidden as e:
        logger.error(f"Access denied: {e}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e

    return blob.download_as_string().decode("utf-8")


def execute_query(bq_client: bigquery.Client, query: str, table_ref: str) -> None:
    """Execute SQL query in BigQuery.

    Args:
        bq_client (bigquery.Client): BigQuery client.
        query (str): SQL query.
        table_ref (str): Table reference
    """
    try:
        job_config = bigquery.QueryJobConfig(destination=table_ref)
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
    except NotFound as e:
        logger.error(f"Table not found: {e}")
        raise e
    except Forbidden as e:
        logger.error(f"Access denied: {e}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e


@functions_framework.http
def main(request: functions_framework.Request) -> str:
    """HTTP Cloud Function.

    Args:
        request (flask.Request): The request object.

    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`.
    """
    # Get environment variables
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("DATASET_ID")
    table_id = os.environ.get("TABLE_ID")
    bucket_name = os.environ.get("BUCKET_NAME")
    file_name = request.args.get("file_name")

    # Load SQL query from GCS
    gcs_client = storage.Client()
    query = load_query_from_gcs(gcs_client, bucket_name, file_name)

    # Run query
    bq_client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    execute_query(bq_client, query, table_ref)

    return "Query executed successfully."
