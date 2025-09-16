from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14), 
}

with DAG(
    dag_id="Flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # or schedule=None (newer syntax)
    catchup=False,
) as dag:

    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow_project_gds")
    bq_project = Variable.get("bq_project", default_var="amazing-centaur-465013-i9")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    # Task-1: File Sensor
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow_project-1/source-{env}/flight_booking.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/airflow_project-1/spark-job/spark_transformation_job.py",
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "80085016100-compute@developer.gserviceaccount.com",
                "network_uri": "projects/amazing-centaur-465013-i9/global/networks/default",
                "subnetwork_uri": "projects/amazing-centaur-465013-i9/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="amazing-centaur-465013-i9",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    file_sensor >> pyspark_task
