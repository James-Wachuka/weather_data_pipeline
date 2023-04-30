from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator

# Define the default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
dag = DAG("weather_data_pipeline",
          default_args=default_args,
          schedule_interval=timedelta(days=1))

# Define the PySpark job to execute
job_file = "gs://weather_app_dez/weather_data.py"
job_args = ["--py-files", "gs://weather_app_dez/weather_data.py"]
job = {"weather_app.py": job_file, "args": job_args}

# Define the Dataproc cluster configuration
cluster_config = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
}

# Define the operators to execute in the DAG
with dag:
    # Create a Dataproc cluster to execute the PySpark job
    create_cluster = DataprocCreateClusterOperator(task_id="create_cluster",
                                                   project_id="dez-dtc-23-384116",
                                                   region="us",
                                                   cluster_name="weather-data-cluster",
                                                   cluster_config=cluster_config,
                                                   retry=2,
                                                   retry_delay=timedelta(minutes=5))
    
    # Submit the PySpark job to the Dataproc cluster
    submit_job = DataprocSubmitJobOperator(task_id="submit_job",
                                           project_id="dez-dtc-23-384116",
                                           region="us",
                                           job=job,
                                           cluster_name="weather-data-cluster",
                                           retry=2,
                                           retry_delay=timedelta(minutes=5))
    
    # Delete the Dataproc cluster after the PySpark job completes
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_cluster",
                                                   project_id="dez-dtc-23-384116",
                                                   region="us",
                                                   cluster_name="weather-data-cluster",
                                                   retry=2,
                                                   retry_delay=timedelta(minutes=5))
    
    # Load the processed weather data from GCS into BigQuery
    load_to_bigquery = GCSToBigQueryOperator(task_id="load_to_bigquery",
                                             bucket="weather_app_dez",
                                             source_objects=["output/*"],
                                             destination_project_dataset_table="dez-dtc-23-384116:weather_app_dez.weather_data",
                                             schema_fields=[{"name": "City", "type": "STRING"},
                                                            {"name": "AverageHumidity", "type": "FLOAT"},
                                                            {"name": "MaxWindSpeed", "type": "FLOAT"}],
                                             write_disposition="WRITE_TRUNCATE",
                                             autodetect=False)
    
    # Define the execution order of the operators in the DAG
    create_cluster >> submit_job >> delete_cluster >> load_to_bigquery
