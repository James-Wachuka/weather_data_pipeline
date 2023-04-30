FROM python:3.8-slim-buster

RUN pip install apache-airflow[google]==2.2.0 && \
    pip install google-cloud-storage==1.43.0 && \
    pip install google-cloud-bigquery==2.20.0 &&\
    pip install google-auth==2.6.0 && \
    pip install google-auth-oauthlib==0.4.6 &&\
    pip isntall google-auth-httplib2==0.1.0

# Set the working directory
WORKDIR /usr/local/airflow

# Copy the DAG file into the container
COPY weather_dag.py /usr/local/airflow/dags/
COPY weather_app.py /usr/local/airflow/
COPY google_cred.json /usr/local/airflow/


ENV GOOGLE_CREDENTIALS=google_cred.json

# Initialize Airflow database
RUN airflow db init

# Start the Airflow webserver and scheduler
CMD ["bash", "-c", "airflow webserver -p 8080 & airflow scheduler"]
