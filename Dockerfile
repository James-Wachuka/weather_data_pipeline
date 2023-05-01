FROM python:3.8

# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install PySpark and the Google Cloud libraries
RUN pip install pyspark prefect google-cloud-storage google-cloud-bigquery

# Copy the Python script to the container
COPY my_script.py /

# Set the command to run the script
CMD ["pyspark", "--driver-memory", "4g", "--executor-memory", "4g", "--conf", "spark.driver.maxResultSize=4g", "--master", "local[*]", "/my_script.py"]

