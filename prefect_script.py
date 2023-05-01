from google.cloud import storage, bigquery
import requests
import json
from google.cloud.exceptions import NotFound
import random
from prefect import task, Flow
from pyspark.sql import SparkSession


# Define the OpenWeatherMap API key and base URL
api_key = "ac1f66003fb6cf38d94e6b9312f580f9"
base_url = "https://api.openweathermap.org/data/2.5/weather"

spark = SparkSession.builder.appName("WeatherData").getOrCreate()

# Define the cities list
cities_100 = ['New York', 'London', 'Paris', 'Tokyo', 'Sydney', 'Moscow', 'Beijing', 'Rio de Janeiro', 'Mumbai', 'Cairo']
cities = random.sample(cities_100, 5)

# Define a function to fetch weather data for a city and return a Spark dataframe
@task
def fetch_weather_data(cities):
    weather_data = None
    for city in cities:
        # Send a request to the OpenWeatherMap API for the city's weather data
        params = {"q": city, "appid": api_key, "units": "metric"}
        response = requests.get(base_url, params=params)
        data = response.json()

        # Extract the relevant weather data from the API response
        temp = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]

        # Create a Spark dataframe with the weather data for the city
        city_weather_data = spark.createDataFrame([(city, temp, humidity, wind_speed)],
                                ["City", "Temperature", "Humidity", "WindSpeed"])
        if weather_data is None:
            weather_data = city_weather_data
        else:
            weather_data = weather_data.union(city_weather_data)
    return weather_data
    
    
'''
# Use the fetch_weather_data function to fetch weather data for all cities and merge them into a single dataframe
@task
def merge_weather_data(cities):
    weather_data = None
    for city in cities:
        city_weather_data = fetch_weather_data(city)
        if weather_data is None:
            weather_data = city_weather_data
        else:
            weather_data = weather_data.union(city_weather_data)
    return weather_data
'''
# Perform some basic processing and transformation on the weather data using PySpark
@task
def process_weather_data(weather_data):
    processed_data = weather_data.filter("Temperature > 10") \
                           .groupBy("City") \
                           .agg({"Humidity": "avg", "WindSpeed": "max"}) \
                           .withColumnRenamed("avg(Humidity)", "AverageHumidity") \
                           .withColumnRenamed("max(WindSpeed)", "MaxWindSpeed")
    return processed_data

# Write the weather data as a CSV file to a Google Cloud Storage bucket
@task
def write_weather_data_to_gcs(weather_data):
    bucket_name = "weather_app_dez"
    file_name = "weather_data.csv"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(weather_data.toPandas().to_csv(index=False), content_type="text/csv")
    return f"gs://{bucket_name}/{file_name}"

# Create a new BigQuery table and load the data from the CSV file
@task
def write_weather_data_to_bigquery(uri):
    table_name = "dez-dtc-23-384116.weather_app.weather_data"
    bigquery_client = bigquery.Client()
    table = bigquery.Table(table_name)
    schema = [bigquery.SchemaField("City", "STRING"),
              bigquery.SchemaField("AverageHumidity", "FLOAT"),
              bigquery.SchemaField("MaxWindSpeed", "FLOAT")]
    table.schema = schema
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True)
    job = bigquery_client.load_table_from_uri(uri, table, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into BigQuery table {table_name}")

with Flow("Weather Data Pipeline") as flow:

    cities = random.sample(cities_100, 5)
    weather_dat = fetch_weather_data(cities)
    processed_data = process_weather_data(weather_dat)
    uri = write_weather_data_to_gcs(processed_data)
    write_weather_data_to_bigquery(uri)

flow.run()