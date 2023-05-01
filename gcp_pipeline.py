from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery
import requests
import json
from google.cloud.exceptions import NotFound
import random


# Define the OpenWeatherMap API key and base URL
api_key = "ac1f66003fb6cf38d94e6b9312f580f9"
base_url = "https://api.openweathermap.org/data/2.5/weather"

cities_100 = ['New York', 'London', 'Paris', 'Tokyo', 'Sydney', 'Moscow', 'Beijing', 'Rio de Janeiro', 'Mumbai', 'Cairo', 'Rome', 'Berlin', 'Toronto', 'Lagos', 'Bangkok', 'Melbourne', 'Johannesburg', 'Los Angeles', 'Dubai', 'Istanbul', 'Hong Kong', 'Madrid', 'Singapore', 'Seoul', 'Barcelona', 'Vienna', 'São Paulo', 'Prague', 'Zurich', 'Budapest', 'Warsaw', 'Stockholm', 'Oslo', 'Helsinki', 'Athens', 'Brussels', 'Amsterdam', 'Copenhagen', 'Dublin', 'Reykjavik', 'Wellington', 'Auckland', 'Kuala Lumpur', 'Manila', 'Jakarta', 'New Delhi', 'Lima', 'Buenos Aires', 'Caracas', 'Lisbon', 'Havana', 'San Francisco', 'Washington DC', 'Boston', 'Seattle', 'Chicago', 'Miami', 'Atlanta', 'Houston', 'Dallas', 'Denver', 'Phoenix', 'Las Vegas', 'San Diego', 'Honolulu', 'Vancouver', 'Montreal', 'Mexico City', 'Santiago', 'Bogotá', 'Santo Domingo', 'San Juan', 'Guatemala City', 'San Salvador', 'Panama City', 'Tegucigalpa', 'Managua', 'San José', 'Helsinki', 'Osaka', 'Kyoto', 'Shanghai', 'Munich', 'Hamburg', 'Frankfurt', 'Cologne', 'Dublin', 'Belfast', 'Glasgow', 'Edinburgh', 'Liverpool', 'Manchester', 'Birmingham', 'Bristol', 'Brighton', 'Cambridge', 'Oxford', 'Leeds', 'Sheffield', 'Newcastle upon Tyne', 'Cardiff']

cities = random.sample(cities_100, 25)

# Initialize a Spark session
spark = SparkSession.builder.appName("WeatherData").getOrCreate()

# Define a function to fetch weather data for a city and return a Spark dataframe
def fetch_weather_data(city):
    # Send a request to the OpenWeatherMap API for the city's weather data
    params = {"q": city, "appid": api_key, "units": "metric"}
    response = requests.get(base_url, params=params)
    data = response.json()

    # Extract the relevant weather data from the API response
    temp = data["main"]["temp"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    # Create a Spark dataframe with the weather data for the city
    df = spark.createDataFrame([(city, temp, humidity, wind_speed)],
                               ["City", "Temperature", "Humidity", "WindSpeed"])
    return df

# Use the fetch_weather_data function to fetch weather data for all cities and merge them into a single dataframe
weather_data = None
for city in cities:
    city_weather_data = fetch_weather_data(city)
    if weather_data is None:
        weather_data = city_weather_data
    else:
        weather_data = weather_data.union(city_weather_data)

# Perform some basic processing and transformation on the weather data using PySpark
weather_data = weather_data.filter("Temperature > 10") \
                           .groupBy("City") \
                           .agg({"Humidity": "avg", "WindSpeed": "max"}) \
                           .withColumnRenamed("avg(Humidity)", "AverageHumidity") \
                           .withColumnRenamed("max(WindSpeed)", "MaxWindSpeed")

# Show the final processed and transformed weather data
weather_data.show()

# Write the weather data as a CSV file to a Google Cloud Storage bucket
bucket_name = "weather_app_dez"
file_name = "weather_data.csv"
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_name)
blob.upload_from_string(weather_data.toPandas().to_csv(index=False), content_type="text/csv")

# Create a new BigQuery table and load the data from the CSV file
table_name = "dez-dtc-23-384116.weather_app.weather_data"
bigquery_client = bigquery.Client()
table = bigquery.Table(table_name)
schema = [bigquery.SchemaField("City", "STRING"),
          bigquery.SchemaField("AverageHumidity", "FLOAT"),
          bigquery.SchemaField("MaxWindSpeed", "FLOAT")]
table.schema = schema
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True)
job = bigquery_client.load_table_from_uri(f"gs://{bucket_name}/{file_name}", table, job_config=job_config)
job.result()
print(f"Loaded {job.output_rows} rows into BigQuery table {table_name}")
