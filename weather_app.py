import requests
from pyspark.sql import SparkSession

# Define the OpenWeatherMap API key and base URL
api_key = "ac1f66003fb6cf38d94e6b9312f580f9"
base_url = "https://api.openweathermap.org/data/2.5/weather"

# Define a list of cities for which to fetch weather data
cities = ["New York", "London", "Paris", "Tokyo", "Sydney"]

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
