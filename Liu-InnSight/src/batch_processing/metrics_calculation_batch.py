from pyspark.sql import SparkSession
import sys
import configparser

"""
Usage: python3 metrics_calculation_batch.py <city name or all>

This Spark job reads cleaned data from Postgres, performs metrics calculation then
stores the output back into Postgres.

Requires config.ini of the form:
['DEFAULT']
POSTGRESQL_IP = <postgres ip>
POSTGRESQL_PORT = <postgres port e.g., 5432>
DB_USER = <database user>
DB_PASSWORD = <database password>

Note: There doesnt appear to be a metrics calculation script for reading from Parquet.
"""

# Read in configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Create Spark session
spark = SparkSession.builder.appName("CalculateMetrics").getOrCreate()

# Set up Postgres parameters
mode = "overwrite"
url = "jdbc:postgresql://{}:{}/price_insight_db".format(config['DEFAULT']['POSTGRESQL_IP'],
                                                        config['DEFAULT']['POSTGRESQL_PORT'])
properties = {"user": config['DEFAULT']['DB_USER'],
              "password": config['DEFAULT']['DB_PASSWORD'],
              "driver": "org.postgresql.Driver"}

# Grab the city name from the argument list
city = sys.argv[1]

# Read dataset from Postgres
df_listing = spark.read.jdbc(url=url, table="listing_" + city, properties=properties)

# Calculate the average price by (zipcode, date)
result_average_price_trend = df_listing.groupBy("zipcode", "timestamp").agg({'price': 'mean'})

# Calculate average price by (zipcode, month)
result_seasonality = df_listing.groupBy("zipcode", "month").agg({'price': 'mean'})

# Creates a temporary table called listing_info using the dataframe read from Postgres
df_listing.createOrReplaceTempView('listing_info')

# Calculate rental type distribution using a Spark SQL query on the
# temporary listing_info table
result_rental_type_distribution = spark.sql("""
SELECT zipcode,
       property_type,
       Count(1) AS count
FROM   (SELECT zipcode,
               id,
               property_type,
               Max(timestamp) AS t_max
        FROM   listing_info
        GROUP  BY zipcode,
                  id,
                  property_type) sub
GROUP  BY zipcode,
          property_type
""")

# Calculate room type distribution using a Spark SQL query on the
# temporary listing_info table
result_room_type_distribution = spark.sql("""
SELECT zipcode,
       bedrooms,
       Count(1) AS count
FROM   (SELECT zipcode,
               id,
               bedrooms,
               Max(timestamp) AS t_max
        FROM   listing_info
        GROUP  BY zipcode,
                  id,
                  bedrooms) sub
GROUP  BY zipcode,
          bedrooms
""")

# Write each of the above dataframes back to Postgres
# Note that the mode is overwrite
try:
       result_average_price_trend.write.jdbc(url=url, table="average_price_trend_"+city, mode=mode, properties=properties)
except Exception as e:
       print(f"Unable to write result_average_price_trend for ${city}. Exception:\n ${e})

try:
       result_seasonality.write.jdbc(url=url, table="seasonality_"+city, mode=mode, properties=properties)
except Exception as e:
       print(f"Unable to write result_seasonlity for ${city}. Exception:\n ${e})

try:
       result_rental_type_distribution.write.jdbc(url=url, table="result_rental_type_distribution_"+city, mode=mode, properties=properties)
except Exception as e:
       print(f"Unable to write result_rental_type_distribution for ${city}. Exception:\n ${e})

try:
       result_room_type_distribution.write.jdbc(url=url, table="result_room_type_distribution_"+city, mode=mode, properties=properties)
except Exception as e:
       print(f"Unable to write result_room_type_distribution for ${city}. Exception:\n ${e})
