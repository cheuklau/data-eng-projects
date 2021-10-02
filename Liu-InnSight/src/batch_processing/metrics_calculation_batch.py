from pyspark.sql import SparkSession
import sys
import configparser

spark = SparkSession.builder.appName("CalculateMetrics").getOrCreate()
config = configparser.ConfigParser()
config.read('config.ini')

city = sys.argv[1]

mode = "overwrite"
url = "jdbc:postgresql://{}:{}/price_insight_db".format(config['DEFAULT']['POSTGRESQL_IP'],
                                                        config['DEFAULT']['POSTGRESQL_PORT'])
properties = {"user": config['DEFAULT']['DB_USER'],
              "password": config['DEFAULT']['DB_PASSWORD'],
              "driver": "org.postgresql.Driver"}

df_listing = spark.read.jdbc(url=url, table="listing_" + city, properties=properties)

# calculate the average price by (zipcode,date), for the trend chart
result_average_price_trend = df_listing.groupBy("zipcode", "timestamp").agg({'price': 'mean'})

# calculate average price by (zipcode, month)
result_seasonality = df_listing.groupBy("zipcode", "month").agg({'price': 'mean'})

# register the df_listing as a SQL temp review
df_listing.createOrReplaceTempView('listing_info')

# calculate rental type distribution
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

# calculate room type distribution
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

try:
    result_average_price_trend.write.jdbc(url=url, table="average_price_trend_" + city, mode=mode,
                                          properties=properties)
    result_seasonality.write.jdbc(url=url, table="seasonality_" + city, mode=mode, properties=properties)
    result_rental_type_distribution.write.jdbc(url=url, table="result_rental_type_distribution_" + city, mode=mode,
                                               properties=properties)
    result_room_type_distribution.write.jdbc(url=url, table="result_room_type_distribution_" + city, mode=mode,
                                             properties=properties)
except Exception as e:
    print(e)
