from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

spark = SparkSession.builder.appName('DataCleaning').getOrCreate()

mode = "append"
url = "jdbc:postgresql://{}:{}/price_insight_db".format(config['DEFAULT']['POSTGRESQL_IP'],
                                                        config['DEFAULT']['POSTGRESQL_PORT'])
properties = {"user": config['DEFAULT']['DB_USER'],
              "password": config['DEFAULT']['DB_PASSWORD'],
              "driver": "org.postgresql.Driver"}

get_price_udf = udf(lambda x: float(x.replace('$', '').replace(',', '')), FloatType())

selected_column_name_list = ['id', 'last_scraped', 'name', 'listing_url', 'city', 'zipcode', 'price',
                             'property_type', 'bedrooms', 'accommodates', 'number_of_reviews',
                             'review_scores_rating', 'reviews_per_month']

city = sys.argv[1]

fh = open("s3_file_urls.txt", "r")
file_list = fh.read().splitlines()
for file in file_list:
    try:
        raw_df = spark.read.csv(file, header=True, inferSchema=True, multiLine=True, escape='"')
        df_with_selected_cols = raw_df.select(selected_column_name_list)
        df_filter_na = df_with_selected_cols.na.drop(subset=['zipcode', 'price'])
        df_filter_null = df_filter_na.filter(df_filter_na['zipcode'].isNotNull())
        df_zipcode_formatted = df_filter_null.withColumn('zipcode', df_filter_null['zipcode'].cast(IntegerType()))
        df_price_formatted = df_zipcode_formatted.withColumn('price', get_price_udf(df_zipcode_formatted['price']))
        df_tsp_renamed = df_price_formatted.withColumnRenamed('last_scraped', 'timestamp')
        df_with_month = df_tsp_renamed.withColumn('month', month(df_tsp_renamed['timestamp']))
        df_final = df_with_month.withColumn('date', df_with_month['timestamp'].cast('date'))
        df_final.show()

        # write cleaned data to database
        df_final.write.jdbc(url=url, table="listing_" + city, mode=mode, properties=properties)
    except Exception as e:
        print(e)
