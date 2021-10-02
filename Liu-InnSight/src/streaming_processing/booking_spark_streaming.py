"""
This is a script to ingest real time booking events data via Kafka
into Spark Streaming. The data is saved to an external database PostgreSQL.
"""

from pyspark.streaming.kafka import KafkaUtils
import configparser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# database config
config = configparser.ConfigParser()
config.read('config.ini')

mode = "append"
url = "jdbc:postgresql://{}:{}/price_insight_db".format(config['DEFAULT']['POSTGRESQL_IP'],
                                                        config['DEFAULT']['POSTGRESQL_PORT'])
properties = {"user": config['DEFAULT']['DB_USER'],
              "password": config['DEFAULT']['DB_PASSWORD'],
              "driver": "org.postgresql.Driver"}

# fire up the consumer
topic = 'booking'


def main():
    sc = SparkContext(appName="BookingDataProcessing")
    # set microbatch interval as 10 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 10)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {
        "metadata.broker.list": "ec2-52-89-17-35.us-west-2.compute.amazonaws.com:9092,ec2-52-89-17-35.us-west-2.compute.amazonaws.com:9092"})
    # Load JSON data from Kafka
    lines = kafkaStream.map(lambda x: x[1])
    booking_rdd = lines.map(lambda x: x.replace('"', '').split(','))
    booking_rdd.pprint()

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            # Example: ['85371', '352', '2019-02-11 07:56:44']
            row_rdd = rdd.map(lambda w: Row(zipcode=w[0], price=w[1], timestamp=w[2]))
            words_df = spark.createDataFrame(row_rdd)
            words_df.show()
            words_df.write.jdbc(url=url, table='streaming_data', mode=mode, properties=properties)
        except Exception as e:
            print(e)

    booking_rdd.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
