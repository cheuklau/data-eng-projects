import configparser
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtil


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# Read in database configuration file 'config.ini'.
# ['DEFAULT']
#   POSTGRESQL_IP = <postgres IP>
#   POSTGRESQL_PORT = <postgres port>
#   DB_USER = <postgres database user>
#   DB_PASSWORD = <postgres database password>
config = configparser.ConfigParser()
config.read('config.ini')

# Define database properties.
# Note that database name is 'price_insight_db'.
mode = "append"
url = "jdbc:postgresql://{}:{}/price_insight_db".format(config['DEFAULT']['POSTGRESQL_IP'],
                                                        config['DEFAULT']['POSTGRESQL_PORT'])
properties = {"user": config['DEFAULT']['DB_USER'],
              "password": config['DEFAULT']['DB_PASSWORD'],
              "driver": "org.postgresql.Driver"}

# Kafka topic name
topic = 'booking'


def main():

    # Define SparkContext
    # This is used by Driver Process to establish communication with the cluster
    # and the resource managers to coordinate and execute jobs.
    sc = SparkContext(appName="BookingDataProcessing")

    # Define StreamingContext
    # Main entry point for Spark Streaming functionality.
    # We pass in the SparkContext and set the batchDuration to 10 seconds.
    # The batchDuration is the time interval at which streaming data will be divided into batches.
    ssc = StreamingContext(sc, 10)

    # Create an input stream (DStream) that directly pulls messages from Kafka Brokers without
    # using any receiver. Stream guarantees that each message is included in
    # transformations exactly once.
    # We pass in the StreamingContext, the topic and the Kafka Broker list.
    # Note the Broker IPs are hardcoded from the original project.
    # Note: The consumed offsets are tracked by the stream itself. For operability
    # with monitoring tools that depend on Zookeeper, you have to update Kafka/Zookeeper
    # yourself from streaming application.
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {
        "metadata.broker.list": "ec2-52-89-17-35.us-west-2.compute.amazonaws.com:9092,ec2-52-89-17-35.us-west-2.compute.amazonaws.com:9092"})

    # Load JSON data from Kafka
    # map() returns a new DStream by applying a function to all elements of this DStream.
    # lines grabs the value of each stream.
    # booking_rdd removes all double quotes and splits on the comma.
    lines = kafkaStream.map(lambda x: x[1])
    booking_rdd = lines.map(lambda x: x.replace('"', '').split(','))
    booking_rdd.pprint()

    def process(time, rdd)
        """
        Convert an RDD to a DataFrame and write it to Postgres.

        """

        try:
            # Get SparkSession.
            # Note that we wrote a function to ensure only one is created
            # for the entire execution of this script.
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            # Example: ['85371', '352', '2019-02-11 07:56:44']
            row_rdd = rdd.map(lambda w: Row(zipcode=w[0], price=w[1], timestamp=w[2]))
            words_df = spark.createDataFrame(row_rdd)
            words_df.show()

            # Write DataFrame to Postgres
            words_df.write.jdbc(url=url, table='streaming_data', mode=mode, properties=properties)
        except Exception as e:
            print(e)

    # foreachRDD() applies a function to each RDD in this DStream.
    # Our function converts each RDD to a DataFrame which is then written to Postgres.
    booking_rdd.foreachRDD(process)

    # Start execution of the Stream.
    ssc.start()

    # Wait for execution of the Stream to stop.
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
