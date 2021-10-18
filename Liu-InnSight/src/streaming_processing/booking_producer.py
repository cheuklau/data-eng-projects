# Import KafkaProducer package
# Documentation: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
from kafka import KafkaProducer
import time
from time import strftime

# Define Kafka topic to write to
topic = 'booking'


def main():

    # Set up the Kafka Producer.
    # The Kafka Producer is the client that publishes records to the Kafka cluster.
    # 'bootstrap_servers' define the server that the Producer should contact to bootstrap
    # initial cluster metadata. This does not need to be the full node list.
    # For this exercise, we are expected to have Kafka set up on localhost.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Open booking data file produced by Faker data.
    file_name = "booking.txt"
    print('Start importing booking event data...')

    # For each line, we parse it to form the following:
    # zipcode. price, datetime
    # We then encode it and use the Kafka Producer to send it to the specified
    # topic in the cluster.
    record = 1
    with open(file_name) as f:
        for line in f:
            cur_time = strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            booking_info = line.replace('\n', '').split(',')
            zipcode = booking_info[1]
            price = booking_info[3]
            msg = zipcode + ',' + price + ',' + cur_time
            # send() publishes a message to a topic
            producer.send(topic, msg.encode('utf-8'))
            print(f"Total records sent: {record}")
            record += 1
    # flush() makes all buffered records immediately available to send
    # and blocks on the completioon of the requests associated with these records.
    producer.flush()
    f.close()


if __name__ == '__main__':
    main()
