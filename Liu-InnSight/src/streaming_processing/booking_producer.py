"""
This kafka producer job will generate streaming data showing real time booking record.
"""
from kafka import KafkaProducer, KafkaConsumer
import time
from time import strftime

topic = 'booking'


def main():
    # set up the producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # Open booking data file
    file_name = "booking.txt"
    print('Start importing booking event data...')

    with open(file_name) as f:
        for line in f:
            cur_time = strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            booking_info = line.replace('\n', '').split(',')
            zipcode = booking_info[1]
            price = booking_info[3]
            msg = zipcode + ',' + price + ',' + cur_time
            producer.send(topic, msg.encode('utf-8'))
            print("now it is sending... ")
    producer.flush()
    f.close()


if __name__ == '__main__':
    main()
