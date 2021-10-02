# Innsight

## Introduction

InnSight is a platform providing Airbnb hosts insight on Airbnb rental properties and local market dynamics.

## Architecture

The platform architecture is shown in the following figure.

![tech stack](./common/images/architecture.png)

The top half of the platform architecture is composed of the following:
- Dataset from [InsideAirbnb](http://insideairbnb.com/get-the-data.html).
    * Historical datasize is approximately 250GB.
    * New data is added at approximately 10GB per month.
- [Python](https://www.python.org/) script is used to move the InsideAirbnb dataset into [AWS S3](https://aws.amazon.com/s3/).
- [Spark](https://spark.apache.org/) is used to read the data from S3, unify the raw data into a common schema and to remove duplicates. The results are then stored into [Parquet](https://parquet.apache.org/).
- Spark is used again to read the data from Parquet, and to perform metrics calculation and then store the results into [Postgres](https://www.postgresql.org/).
- [Airflow](https://airflow.apache.org/) is used to run the above stages once per month as new data arrives.
- [Flask](https://flask.palletsprojects.com/en/2.0.x/) is used to present the data from Postgres in a [Dash](https://plotly.com/dash/) UI.

The bottom half of the platform architecture is composed of the following:
- Real-time booking data.
    * Simulated for now, but can be fetched from APIs e.g., the PredictHQ Events data [API](https://www.predicthq.com/apis).
- [Kafka](https://kafka.apache.org/) ingests the real-time data and passes it to [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html).
- Spark Streaming processes the real-time data and stores it in Postgres.
- Flask is used to present the data from Postgres in a Dash UI.

## File Description



