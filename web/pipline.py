import multiprocessing
import json
import random
import time
from confluent_kafka import Producer, Consumer
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import spark_kafka_integration
import fake_data

def generate_fake_ecom_events():
    while True:
        fake_data.simulate_ecommerce_events()
        time.sleep(random.uniform(0.1, 2.0))

def process_kafka_stream():
    spark_kafka_integration.process_kafka_stream()

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    event_generation_process = multiprocessing.Process(target=generate_fake_ecom_events)
    event_generation_process.start()

    spark_processing_process = multiprocessing.Process(target=process_kafka_stream)
    spark_processing_process.start()
    
    event_generation_process.join()
    spark_processing_process.join()
    
    
