# -*- coding: utf-8 -*-

# MIT License
#
# Copyright (c) 2016 Tomas Pavuk <433592@mail.muni.cz>, Institute of Computer Science, Masaryk University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import ujson as json  # Fast JSON parser
import sys  # Common system functions
import os  # Common operating system functions

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver

from kafka import KafkaProducer  # Kafka Python client


def initialize_and_parse_input_stream(input_zookeeper, input_topic, microbatch_duration):
    """
    Initialize spark context, streaming context, input DStream and parse json from DStream.

    :param input_zookeeper: input zookeeper hostname:port
    :param input_topic: input kafka topic
    :param microbatch_duration: duration of micro batches in seconds

    :return ssc, parsed_stream: initialized streaming context and json with data from DStream
    """
    # Application name used as identifier
    application_name = os.path.basename(sys.argv[0])
    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, microbatch_duration)

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, input_zookeeper, "spark-consumer-" + application_name,
                                           {input_topic: 1})

    # Parse input stream in the json format
    parsed_stream = input_stream.map(lambda line: json.loads(line[1]))

    return ssc, parsed_stream


def initialize_kafka_producer(output_zookeeper):
    """
    Initialize Kafka producer for output.

    :param: output_zookeper: output zookeeper hostname:port
    :return: Initialzied Kafka producer through which output data can be send to the Kafka topic
    """
    # Application name used as identifier
    try:
        application_name = os.path.basename(sys.argv[0])
        return KafkaProducer(bootstrap_servers=output_zookeeper, client_id="spark-producer-" + application_name)
    except Exception as e:
        cprint("[warning] Unable to initialize kafka producer.", "red")


def process_data_and_send_result(processed_input, kafka_producer, output_topic, processing_function):
    """
    For each RDD in processed_input call the processing function.

    :param processed_input: input which is being formatted by processing_function and send to output
    :param kafka_producer: producer through which output is sent
    :param output_topic: output kafka topic
    :param processing_function: function which formats output and calls send_data_to_kafka
    """
    # Call the processing function
    processed_input.foreachRDD(lambda rdd: processing_function(rdd.collectAsMap(), kafka_producer, output_topic))

    # Send any remaining buffered records
    try:
        kafka_producer.flush()
    except Exception as e:
        cprint("[warning] Unable to access producer.", "yellow")


def send_data_to_kafka(data, producer, topic):
    """
    Send given data to the specified kafka topic.

    :param data: data to send
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """
    try:
        producer.send(topic, str(data))
    except KafkaTimeoutError as e:
        cprint("[warning] Unable to send data through topic " + topic + ".", "yellow")


def spark_start(ssc):
    """
    Start Spark streaming context

    :param ssc: initialized streaming context
    """
    ssc.start()
    ssc.awaitTermination()
