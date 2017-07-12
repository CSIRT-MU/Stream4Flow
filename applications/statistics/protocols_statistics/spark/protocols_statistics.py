# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016 Milan Cermak <cermak@ics.muni.cz>, Institute of Computer Science, Masaryk University
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

"""
 Counts number of flows, packets, and bytes for TCP, UDP, and other flows received from Kafka every 10 seconds.

 Usage:
    protocols_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz
    <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic>

 To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
 you can run the example
    $ ./run-application.sh ./examples/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output
"""


import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver

from kafka import KafkaProducer  # Kafka Python client


def send_to_kafka(data, producer, topic):
    """
    Send given data to the specified kafka topic.

    :param data: data to send
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """
    producer.send(topic, str(data))
    producer.flush()


def process_results(results, producer, topic):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
        {"@type": "protocols_statistics", "protocol" : <protocol>, "flows": <#flows>, "packets": <#packets>, "bytes": <#bytes>}

    :param results: map of UDP, TCP, and other statistics ("protocol", (#flows, #packets, #bytes))
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """

    # Transform given results into the JSON
    output_json = ""
    for key, value in results.iteritems():
        output_json += "{\"@type\": \"protocols_statistics\", \"protocol\": \"" + key + "\", \"flows\": " + str(value[0]) + ", \"packets\": " + str(value[1]) + ", \"bytes\": " + str(value[2]) + "}\n"

    # Check if there are any results
    if output_json:
        # Print results to standard output
        cprint(output_json)

        # Send results to the specified kafka topic
        send_to_kafka(output_json, producer, topic)


def get_protocol_name(protocol_identifier):
    """
    Returns protocol name for the given identifier.

    :param protocol_identifier: Number representing the protocol.
    :return: string "tcp" if protocol_identifier is 6, "udp" if protocol_identifier is 17, and "other" otherwise
    """

    # Check identifier and return corresponfing string
    if protocol_identifier == 6:
        return "tcp"
    elif protocol_identifier == 17:
        return "udp"
    else:
        return "other"


def count_protocols_statistics(flows_stream, window_duration, window_slide):
    """
    Count number of transferred flows, packets, and bytes of TCP, UDP, and other protocols using Spark Streaming functions.

    :param flows_stream: DStream of parsed flows in the JSON format
    :param window_duration: Duration of the time window for statistics count
    :param window_slide: Slide interval of the time window for statistics count (typically same as window_duration)
    :return: union DStream of UDP, TCP, and other protocols statistics
    """

    # Check required flow keys
    flows_stream_checked = flows_stream.filter(lambda flow_json: ("ipfix.protocolIdentifier" in flow_json.keys()))

    # Set protocol name as a key and map number of flows, packets, and bytes
    flows_mapped = flows_stream_checked.map(lambda flow_json: (get_protocol_name(flow_json["ipfix.protocolIdentifier"]), (1, flow_json["ipfix.packetDeltaCount"], flow_json["ipfix.octetDeltaCount"])))

    # Reduce mapped flows to get statistics for smallest analysis interval and reduce volume of processed data
    flows_reduced = flows_mapped.reduceByKey(lambda actual, update: (
                                                 actual[0] + update[0],
                                                 actual[1] + update[1],
                                                 actual[2] + update[2]
                                             ))

    # Set time window and compute statistics over the window using the same reduce
    flows_statistics = flows_reduced.window(window_duration, window_slide)\
                                    .reduceByKey(lambda actual, update: (
                                                     actual[0] + update[0],
                                                     actual[1] + update[1],
                                                     actual[2] + update[2]
                                                 ))

    # Return computed statistics
    return flows_statistics


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)

    # Parse obtained arguments
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = 10  # Analysis window duration (10 seconds)
    window_slide = 10  # Slide interval of the analysis window (10 seconds)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name, {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    flows_json = input_stream.map(lambda x: json.loads(x[1]))

    # Count statistics of the UDP, TCP, and other protocols
    statistics = count_protocols_statistics(flows_json, window_duration, window_slide)

    # Initialize kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=args.output_zookeeper,
                                   client_id="spark-producer-" + application_name)

    # Process computed statistics and send them to the specified host
    statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, args.output_topic))

    # Start Spark streaming context
    ssc.start()
    ssc.awaitTermination()
