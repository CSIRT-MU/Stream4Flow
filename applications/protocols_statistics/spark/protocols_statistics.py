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
    protocols_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh <output-hostname>:<output-port>

 To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
 you can run the example
    $ ./run-application.sh ./examples/protocols_statistics.py -iz producer:2181 -it ipfix.entry -oh consumer:20101
"""


import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver


def send_data(data, output_host):
    """
    Send given data to the specified host using standard socket interface.

    :param data: data to send
    :param output_host: data receiver in the "hostname:port" format
    """

    # Split outputHost hostname and port
    host = output_host.split(':')

    # Prepare a TCP socket.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to the outputHost and send given data
    try:
        sock.connect((host[0], int(host[1])))
        sock.send(data)
    except socket.error:
        cprint("[warning] Unable to connect to host " + output_host, "blue")
    finally:
        sock.close()


def process_results(results, output_host):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
        {"@type": "protocols_statistics", "protocol" : <protocol>, "flows": <#flows>, "packets": <#packets>, "bytes": <#bytes>}

    :param results: map of UDP, TCP, and other statistics ("protocol", (#flows, #packets, #bytes))
    :param output_host: results receiver in the "hostname:port" format
    """

    # Transform given results into the JSON
    output_json = ""
    for key, value in results.iteritems():
        output_json += "{\"@type\": \"protocols_statistics\", \"protocol\": \"" + key + "\", \"flows\": " + str(value[0]) + ", \"packets\": " + str(value[1]) + ", \"bytes\": " + str(value[2]) + "}\n"

    # Print results to standard output
    cprint(output_json)

    # Send results to the specified host
    send_data(output_json, output_host)


def count_protocols_statistics(flows_stream):
    """
    Count number of transferred flows, packets, and bytes of TCP, UDP, and other protocols using Spark Streaming functions.

    :param flows_stream: DStream of parsed flows in the JSON format
    :return: union DStream of UDP, TCP, and other protocols statistics
    """

    # Check required flow keys
    flows_stream_checked = flows_stream.filter(lambda flow_json: ("ipfix.protocolIdentifier" in flow_json.keys()))

    # Filter TCP protocol (number 6)
    flows_tcp = flows_stream_checked.filter(lambda flow_json: (flow_json["ipfix.protocolIdentifier"] == 6))
    # Filter UDP protocol (number 17)
    flows_udp = flows_stream_checked.filter(lambda flow_json: (flow_json["ipfix.protocolIdentifier"] == 17))
    # Filter other protocols
    flows_other = flows_stream_checked.filter(lambda flow_json: ((flow_json["ipfix.protocolIdentifier"] != 6) and (flow_json["ipfix.protocolIdentifier"] != 17)))

    # Count TCP statistics (#flows, #packets, #bytes)
    tcp_statistics = flows_tcp.map(lambda flow_json: ("tcp", (1, flow_json["ipfix.packetDeltaCount"], flow_json["ipfix.octetDeltaCount"])))\
                              .reduceByKey(lambda actual, update: (
                                               actual[0] + update[0],
                                               actual[1] + update[1],
                                               actual[2] + update[2]
                                           ))
    # Count UDP statistics (#flows, #packets, #bytes)
    udp_statistics = flows_udp.map(lambda flow_json: ("udp", (1, flow_json["ipfix.packetDeltaCount"], flow_json["ipfix.octetDeltaCount"])))\
                              .reduceByKey(lambda actual, update: (
                                               actual[0] + update[0],
                                               actual[1] + update[1],
                                               actual[2] + update[2]
                                           ))

    # Count other statistics (#flows, #packets, #bytes)
    other_statistics = flows_other.map(lambda flow_json: ("other", (1, flow_json["ipfix.packetDeltaCount"], flow_json["ipfix.octetDeltaCount"])))\
                                  .reduceByKey(lambda actual, update: (
                                                   actual[0] + update[0],
                                                   actual[1] + update[1],
                                                   actual[2] + update[2]
                                               ))

    # Union TCP, UDP, and other statistics and return them as a single DStream
    return tcp_statistics.union(udp_statistics).union(other_statistics)


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)

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

    # Set window and slide duration for flows analysis
    flows_json_windowed = flows_json.window(window_duration, window_slide)

    # Count statistics of the UDP, TCP, and other protocols
    statistics = count_protocols_statistics(flows_json_windowed)

    # Process computed statistics and send them to the specified host
    statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), args.output_host))

    # Start Spark streaming context
    ssc.start()
    ssc.awaitTermination()
