# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016  Michal Stefanik <stefanik.m@mail.muni.cz>, Milan Cermak <cermak@ics.muni.cz>
# Institute of Computer Science, Masaryk University
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
Description: A method for detection of DoS/DDoS attacks based on an evaluation of
the incoming/outgoing packet volume ratio and its variance to the long-time (long window) ratio.

Usage:
  detection_ddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -nf <regex for network range>

  To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
  you can run the example
    $ /home/spark/applications/run-application.sh /home/spark/applications/detection/ddos/detection_ddos.py
     -iz producer:2181 -it ipfix.entry -oh consumer:20101 -nf "10\.10\..+"
"""

import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface
import re  # Regular expression match

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


def print_and_send(rdd, output_host):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
        {"@type": "detection.ddos", "host" : <destination_ip> "shortratio" : <short-term ratio>,
        "longratio": <long-term ration>, "attackers": [set of attackers]}

    :param rdd: rdd to be parsed and sent
    """
    results = ""
    rdd_map = rdd.collectAsMap()

    # generate JSON response for each aggregated rdd
    for host, stats in rdd_map.iteritems():
        short_ratio = float(stats[0][0]) / stats[0][1]
        long_ratio = float(stats[1][0]) / stats[1][1]
        attackers = list(stats[0][2])

        new_entry = {"@type": "detection.ddos",
                     "dst_ipv4": host,
                     "shortratio": short_ratio,
                     "longratio": long_ratio,
                     "attackers": attackers}

        results += ("%s\n" % json.dumps(new_entry))

    # Print results to stdout
    cprint(results)

    # Send results to the given socket.
    send_data(results, output_host)


def inspect_ddos(stream_data):
    """
    Main method performing the flows aggregation in short and long window and comparison of their ratios

    :type stream_data: Initialized spark streaming context.
    """
    # Create regex for monitored network
    local_ip_pattern = re.compile(network_filter)

    # Filter only the data with known source and destination IP
    filtered_stream_data = stream_data \
        .map(lambda x: json.loads(x[1])) \
        .filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys() and
                                  "ipfix.destinationIPv4Address" in json_rdd.keys()
                                  ))

    # Create stream of base windows
    small_window = filtered_stream_data.window(base_window_length, base_window_length)

    # Count number of incoming packets from each source ip address for each destination ip address
    # from a given network range
    incoming_small_flows_stats = small_window \
        .filter(lambda json_rdd: re.match(local_ip_pattern, json_rdd["ipfix.destinationIPv4Address"])) \
        .map(lambda json_rdd: (json_rdd["ipfix.destinationIPv4Address"],
                               (json_rdd["ipfix.packetDeltaCount"], 0, {json_rdd["ipfix.sourceIPv4Address"]})))

    # Count number of outgoing packets for each source ip address from a given network range
    outgoing_small_flows_stats = small_window \
        .filter(lambda json_rdd: re.match(local_ip_pattern, json_rdd["ipfix.sourceIPv4Address"])) \
        .map(lambda json_rdd: (json_rdd["ipfix.sourceIPv4Address"],
                               (0, json_rdd["ipfix.packetDeltaCount"], set()))) \

    # Merge DStreams of incoming and outgoing number of packets
    small_window_aggregated = incoming_small_flows_stats.union(outgoing_small_flows_stats)\
        .reduceByKey(lambda actual, update: (actual[0] + update[0],
                                             actual[1] + update[1],
                                             actual[2].union(update[2])))

    # Create long window for long term profile
    union_long_flows = small_window_aggregated.window(long_window_length, base_window_length)
    long_window_aggregated = union_long_flows.reduceByKey(lambda actual, update: (actual[0] + update[0],
                                                          actual[1] + update[1])
                                                          )
    # Union DStreams with small and long window
    # RDD in DStream in format (local_device_IPv4, (
    # (short_inc_packets, short_out_packets, short_source_IPv4s),
    # (long_inc_packets, long_out_packets)))
    windows_union = small_window_aggregated.join(long_window_aggregated)

    # Filter out zero values to prevent division by zero
    nonzero_union = windows_union.filter(lambda rdd: rdd[1][0][1] != 0 and rdd[1][1][1] != 0)

    # Compare incoming and outgoing transfers volumes and filter only those suspicious
    # -> overreaching the minimal_incoming volume of packets and
    # -> short-term ratio is greater than long-term ratio * threshold
    windows_union_filtered = nonzero_union.filter(lambda rdd: rdd[1][0][0] > minimal_incoming and
                                                  float(rdd[1][0][0]) / rdd[1][0][1] > float(rdd[1][1][0]) /
                                                  rdd[1][1][1] * threshold
                                                  )

    # Return the detected records
    return windows_union_filtered


if __name__ == "__main__":
    # Prepare arguments parser (automatically creates -h argument).
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-nf", "--network_filter", help="regular expression filtering the watched IPs", type=str, required=True)

    # Parse arguments.
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic

    # Set method parameters:
    threshold = 50  # Minimal increase of receive/sent packets ratio
    minimal_incoming = 100000  # Minimal count of incoming packets
    long_window_length = 7200  # Window length for average ratio computation (must be a multiple of microbatch interval)
    base_window_length = 30  # Window length for basic computation (must be a multiple of microbatch interval)

    network_filter = args.network_filter  # Filter for network for detection (regex filtering), e.g. "10\.10\..+"

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Run the detection of ddos
    ddos_result = inspect_ddos(input_stream)

    # Process the results of the detection and send them to the specified host
    ddos_result.foreachRDD(lambda rdd: print_and_send(rdd, args.output_host))

    # Start input data processing
    ssc.start()
    ssc.awaitTermination()
