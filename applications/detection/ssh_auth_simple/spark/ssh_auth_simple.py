# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2017 Tomas Pavuk <433592@mail.muni.cz>, Institute of Computer Science, Masaryk University
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
Detects brute-force attacks on the SSH authentication using basic thresholds for number of flows, packets, bytes, and
duration within given time interval.

Default values are:
    * min amount of packets: 10
    * max amount of packets: 20
    * min amount of bytes: 1800
    * max amount of bytes: 5000
    * max flow duration: 12000
    * min amount of flows: 10
    * windows size: 300

Usage:
    ssh_auth_simple.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oh <output-hostname>:<output-port>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./detection/ssh_auth_simple/spark/ssh_auth_simple.py -iz producer:2181\
    -it ipfix.entry -oh consumer:20101
"""


import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface
import time  # Unix time to timestamp conversion

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


# Saves attacks in dictionary, so 1 attack is not reported multiple times
attDict = {}
# Remembers when dictionary was cleaned last time
lastCleaning = time.time()


def clean_old_data_from_dictionary(s_window_duration):
    """
    Clean dictionary of old attacks.

    :param s_window_duration: time for which if record is older it gets deleted (multiplied by 10)
    """
    global lastCleaning
    current_time = time.time()
    # Clean once a day
    if (lastCleaning + 86400) < current_time:
        lastCleaning = current_time
        for key, value in attDict.items():
            # If timestamp of record + 10times window duration is smaller than current time
            if ((10 * s_window_duration * 1000) + value[1]) < (int(current_time * 1000)):
                del attDict[key]


def get_output_json(key, value, flows_increment):
    """
    Create JSON with correct format.

    :param key: key of particular record
    :param value: value of particular record
    :param flows_increment: number of additional flows detected in comparison to previous detection of same attack
    :return: JSON string in desired format
    """

    output_json = ""
    # Convert Unix time to timestamp
    s, ms = divmod(value[3], 1000)
    timestamp = '%s.%03d' % (time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(s)), ms) + 'Z'

    output_json += "{\"@type\": \"ssh_auth_simple\", \"src_ip\": \"" + key[0] + "\", " \
                   "\"dst_ip\": \"" + str(key[1]) + "\", \"flows\": " + str(value[0]) + \
                   ", \"average_packet_count\": " + str(value[1]) + ", \"duration_in_milliseconds\": " + \
                   str(value[2]) + ", \"timestamp\": \"" + str(timestamp) + \
                   "\", \"flows_increment\": " + str(flows_increment) + "}\n"
    return output_json


def process_results(results, output_host, s_window_duration):
    """
    Check if attack was reported, or additional flows were detected in same attack and report it.

    :param results: flows that should be reported as attack
    :param output_host: where to send processed data
    :param s_window_duration: window size (in seconds)
    """

    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        if key in attDict:
            # If there are additional flows for the attack that was reported.
            if attDict[key][0] < value[0]:
                # Calculate incremental flows detected for same attack
                flows_increment = value[0] - attDict[key][0]
                attDict[key] = (value[0], value[3])
                output_json += get_output_json(key, value, flows_increment)
        else:
            attDict[key] = (value[0], value[3])
            output_json += get_output_json(key, value, value[0])

    # Check if there are any results
    if output_json:
        # Print results to standard output
        cprint(output_json)

        # Check if dictionary cleaning is necessary
        clean_old_data_from_dictionary(s_window_duration)

        # Send results to the specified host
        send_data(output_json, output_host)


def get_key_with_ip_version(record, wanted_key):
    """
    Find ipv4 type of key if present, ipv6 otherwise.

    :param record: JSON record searched for key
    :param wanted_key: string from which key will be made and searched (e.g. "source" => ipfix.sourceIPv4Address)
    :return: value corresponding to the key in the record
    """

    key_name = "ipfix." + wanted_key + "IPv4Address"
    if key_name in record.keys():
        return record[key_name]
    key_name = "ipfix." + wanted_key + "IPv6Address"
    return record[key_name]


def check_for_attacks_ssh(flows_stream, min_packets_amount, max_packets_amount, min_bytes_amount, max_bytes_amount,
                          max_duration_time, flows_threshold, s_window_duration, s_window_slide):
    """
    Aggregate flows within given time window and return aggregates that fulfill given requirements.

    :param flows_stream:
    :param min_packets_amount: min amount of packets which passes filter
    :param max_packets_amount: max amount of packets which passes filter
    :param min_bytes_amount: min amount of bytes which passes filter
    :param max_bytes_amount: max amount of bytes which passes filter
    :param max_duration_time: max flow duration which passes filter (in milliseconds)
    :param flows_threshold: min amount of flows which we consider being an attack
    :param s_window_duration: window size (in seconds)
    :param s_window_slide: slide interval of the analysis window
    :return:
    """

    # Check required flow keys
    flows_stream_filtered = flows_stream\
        .filter(lambda flow_json: ("ipfix.protocolIdentifier" in flow_json.keys())
                and ("ipfix.destinationTransportPort" in flow_json.keys())
                and (flow_json["ipfix.protocolIdentifier"] == 6)
                and (flow_json["ipfix.destinationTransportPort"] == 22)
                and (flow_json["ipfix.sourceTransportPort"] > 1024)
                and (min_bytes_amount < flow_json["ipfix.octetDeltaCount"] < max_bytes_amount)
                and (min_packets_amount < flow_json["ipfix.packetDeltaCount"] < max_packets_amount)
                and ((flow_json["ipfix.flowEndMilliseconds"] -
                      flow_json["ipfix.flowStartMilliseconds"]) < max_duration_time))

    # Map and reduce to get format ((SRC_IP, DST_IP), (NUM_OF_FLOWS, AVERAGE_PACKET_COUNT, DURATION, TIMESTAMP))
    flows_mapped = flows_stream_filtered.map(lambda record: ((get_key_with_ip_version(record, "source"),
                                                              get_key_with_ip_version(record, "destination")),
                                                             (1,
                                                              record["ipfix.packetDeltaCount"],
                                                              record["ipfix.flowEndMilliseconds"] -
                                                              record["ipfix.flowStartMilliseconds"],
                                                              record["ipfix.flowStartMilliseconds"])))

    flows_ssh_formatted = flows_mapped \
        .reduceByKey(lambda actual, update: (actual[0] + update[0],
                                             ((update[0] - 1) * actual[1] + update[1]) / update[0],
                                             actual[2] + update[2],
                                             actual[3]))

    # Reduce in window
    possible_attacks = flows_ssh_formatted.window(s_window_duration, s_window_slide)\
        .reduceByKey(lambda actual, update: (actual[0] + update[0],
                                             ((update[0] - 1) * actual[1] + update[1]) / update[0],
                                             actual[2] + update[2],
                                             actual[3]))

    # Filter only flows which satisfy the threshold
    attacks_ssh = possible_attacks.filter(lambda record: record[1][0] >= flows_threshold)

    # Return detected flows
    return attacks_ssh


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-w", "--window_size", help="window size (in seconds)", type=int, required=False, default=300)

    # Define Arguments for detection
    parser.add_argument("-minp", "--min_packets", help="min amount of packets which passes filter",
                        type=int, required=False, default=10)
    parser.add_argument("-maxp", "--max_packets", help="max amount of packets which passes filter",
                        type=int, required=False, default=20)
    parser.add_argument("-minb", "--min_bytes", help="min amount of bytes which passes filter",
                        type=int, required=False, default=1800)
    parser.add_argument("-maxb", "--max_bytes", help="max amount of bytes which passes filter",
                        type=int, required=False, default=5000)
    parser.add_argument("-d", "--max_duration", help="max flow duration which passes filter (in milliseconds)",
                        type=int, required=False, default=12000)
    parser.add_argument("-ft", "--flows_threshold", help="min amount of flows which we consider being an attack",
                        type=int, required=False, default=10)

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = args.window_size  # Analysis window duration (300 seconds/5 minutes default)
    window_slide = 5  # Slide interval of the analysis window (5 second)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    flows_json = input_stream.map(lambda line: json.loads(line[1]))

    # Check for SSH attacks
    attacks = check_for_attacks_ssh(flows_json, args.min_packets, args.max_packets, args.min_bytes, args.max_bytes,
                                    args.max_duration, args.flows_threshold, window_duration, window_slide)

    # Process computed statistics and send them to the standard output
    attacks.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), args.output_host, window_duration))

    # Start Spark streaming context
    ssc.start()
    ssc.awaitTermination()
