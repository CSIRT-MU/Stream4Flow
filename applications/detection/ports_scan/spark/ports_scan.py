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
Detects horizontal and vertical TCP ports scans on the network using adjustable threshold for number of flows

Default values are:
    * window size: 60
    * min amount of flows: 20

Usage:
    ports_scan.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oh <output-hostname>:<output-port>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./detection/ports_scan/spark/ports_scan.py -iz producer:2181\
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


# Saves scans in dictionary, so 1 scan is not reported multiple times
scanDict = {}

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
        for key, value in scanDict.items():
            # If timestamp of record + 10times window duration is smaller than current time
            if ((10 * s_window_duration * 1000) + value[1]) < (int(current_time * 1000)):
                del scanDict[key]


def get_output_json(key, value, flows_increment):
    """
    Create JSON with correct format

    :param key: key of particular record
    :param value: value of particular record
    :param flows_increment: number of additional flows detected in comparison to previous detection of same attack
    :return: JSON string in desired format
    """

    output_json = ""
    # Convert Unix time to timestamp
    s, ms = divmod(value[3], 1000)
    timestamp = '%s.%03d' % (time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(s)), ms) + 'Z'

    if key[0] == 'horizontal':
        output_json += "{\"@type\": \"ports_scan-" + key[0] + "\", \"src_ip\": \"" + key[1] + "\", \"dst_port\": \"" \
                       + str(key[2]) + "\", \"dst_ips\": \"" + str(value[1]) + "\", \"flows\": " \
                       + str(value[0]) + ", " "\"duration_in_milliseconds\": " + str(value[2]) + \
                       ", \"timestamp\": \"" + str(timestamp) + \
                       "\", \"flows_increment\": " + str(flows_increment) + \
                       ", \"targets_count\": " + str(value[4]) + "}\n"
    if key[0] == 'vertical':
        output_json += "{\"@type\": \"ports_scan-" + key[0] + "\", \"src_ip\": \"" + key[1] + "\", \"dst_ip\": \"" \
                       + str(key[2]) + "\", \"dst_ports\": \"" + str(value[1]) + "\", \"flows\": " \
                       + str(value[0]) + ", " "\"duration_in_milliseconds\": " + str(value[2]) + \
                       ", \"timestamp\": \"" + str(timestamp) + \
                       "\", \"flows_increment\": " + str(flows_increment) + \
                       ", \"targets_count\": " + str(value[4]) + "}\n"
    return output_json


def process_results(results, output_host, s_window_duration):
    """
    Check if attack was reported, or additional flows were detected in same attack and report it

    :param results: flows that should be reported as attack
    :param output_host: where to send processed data
    :param s_window_duration: window size in seconds
    """

    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        if key in scanDict:
            if scanDict[key][0] < value[0]:
                flows_increment = value[0] - scanDict[key][0]
                scanDict[key] = (value[0], value[3])
                output_json = get_output_json(key, value, flows_increment)
        else:
            scanDict[key] = (value[0], value[3])
            output_json = get_output_json(key, value, value[0])

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
    Find ipv4 type of key if present, ipv6 otherwise

    :param record: JSON record searched for key
    :param wanted_key: string from which key will be made and searched (e.g. "source" => ipfix.sourceIPv4Address)
    :return: value corresponding to the key in the record
    """

    key_name = "ipfix." + wanted_key + "IPv4Address"
    if key_name in record.keys():
        return record[key_name]
    key_name = "ipfix." + wanted_key + "IPv6Address"
    return record[key_name]


def check_for_attacks_ssh(flows_stream, flows_threshold, s_window_duration, s_window_slide):

    # Check required flow keys
    flows_stream_with_keys = flows_stream.filter(lambda flow_json: ("ipfix.tcpControlBits" in flow_json.keys())
                                                 and ("ipfix.protocolIdentifier" in flow_json.keys())
                                                 and (flow_json["ipfix.protocolIdentifier"] == 6)
                                                 and ("ipfix.destinationTransportPort" in flow_json.keys()))

    # Filter flows with SYN flag
    flows_stream_checked = flows_stream_with_keys\
        .filter(lambda flow_json: ('{0:09b}'.format(flow_json["ipfix.tcpControlBits"]))[5] == '1')

    # Remap to get unique horizontal and vertical portscans, first value being flow count and last being target count
    horizontal_scans = flows_stream_checked.map(lambda record: (("horizontal",
                                                                 get_key_with_ip_version(record, "source"),
                                                                 record["ipfix.destinationTransportPort"]),
                                                                (1,
                                                                 get_key_with_ip_version(record, "destination"),
                                                                 record["ipfix.flowEndMilliseconds"] -
                                                                 record["ipfix.flowStartMilliseconds"],
                                                                 record["ipfix.flowStartMilliseconds"],
                                                                 1)))

    vertical_scans = flows_stream_checked.map(lambda record: (("vertical",
                                                               get_key_with_ip_version(record, "source"),
                                                               get_key_with_ip_version(record, "destination")),
                                                              (1,
                                                               record["ipfix.destinationTransportPort"],
                                                               record["ipfix.flowEndMilliseconds"] -
                                                               record["ipfix.flowStartMilliseconds"],
                                                               record["ipfix.flowStartMilliseconds"],
                                                               1)))

    flows_all = horizontal_scans.union(vertical_scans)

    possible_portscans = flows_all\
        .reduceByKey(lambda actual, update:
                     (actual[0] + update[0],
                      str(actual[1]) + "," + str(update[1]) if str(update[1]) not in str(actual[1]) else str(actual[1]),
                      actual[2] + update[2] if str(update[1]) not in str(actual[1]) else actual[2],
                      actual[3],
                      actual[4] + update[4] if str(update[1]) not in str(actual[1]) else actual[4]))\
        .filter(lambda record: record[1][0] >= flows_threshold)

    portscans_windowed = possible_portscans.window(s_window_duration, s_window_slide)\
        .reduceByKey(lambda actual, update:
                     (actual[0] + update[0],
                      str(actual[1]) + "," + str(update[1]) if str(update[1]) not in str(actual[1]) else str(actual[1]),
                      actual[2] + update[2] if str(update[1]) not in str(actual[1]) else actual[2],
                      actual[3],
                      actual[4] + update[4] if str(update[1]) not in str(actual[1]) else actual[4]))\
        .filter(lambda record: record[1][0] >= flows_threshold)

    return portscans_windowed

if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-w", "--window_size", help="window size (in seconds)", type=int, required=False, default=60)

    # arguments for detection
    parser.add_argument("-ft", "--flows_threshold", help="min amount of flows which trigger detection", type=int,
                        required=False, default=20)

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = args.window_size  # Analysis window duration (60 seconds default)
    window_slide = 1  # Slide interval of the analysis window (1 second)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    flows_json = input_stream.map(lambda line: json.loads(line[1]))

    # Check for portscans
    attacks = check_for_attacks_ssh(flows_json, args.flows_threshold, window_duration, window_slide)

    # Process computed statistics and send them to the standard output
    attacks.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), args.output_host, window_duration))

    ssc.start()
    ssc.awaitTermination()