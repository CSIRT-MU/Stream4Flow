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
Detects horizontal and vertical port scans on the network using adjustable threshold for number of flows

Default values are:
    * window size: 60
    * min amount of flows: 20

Usage:
    portscan.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -m <microbatch-duration>
    -w <window-duration> -t <threshold>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ~/applications/run-application.sh ./ports_scan.py -iz producer:2181 -it ipfix.entry -oz producer:9092
    -ot results.output
"""


import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser

import time  # Unix time to timestamp conversion

from termcolor import cprint  # Colors in the console output
from modules import kafkaIO   # Module for kafka IO operations


# Saves scans in dictionary, so 1 scan is not reported multiple times
scanDict = {}

# Remembers when dictionary was cleaned last time
lastCleaning = time.time()


def clean_old_data_from_dictionary(window_duration):
    """
    Clean dictionary of old attacks.

    :param window_duration: time for which if record is older it gets deleted (multiplied by 10)
    """
    global lastCleaning
    current_time = time.time()
    # Clean once a day
    if (lastCleaning + 86400) < current_time:
        lastCleaning = current_time
        for key, value in scanDict.items():
            # If timestamp of record + 10times window duration is smaller than current time
            if ((10 * window_duration * 1000) + value[1]) < (int(current_time * 1000)):
                del scanDict[key]


def get_output_json(key, value, flows_total, targets_total, total_duration):
    """
    Create JSON with correct format

    :param key: key of particular record
    :param value: value of particular record
    :param flows_total: number of total flows detected for the same scan
    :param targets_total: number of total targets scanned
    :param total_duration: total scan duration in milliseconds
    :return: JSON string in desired format
    """
    output_json = ""
    # Convert Unix time to timestamp
    s, ms = divmod(value[3], 1000)
    timestamp = '%s.%03d' % (time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(s)), ms) + 'Z'

    if key[0] == 'horizontal':
        output_json += "{\"@type\": \"portscan_" + key[0] + "\", \"src_ip\": \"" + key[1] + "\", \"dst_port\": \"" \
                       + str(key[2]) + "\", \"flows\": " + str(flows_total) + ", " "\"duration_in_milliseconds\": " \
                       + str(total_duration) + ", \"timestamp\": \"" + str(timestamp) + "\", \"flows_increment\": " \
                       + str(value[0]) + ", \"targets_total\": " + str(targets_total) + "}\n"
    if key[0] == 'vertical':
        output_json += "{\"@type\": \"portscan_" + key[0] + "\", \"src_ip\": \"" + key[1] + "\", \"dst_ip\": \"" \
                       + str(key[2]) + "\", \"flows\": " + str(flows_total) + ", " "\"duration_in_milliseconds\": " \
                       + str(total_duration) + ", \"timestamp\": \"" + str(timestamp) + "\", \"flows_increment\": " \
                       + str(value[0]) + ", \"targets_total\": " + str(targets_total) + "}\n"
    return output_json


def process_results(results, producer, output_topic, window_duration):
    """
    Check if attack was reported, or additional flows were detected in same attack and report it

    :param results: flows that should be reported as attack
    :param producer: producer that sends the data
    :param output_topic: name of the receiving kafka topic
    :param window_duration: window size in seconds
    """
    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        if key in scanDict:
            if (scanDict[key][1] + (window_duration*1000)) <= value[3]:
                scanDict[key] = (scanDict[key][0] + value[0],
                                 value[3],
                                 scanDict[key][2] + value[4],
                                 scanDict[key][3] + value[2])
                output_json += get_output_json(key, value, scanDict[key][0], scanDict[key][2], scanDict[key][3])
        else:
            scanDict[key] = (value[0], value[3], value[4], value[2])
            output_json += get_output_json(key, value, value[0], value[4], value[2])

    # Check if there are any results
    if output_json:
        # Print results to standard output
        cprint(output_json)

        # Check if dictionary cleaning is necessary
        clean_old_data_from_dictionary(window_duration)

        # Send desired output to the output_topic
        kafkaIO.send_data_to_kafka(output_json, producer, output_topic)


def get_ip(record, direction):
    """
    Return required IPv4 or IPv6 address (source or destination) from given record.
    :param record: JSON record searched for IP
    :param direction: string from which IP will be searched (e.g. "source" => ipfix.sourceIPv4Address or
                      "destination" => ipfix.destinationIPv4Address)
    :return: value corresponding to the key in the record
    """
    key_name = "ipfix." + direction + "IPv4Address"
    if key_name in record.keys():
        return record[key_name]
    key_name = "ipfix." + direction + "IPv6Address"
    return record[key_name]


def process_input(flows_stream, targets_threshold, window_duration, window_slide):
    """
    Process raw data and do MapReduce operations.

    :param flows_stream: input data in JSON format to process
    :param targets_threshold: min amount of flows which we consider being an attack
    :param window_duration: window size (in seconds)
    :param window_slide: slide interval of the analysis window
    :return: detected ports scans
    """
    # Check required flow keys
    flows_stream_with_keys = flows_stream.filter(lambda flow_json: ("ipfix.tcpControlBits" in flow_json.keys())
                                                 and ("ipfix.protocolIdentifier" in flow_json.keys())
                                                 and (flow_json["ipfix.protocolIdentifier"] == 6)
                                                 and ("ipfix.destinationTransportPort" in flow_json.keys()))

    # Filter flows with SYN flag (AND with 31 to select last 5 bits)
    flows_stream_checked = flows_stream_with_keys\
        .filter(lambda flow_json: (flow_json["ipfix.tcpControlBits"] & 31) == 2)

    # Remap to get unique horizontal and vertical portscans, first value being flow count and last being target count
    horizontal_scans = flows_stream_checked.map(lambda record: (("horizontal",
                                                                 get_ip(record, "source"),
                                                                 record["ipfix.destinationTransportPort"]),
                                                                (1,
                                                                 get_ip(record, "destination"),
                                                                 record["ipfix.flowEndMilliseconds"] -
                                                                 record["ipfix.flowStartMilliseconds"],
                                                                 record["ipfix.flowStartMilliseconds"],
                                                                 1)))

    vertical_scans = flows_stream_checked.map(lambda record: (("vertical",
                                                               get_ip(record, "source"),
                                                               get_ip(record, "destination")),
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
        .filter(lambda record: record[1][4] >= targets_threshold)

    portscans_windowed = possible_portscans.window(window_duration, window_slide)\
        .reduceByKey(lambda actual, update:
                     (actual[0] + update[0],
                      str(actual[1]) + "," + str(update[1]) if str(update[1]) not in str(actual[1]) else str(actual[1]),
                      actual[2] + update[2] if str(update[1]) not in str(actual[1]) else actual[2],
                      actual[3],
                      actual[4] + update[4] if str(update[1]) not in str(actual[1]) else actual[4]))\
        .filter(lambda record: record[1][4] >= targets_threshold)

    return portscans_windowed


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-m", "--microbatch", help="microbatch duration", type=int, required=False, default=5)
    parser.add_argument("-w", "--window", help="analysis window duration", type=int, required=False, default=60)

    # arguments for detection
    parser.add_argument("-t", "--threshold", help="min amount of targets which trigger detection", type=int,
                        required=False, default=20)

    # Parse arguments
    args = parser.parse_args()

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Check for port scans
    processed_input = process_input(parsed_input_stream, args.threshold, args.window, args.microbatch)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, args.window,
                                         process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
