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
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -m <microbatch-duration>
    -w <window-duration>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ~/applications/run-application.sh ./ssh_auth_simple.py -iz producer:2181 -it ipfix.entry -oz producer:9092
    -ot results.output
"""


import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import time  # Unix time to timestamp conversion
from termcolor import cprint  # Colors in the console output

from modules import kafkaIO  # IO operations with kafka topics


# Saves attacks in dictionary, so 1 attack is not reported multiple times
attDict = {}
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
        for key, value in attDict.items():
            # If timestamp of record + 10times window duration is smaller than current time
            if ((10 * window_duration * 1000) + value[1]) < (int(current_time * 1000)):
                del attDict[key]


def get_output_json(key, value, flows_total):
    """
    Create JSON with correct format.

    :param key: key of particular record
    :param value: value of particular record
    :param flows_total: number of total flows detected detected for the attack
    :return: JSON string in desired format
    """
    output_json = ""
    # Convert Unix time to timestamp
    s, ms = divmod(value[3], 1000)
    timestamp = '%s.%03d' % (time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(s)), ms) + 'Z'

    output_json += "{\"@type\": \"ssh_auth_simple\", \"src_ip\": \"" + key[0] + "\", " \
                   "\"dst_ip\": \"" + str(key[1]) + "\", \"flows\": " + str(flows_total) + \
                   ", \"average_packet_count\": " + str(value[1]) + ", \"duration_in_milliseconds\": " + \
                   str(value[2]) + ", \"timestamp\": \"" + str(timestamp) + \
                   "\", \"flows_increment\": " + str(value[0]) + "}\n"
    return output_json


def process_results(results, producer, topic, window_duration):
    """
    Check if attack was reported, or additional flows were detected in same attack and report it.

    :param results: flows that should be reported as attack
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    :param window_duration: window size (in seconds)
    """
    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        if key in attDict:
            # If there are additional flows for the attack that was reported.
            if (attDict[key][1] + window_duration * 1000) <= value[3]:
                attDict[key] = (attDict[key][0] + value[0], value[3])
                output_json += get_output_json(key, value, attDict[key][0])
        else:
            attDict[key] = (value[0], value[3])
            output_json += get_output_json(key, value, value[0])

    # Check if there are any results
    if output_json:
        # Print results to standard output
        cprint(output_json)

        # Check if dictionary cleaning is necessary
        clean_old_data_from_dictionary(window_duration)

        # Send results to the specified kafka topic
        kafkaIO.send_data_to_kafka(output_json, producer, topic)


def get_ip(record, direction):
    """
    Return required IPv4 or IPv6 address (source or destination) from given record.
    :param record: JSON record searched for IP
    :param direction: string from which IP will be searched (e.g. "source" => ipfix.sourceIPv4Address or "destination" => ipfix.destinationIPv4Address)
    :return: value corresponding to the key in the record
    """
    key_name = "ipfix." + direction + "IPv4Address"
    if key_name in record.keys():
        return record[key_name]
    key_name = "ipfix." + direction + "IPv6Address"
    return record[key_name]


def check_for_attacks_ssh(flows_stream, min_packets_amount, max_packets_amount, min_bytes_amount, max_bytes_amount,
                          max_duration_time, flows_threshold, window_duration, window_slide):
    """
    Aggregate flows within given time window and return aggregates that fulfill given requirements.

    :param flows_stream: input flows
    :param min_packets_amount: min amount of packets which passes filter
    :param max_packets_amount: max amount of packets which passes filter
    :param min_bytes_amount: min amount of bytes which passes filter
    :param max_bytes_amount: max amount of bytes which passes filter
    :param max_duration_time: max flow duration which passes filter (in milliseconds)
    :param flows_threshold: min amount of flows which we consider being an attack
    :param window_duration: window size (in seconds)
    :param window_slide: slide interval of the analysis window
    :return: detected attacks
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
    flows_mapped = flows_stream_filtered.map(lambda record: ((get_ip(record, "source"),
                                                              get_ip(record, "destination")),
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
    possible_attacks = flows_ssh_formatted.window(window_duration, window_slide)\
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
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-m", "--microbatch", help="microbatch duration", type=int, required=False, default=5)
    parser.add_argument("-w", "--window", help="analysis window duration", type=int, required=False, default=300)

    # Define Arguments for detection
    parser.add_argument("-minp", "--min_packets", help="min amount of packets which passes filter",
                        type=int, required=False, default=10)
    parser.add_argument("-maxp", "--max_packets", help="max amount of packets which passes filter",
                        type=int, required=False, default=20)
    parser.add_argument("-minb", "--min_bytes", help="min amount of bytes which passes filter",
                        type=int, required=False, default=1800)
    parser.add_argument("-maxb", "--max_bytes", help="max amount of bytes which passes filter",
                        type=int, required=False, default=5000)
    parser.add_argument("-maxd", "--max_duration", help="max flow duration which passes filter (in milliseconds)",
                        type=int, required=False, default=12000)
    parser.add_argument("-t", "--threshold", help="min amount of flows which we consider being an attack",
                        type=int, required=False, default=10)

    # Parse arguments
    args = parser.parse_args()

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Check for SSH attacks
    attacks = check_for_attacks_ssh(parsed_input_stream, args.min_packets, args.max_packets, args.min_bytes, args.max_bytes,
                                    args.max_duration, args.threshold, args.window, args.microbatch)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(attacks, kafka_producer, args.output_topic, args.window, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
