# -*- coding: utf-8 -*-

#
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

"""
Detects external dns resolvers used in the specified local network.

Default output parameters:
    * Address and port of the broker: producer:9092
    * Kafka topic: results.output

Usage:
    dns_external_resolvers.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -lc <local-network>/<subnet-mask>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./detection/dns_external_resolvers/spark/dns_external_resolvers.py -iz producer:2181\
    -it ipfix.entry -oz producer:9092 -ot results.output -lc 10.10.0.0/16
"""

import argparse  # Arguments parser
from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network

from modules import kafkaIO  # IO operations with kafka topics


def get_output_json(key, value):
    """
    Create JSON with correct format.

    :param key: Source ip address
    :param value: Dictionary value for statistic
    :return: JSON string in desired format
    """

    return "{\"@type\": \"external_dns_resolver\", \"src_ip\": \"" + str(key[0]) + "\"" +\
           ", \"resolved_address\": \"" + str(key[1]) + "\"" +\
           ", \"resolver_ip\": \"" + str(value[0]) + "\"" +\
           ", \"count\": \"" + str(value[2]) + \
           ", \"timestamp\": \"" + str(value[1]) + "}\n"


def process_results(results, producer, s_output_topic):
    """
    Format and report computed statistics.

    :param results: Computed statistics
    :param producer: Producer that sends the data
    :param s_output_topic: Name of the receiving kafka topic
    """

    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        output_json += get_output_json(key, value)

    if output_json:
        # Print data to standard output
        print(output_json)

        # Send results to the specified kafka topic
        # kafkaIO.send_data_to_kafka(output_json, producer, s_output_topic)


def get_external_dns_resolvers(dns_input_stream, all_data_stream, s_window_duration, top_n):
    """
    Gets used external dns resolvers from input stream

    :param dns_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :param all_data_stream: All incoming flows
    :param top_n: Top values for each ip address which are considered
    :return: Detected external resolvers
    """

    dns_resolved = dns_input_stream \
        .filter(lambda record: record["ipfix.DNSQType"] == 1)\
        .filter(lambda record: record["ipfix.sourceTransportPort"] == 53)\
        .filter(lambda record: record["ipfix.DNSCrrType"] == 1)\
        .map(lambda record: ((record["ipfix.destinationIPv4Address"], IPAddress(int(record["ipfix.DNSRData"][:10], 16))),
                             (record["ipfix.sourceIPv4Address"], record["ipfix.flowStartMilliseconds"], 1)))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual[0],
                                             actual[1],
                                             actual[2] + update[2]))

    detected_external = all_data_stream \
        .filter(lambda flow_json: flow_json["ipfix.protocolIdentifier"] == 6) \
        .map(lambda record: ((record["ipfix.sourceIPv4Address"], IPAddress(record["ipfix.destinationIPv4Address"])),
                             record["ipfix.flowStartMilliseconds"])) \
        .join(dns_resolved)\
        .filter(lambda record: ((record[1][0] - record[1][1][1]) <= 2000) and ((record[1][0] - record[1][1][1]) >= -2000)) \
        .map(lambda record: ((record[0][0], record[0][1]), (record[1][1][0], record[1][1][1], record[1][1][2])))

    return detected_external


def get_dns_stream(flows_stream):
    """
    Filter to get only flows containing DNS information.

    :param flows_stream: Input flows
    :return: Flows with DNS information
    """
    return flows_stream \
        .filter(lambda flow_json: ("ipfix.DNSName" in flow_json.keys()) and
                                  ("ipfix.sourceIPv4Address" in flow_json.keys()))


def get_flows_external_to_local(s_dns_stream, local_network):
    """
    Filter to contain flows going from the specified local network to the different network.

    :param s_dns_stream: Input flows
    :param local_network: Local network's address
    :return: Flows coming from local network to external networks
    """
    return s_dns_stream \
        .filter(lambda dns_json: (IPAddress(dns_json["ipfix.sourceIPv4Address"]) not in IPNetwork(local_network)) and
                                 (IPAddress(dns_json["ipfix.destinationIPv4Address"]) in IPNetwork(local_network)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-w", "--window_size", help="window size (in seconds)", type=int, required=False, default=20)
    parser.add_argument("-m", "--microbatch", help="microbatch (in seconds)", type=int, required=False, default=10)

    # Define Arguments for detection
    parser.add_argument("-lc", "--local_network", help="local network", type=str, required=True)

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    window_duration = args.window_size  # Analysis window duration (20 seconds default)
    microbatch = args.microbatch
    output_topic = args.output_topic

    # Initialize input stream and parse it into JSON
    sc, ssc, parsed_input_stream = kafkaIO \
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, microbatch)

    # Prepare input stream
    dns_stream = get_dns_stream(parsed_input_stream)
    dns_external_to_local = get_flows_external_to_local(dns_stream, args.local_network)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Calculate and process DNS statistics
    get_external_dns_resolvers(dns_external_to_local, parsed_input_stream, window_duration, 10) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, output_topic))

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
