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

Usage:
    dns_open_resolvers.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -ln <local-network>/<subnet-mask>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./detection/dns_open_resolvers/spark/dns_open_resolvers.py -iz producer:2181\
    -it ipfix.entry -oz producer:9092 -ot results.output -ln 10.10.0.0/16
"""

import argparse  # Arguments parser
import time  # Unix time to timestamp conversion
import re  # Regular expressions for string matching
import os.path  # Checking whether file exists

from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network
from modules import kafkaIO  # IO operations with kafka topics
from modules import DNSResponseConverter  # Convert byte array to the IP address
from termcolor import cprint  # Colors in the console output


def get_output_json(key, value):
    """
    Create JSON with correct format.

    :param key: Source ip address
    :param value: Dictionary value for statistic
    :return: JSON string in desired format
    """
    # Convert Unix time to timestamp
    s, ms = divmod(value[0], 1000)
    timestamp = '%s.%03d' % (time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(s)), ms) + 'Z'

    return "{\"@type\": \"open_dns_resolver\"" +\
           ", \"resolver_ip\": \"" + str(key[0]) + "\"" +\
           ", \"flows\": " + str(value[1]) + \
           ", \"resolved_data\": \"" + str(key[1]) + "\"" +\
           ", \"resolved_query\": \"" + str(key[2]) + "\"" +\
           ", \"timestamp\": \"" + str(timestamp) + "\"}\n"


def process_results(results, producer, output_topic):
    """
    Format and report detected records.

    :param results: Detected records
    :param producer: Kafka producer that sends the data to output topic
    :param output_topic: Name of the receiving kafka topic
    """
    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        output_json += get_output_json(key, value)

    if output_json:
        # Print data to standard output
        cprint(output_json)

        # Send results to the specified kafka topic
        kafkaIO.send_data_to_kafka(output_json, producer, output_topic)


def filter_ip_for_networks(ip_to_filter, whitelist_networks):
    """
    Filters ip for networks

    :param ip_to_filter: IPv4 which is filtered
    :param whitelist_networks: Array of networks for which IP is checked
    :return: True if ip belongs to any of the whitelisted networks, false otherwise
    """
    for network in whitelist_networks:
        if ip_to_filter in network:
            return True
    return False


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


def get_open_dns_resolvers(input_stream, whitelist_domains, whitelist_networks):
    """
    Gets used open dns resolvers from input stream

    :param input_stream: Input flows
    :param whitelist_domains: Regex containing all whitelisted domains
    :param whitelist_networks: Array with all whitelisted networks
    :return: Detected open resolvers
    """

    # Filter non-empty, no-error responses with return types for A, NS, CNAME, AAAA
    filtered_records = input_stream\
        .filter(lambda flow_json: flow_json["ipfix.DNSCrrType"] == 1
                                  or flow_json["ipfix.DNSCrrType"] == 2
                                  or flow_json["ipfix.DNSCrrType"] == 5
                                  or flow_json["ipfix.DNSCrrType"] == 28) \
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] >> 15) & 1) \
        .filter(lambda flow_json: flow_json["ipfix.DNSRDataLength"] > 0) \
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] & 15) == 0)

    # Convert to resolved data (ip or domain)
    detected_open_resolvers = filtered_records\
        .filter(lambda flow_json:
                          not filter_ip_for_networks(
                              IPAddress(DNSResponseConverter.convert_dns_rdata(flow_json["ipfix.DNSRData"],
                                                                               flow_json["ipfix.DNSCrrType"])),
                              whitelist_networks)
                          if (flow_json["ipfix.DNSCrrType"] == 1 or flow_json["ipfix.DNSCrrType"] == 28)
                          else not re.match(whitelist_domains,
                                       DNSResponseConverter.convert_dns_rdata(flow_json["ipfix.DNSRData"],
                                                                              flow_json["ipfix.DNSCrrType"])))
    # Map detected records
    mapped_open_resolvers = detected_open_resolvers \
        .map(lambda record: ((get_ip(record, "source"),
                             DNSResponseConverter.convert_dns_rdata(record["ipfix.DNSRData"], record["ipfix.DNSCrrType"]),
                              record["ipfix.DNSCrrName"]),
                             (record["ipfix.flowStartMilliseconds"], 1)
                             ))\
        .reduceByKey(lambda actual, update: (actual[0], actual[1] + update[1]))

    return mapped_open_resolvers


def get_dns_stream(flows_stream):
    """
    Filter to get only flows containing DNS information.

    :param flows_stream: Input flows
    :return: Flows with DNS information
    """
    return flows_stream \
        .filter(lambda flow_json: "ipfix.DNSName" in flow_json.keys())


def get_flows_local_to_external(flows_stream, local_network):
    """
    Filter to contain flows going from the specified local network to the different network.

    :param flows_stream: Input flows
    :param local_network: Local network's address
    :return: Flows coming from local network to external networks
    """
    return flows_stream \
        .filter(lambda dns_json: (IPAddress(get_ip(dns_json, "source")) in IPNetwork(local_network)) and
                                 (IPAddress(get_ip(dns_json, "destination")) not in IPNetwork(local_network)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-m", "--microbatch", help="microbatch (in seconds)", type=int, required=False, default=5)

    # Define Arguments for detection
    parser.add_argument("-ln", "--local_network", help="local network", type=str, required=True)
    parser.add_argument("-wd", "--whitelisted_domains", help="whitelisted domains",
                        type=str, required=False, default="./whitelisted_domains.txt")
    parser.add_argument("-wn", "--whitelisted_networks", help="whitelisted networks",
                        type=str, required=False, default="./whitelisted_networks.txt")

    # Parse arguments
    args = parser.parse_args()

    # Read whitelisted domains (100 maximum)
    whitelisted_domains = ""
    whitelisted_domains_regex = ""
    if os.path.isfile(args.whitelisted_domains):
        with open(args.whitelisted_domains, 'r') as f:
            strings = f.readlines()
        whitelisted_domains = [".*" + line.strip() for line in strings]
        whitelisted_domains_regex = "(" + ")|(".join(whitelisted_domains) + ")"
    else:
        cprint("[warning] File with whitelisted domains does not exist.", "blue")

    # Read whitelisted IPs
    whitelisted_networks = ""
    if os.path.isfile(args.whitelisted_networks):
        with open(args.whitelisted_networks, 'r') as f:
            strings = f.readlines()
        whitelisted_networks = [IPNetwork(line.strip()) for line in strings]
    else:
        cprint("[warning] File with whitelisted networks does not exist.", "blue")

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Prepare input stream
    dns_stream = get_dns_stream(parsed_input_stream)
    dns_external_to_local = get_flows_local_to_external(dns_stream, args.local_network)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Calculate and process DNS statistics
    get_open_dns_resolvers(dns_external_to_local, whitelisted_domains_regex, whitelisted_networks) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, args.output_topic))

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
