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
Computes basic dns statistics within given time interval.

Default output parameters:
    * Address and port of the broker: producer:9092
    * Kafka topic: results.output

Domain name filtering for non-existing domains statistic:
    * All domains containing substrings in file are filtered out
    * Usage: -f <filepath>
    * Format: One domain name per line

Usage:
    dns_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -lc <local-network>/<subnet-mask>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./statistics/dns_statistics/spark/dns_statistics.py -iz producer:2181\
    -it ipfix.entry -oz producer:9092 -ot results.output -lc 10.10.0.0/16
"""

from __future__ import with_statement  # File reading operation

import argparse  # Arguments parser
import ujson as json  # Fast JSON parser

from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network
from modules import kafkaIO  # IO operations with kafka topics


def get_output_json(dictionary, s_statistic_type):
    """
    Create JSON with correct format.

    :param dictionary: All computed records for the statistic type
    :param s_statistic_type: Type of the computed statistic
    :return: JSON string in desired format
    """

    return "{\"@type\": \"dns_statistics\", \"@stat_type\": \"" + s_statistic_type + "\", \"data_array\": [" + \
           ', '.join(get_format(dictionary, s_statistic_type)) + "]" + "}\n"


def get_format(dictionary, s_statistic_type):
    """
    Gets dictionary with computed statistics and formats it into list
    of key, value pairs (key, value, ips if statistic type is queried_by_ip)
    that will be sent as part of the output json.

    :param dictionary: Dictionary with computer statistics
    :param s_statistic_type: Type of the statistic
    :return: List with key, value pairs (key, value, ip tripple if statistic type is queried_by_ip)
    """
    rec_list = []
    if s_statistic_type == "queried_by_ip":
        for k, v in dictionary.iteritems():
            # Converts dictionary to JSON format
            conv_json = "{\"key\": \"" + unicode(k[0]).encode('utf8') + "\", \"value\": " + unicode(v).encode('utf8') \
                        + ", \"ip\": \"" + unicode(k[1]).encode('utf8') + "\"}"

            # Appends JSON to the list
            rec_list.append(conv_json)

    else:
        for k, v in dictionary.iteritems():
            # Changes the field names in dictionary
            temp_dict = {"key": k, "value": v}

            # Converts dictionary to JSON
            conv_json = json.dumps(temp_dict)

            # Appends JSON to the list
            rec_list.append(conv_json)

    return rec_list


def process_results(results, producer, s_output_topic, s_statistic_type):
    """
    Format and report computed statistics.

    :param results: Computed statistics
    :param producer: Producer that sends the data
    :param s_output_topic: Name of the receiving kafka topic
    :param s_statistic_type: Type of the statistic
    """
    output_json = ""
    # Transform given results into the JSON
    output_json += get_output_json(results, s_statistic_type)

    if output_json:
        # Print data to standard output
        print(output_json)

        # Send results to the specified kafka topic
        kafkaIO.send_data_to_kafka(output_json, producer, s_output_topic)


def get_query_type(key):
    """
    Translates numerical key into the string value for dns query type.

    :param key: Numerical value to be translated
    :return: Translated query type
    """
    return {
        1: 'A', 2: 'NS', 3: 'MD', 4: 'MF', 5: 'CNAME', 6: 'SOA', 7: 'MB', 8: 'MG', 9: 'MR0', 10: 'NULL', 11: 'WKS',
        12: 'PTR', 13: 'HINFO', 14: 'MINFO', 15: 'MX', 16: 'TXT', 17: 'RP', 18: 'AFSDB', 19: 'X25', 20: 'ISDN',
        21: 'RT', 22: 'NSAP', 23: 'NSAP-PTR', 24: 'SIG', 25: 'KEY', 26: 'PX', 27: 'GPOS', 28: 'AAAA', 29: 'LOC',
        30: 'NXT', 31: 'EID', 32: 'NIMLOC', 33: 'SRV', 34: 'ATMA', 35: 'NAPTR', 36: 'KX', 37: 'CERT', 38: 'A6',
        39: 'DNAME', 49: 'DHCID', 50: 'NSEC3', 51: 'NSEC3PARAM', 52: 'TLSA', 53: 'SMIMEA', 55: 'HIP', 56: 'NINFO',
        57: 'RKEY', 58: 'TALINK', 59: 'CDS', 60: 'CDNSKEY', 61: 'OPENPGPKEY', 62: 'CSYNC', 99: 'SPF', 100: 'UINFO',
        101: 'UID', 102: 'GID', 103: 'UNSPEC', 104: 'NID', 105: 'L32', 106: 'L64', 107: 'LP', 108: 'EUI48',
        109: 'EUI164', 249: 'TKEY', 250: 'TSIG', 251: 'IXFR', 252: 'AXFR', 253: 'MAILB', 254: 'MAILA', 255: '*',
        256: 'URI', 257: 'CAA', 258: 'AVC', 32768: 'TA', 32769: 'DLV'
    }.get(key, 'OTHER')


def get_response_code(key):
    """
    Translates numerical key into the string value for dns response code.

    :param key: Numerical value to be translated
    :return: Translated response code
    """
    return {
        0: 'NoError', 1: 'FormErr', 2: 'ServFail', 3: 'NXDomain', 4: 'NotImp', 5: 'Refused', 6: 'YXDomain',
        7: 'YXRRSet', 8: 'NXRRSet', 9: 'NotAuth', 10: 'NotZone', 11: 'BADVERS', 12: 'BADSIG', 13: 'BADKEY',
        14: 'BADTIME'
    }.get(key, 'Other')


def get_rec_types(s_input_stream, s_window_duration):
    """
    Gets the number of occurrences for each type of DNS Record.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream \
        .map(lambda record: (get_query_type(record["ipfix.DNSQType"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_res_codes(s_input_stream, s_window_duration):
    """
    Gets the number of occurrences for each type of DNS response code.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream \
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] >> 15) & 1) \
        .map(lambda record: (get_response_code(record["ipfix.DNSFlagsCodes"] & 15), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_queried_domains(s_input_stream, s_window_duration):
    """
    Gets the number of occurrences for each queried domain name.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    # Records with 1 occurrence are discarded to reduce the amount of data dramatically
    return s_input_stream\
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] >> 15) == 0) \
        .map(lambda record: (record["ipfix.DNSName"], 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .filter(lambda record: record[1] >= 2)\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_non_existing_queried_domains(s_input_stream, s_window_duration):
    """
    Gets the number of occurrences for each domain name that was resolved as being non-existent.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream \
        .filter(lambda flow_json: flow_json["ipfix.DNSFlagsCodes"] >> 15 & 1) \
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] & 15) == 3) \
        .map(lambda record: (record["ipfix.DNSName"], 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_queried_external_dns_servers(s_input_stream, s_window_duration):
    """
    For each queried DNS server not in local network computes the amount of queries from the local network.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream\
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] >> 15) == 0) \
        .map(lambda record: (record["ipfix.destinationIPv4Address"], 1))\
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_queried_local_dns_from_outside(s_input_stream, s_window_duration):
    """
    For each DNS server on local network computes the amount of successfully resolved queries from the outside networks.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream \
        .filter(lambda flow_json: flow_json["ipfix.DNSFlagsCodes"] >> 15 & 1) \
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] & 15) != 5) \
        .map(lambda record: (record["ipfix.sourceIPv4Address"], 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_queried_domains_by_ip(s_input_stream, s_window_duration):
    """
    For each queried domain computes all ips that queried it with the query count.

    :param s_input_stream: Input flows
    :param s_window_duration: Length of the window in seconds
    :return: Transformed input stream
    """
    return s_input_stream\
        .filter(lambda flow_json: (flow_json["ipfix.DNSFlagsCodes"] >> 15) == 0)\
        .map(lambda record: ((record["ipfix.DNSName"], record["ipfix.sourceIPv4Address"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_duration) \
        .reduceByKey(lambda actual, update: (actual + update))


def get_dns_stream(flows_stream):
    """
    Filter to get only flows containing DNS information.

    :param flows_stream: Input flows
    :return: Flows with DNS information
    """
    return flows_stream \
        .filter(lambda flow_json: ("ipfix.DNSName" in flow_json.keys()) and
                                  ("ipfix.sourceIPv4Address" in flow_json.keys()))


def get_flows_local_to_external(s_dns_stream, local_network):
    """
    Filter to contain flows going from the specified local network to the different network.

    :param s_dns_stream: Input flows
    :param local_network: Local network's address
    :return: Flows coming from local network to external networks
    """
    return s_dns_stream \
        .filter(lambda dns_json: (IPAddress(dns_json["ipfix.sourceIPv4Address"]) in IPNetwork(local_network)) and
                                 (IPAddress(dns_json["ipfix.destinationIPv4Address"]) not in IPNetwork(local_network)))


def get_flows_from_local(s_dns_stream, local_network):
    """
    Filter to contain flows going from the specified local network.

    :param s_dns_stream: Input flows
    :param local_network: Local network's address
    :return: Flows coming from local network
    """
    return s_dns_stream\
        .filter(lambda dns_json: (IPAddress(dns_json["ipfix.destinationIPv4Address"]) in IPNetwork(local_network)))


def get_flows_to_local(s_dns_stream, local_network):
    """
    Filter to contain flows going to the specified local network.

    :param s_dns_stream: Input flows
    :param local_network: Local network's address
    :return: Flows coming to local network
    """
    return s_dns_stream \
        .filter(lambda dns_json: (IPAddress(dns_json["ipfix.sourceIPv4Address"]) in IPNetwork(local_network)))


def filter_out_domain(stream, domain_to_filter):
    """
    Filters flows from the passed domain name.

    :param stream: Input flows
    :param domain_to_filter: Domain name which should be filtered out
    :return: Filtered flows
    """
    return stream.filter(lambda record: domain_to_filter not in record["ipfix.DNSName"])


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
    parser.add_argument("-f", "--filtered_domains", help="path to file with filtered out domains",
                        type=str, required=False, default="")

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    window_duration = args.window_size  # Analysis window duration (20 seconds default)
    microbatch = args.microbatch
    output_topic = args.output_topic

    # Read domains that should be filtered out for statistic type "nonexisting_domain"
    filtered_domains = ""
    if args.filtered_domains:
        with open(args.filtered_domains, 'r') as f:
            strings = f.readlines()
        filtered_domains = [line.strip() for line in strings]

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, microbatch)

    # Process input in the desired way
    dns_stream = get_dns_stream(parsed_input_stream)

    # Prepare input streams for corresponding statistics
    dns_local_to_external = get_flows_local_to_external(dns_stream, args.local_network)
    dns_from_local = get_flows_from_local(dns_stream, args.local_network)
    dns_to_local = get_flows_to_local(dns_stream, args.local_network)
    filtered_domains_stream = dns_to_local
    for domain in filtered_domains:
        filtered_domains_stream = filter_out_domain(filtered_domains_stream, domain)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Calculate and process DNS statistics
    get_rec_types(dns_from_local, window_duration) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, output_topic, "record_type"))

    get_res_codes(dns_to_local, window_duration) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, output_topic, "response_code"))

    get_queried_domains(dns_from_local, window_duration) \
        .foreachRDD(lambda rdd: process_results(dict(rdd.top(100, key=lambda x: x[1])),
                                                kafka_producer, output_topic, "queried_domain"))

    get_non_existing_queried_domains(filtered_domains_stream, window_duration) \
        .foreachRDD(lambda rdd: process_results(dict(rdd.top(100, key=lambda x: x[1])),
                                                kafka_producer, output_topic, "nonexisting_domain"))

    get_queried_external_dns_servers(dns_local_to_external, window_duration) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, output_topic, "external_dns"))

    get_queried_local_dns_from_outside(dns_local_to_external, window_duration) \
        .foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, output_topic, "queried_local"))

    get_queried_domains_by_ip(dns_from_local, window_duration) \
        .foreachRDD(lambda rdd: process_results(dict(rdd.top(100, key=lambda x: x[1])),
                                                kafka_producer, output_topic, "queried_by_ip"))

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
