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

Domain name filtering for non-existing domains statistic:
    * All domains containing substrings in file are filtered out
    * Usage: -fd <filepath>
    * Format: One domain name per line

Usage:
    dns_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -ln <local-network>/<subnet-mask>
    -m <microbatch-duration> -w <window-duration>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ~/applications/run-application.sh ./dns_statistics.py -iz producer:2181 -it ipfix.entry -oz producer:9092
    -ot results.output -ln 10.10.0.0/16
"""

from __future__ import with_statement  # File reading operation

import argparse  # Arguments parser
import ujson as json  # Fast JSON parser

from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network
from modules import kafkaIO  # IO operations with kafka topics


def process_results(results, producer, output_topic):
    """
    Format and report computed statistics.

    :param results: Computed statistics
    :param producer: Producer that sends the data
    :param output_topic: Name of the receiving kafka topic
    """
    # Dictionary to store all data for given statistic
    statistics = {}
    for key, value in results.iteritems():
        # Get statistic name (last element of the key)
        statistic_type = key[-1]
        # Create empty list if statistic type is not in statistics dictionary
        if statistic_type not in statistics.keys():
            statistics[statistic_type] = []
        # Get data part in JSON string format
        if statistic_type == "queried_by_ip":
            data = {"key": key[0], "value": value, "ip": key[1]}
        elif (statistic_type == "queried_domain") and (value == 1):
            # Skip queried domains with only one occurrence
            continue
        else:
            data = {"key": key[0], "value": value}
        # Append data to statistics dictionary
        statistics[statistic_type].append(data)

    # Create all statistics JSONs in string format
    output_json = ""
    for statistic_type, data in statistics.iteritems():
        # Check if Top 100 data elements should be selected to reduce volume of data in database
        if statistic_type in ["queried_domain", "nonexisting_domain", "queried_by_ip"]:
            data.sort(key=lambda stat: stat['value'], reverse=True)
            data_array = json.dumps(data[:100])
        else:
            data_array = json.dumps(data)

        output_json += "{\"@type\": \"dns_statistics\", \"@stat_type\": \"" + statistic_type + "\", " + \
                       "\"data_array\": " + data_array + "}\n"

    if output_json:
        # Print data to standard output
        print(output_json)

        # Send results to the specified kafka topic
        kafkaIO.send_data_to_kafka(output_json, producer, output_topic)


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


def is_whitelisted(record, domains_whitelist):
    """
    Check if queried DNS domain is not in given whitelist.

    :param record: record with DNS query
    :param domains_whitelist: list of domains
    :return: True if quieried domain is in the whitelist, False otherwise
    """
    # Iterate through all whitelisted domains
    for domain in domains_whitelist:
        # Check if whitelisted domain is part of the queried name
        if domain in record["ipfix.DNSName"]:
            return True
    # Return False otherwise
    return False


def get_dns_stats_mapping(record, local_network, domains):
    """
    Transform given record to all mappings required for DNS statistics computation.

    :param record: one record of the data stream
    :param local_network: address of local network in CIDR format
    :param domains: array of whitelisted domains
    :return: Array of all mappings derived from given record
    """
    # Array to store all mappings for given record
    maps = []

    # Get record properties
    to_local_network = get_ip(record, "destination") in IPNetwork(local_network)
    from_local_network = get_ip(record, "source") in IPNetwork(local_network)
    is_response = record["ipfix.DNSFlagsCodes"] >> 15 & 1
    is_query = (record["ipfix.DNSFlagsCodes"] >> 15) == 0

    # Map queried domains from successful responses
    if to_local_network and is_response:
        maps.append(((record["ipfix.DNSName"], "queried_domain"), 1))

    # Map queried domains from non-existing response
    if to_local_network and is_response and ((record["ipfix.DNSFlagsCodes"] & 15) == 3):
        # Append mapping if non-existing domain is not whitelisted
        if not is_whitelisted(record, domains):
            maps.append(((record["ipfix.DNSName"], "nonexisting_domain"), 1))

    # Map DNS response codes from all responses
    if to_local_network and is_response:
        maps.append(((get_response_code(record["ipfix.DNSFlagsCodes"] & 15), "response_code"), 1))

    # Map DNS record types from all queries
    if from_local_network:
        maps.append(((get_query_type(record["ipfix.DNSQType"]), "record_type"), 1))

    # Map local DNS servers queried from non-local network that not returned "Refused" response
    if to_local_network and (get_ip(record, "source") not in IPNetwork(local_network)) and \
            is_response and ((record["ipfix.DNSFlagsCodes"] & 15) != 5):
        maps.append(((record["ipfix.sourceIPv4Address"], "queried_local"), 1))

    # Map external DNS servers queried from local network
    if  from_local_network and (get_ip(record, "destination") not in IPNetwork(local_network)) and is_query:
        maps.append(((record["ipfix.destinationIPv4Address"], "external_dns"), 1))

    # Map queried domains by local network IP
    if from_local_network and is_query:
        maps.append(((record["ipfix.DNSName"], record["ipfix.sourceIPv4Address"], "queried_by_ip"), 1))

    # Return array of all mappings derived from given record
    return maps


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-w", "--window", help="window size (in seconds)", type=int, required=False, default=60)
    parser.add_argument("-m", "--microbatch", help="microbatch (in seconds)", type=int, required=False, default=30)

    # Define Arguments for detection
    parser.add_argument("-ln", "--local_network", help="local network", type=str, required=True)
    parser.add_argument("-fd", "--filtered_domains", help="path to file with filtered out non-existing domains",
                        type=str, required=False, default="")

    # Parse arguments
    args = parser.parse_args()

    # Read domains that should be filtered out for statistic type "nonexisting_domain"
    filtered_domains = ""
    if args.filtered_domains:
        with open(args.filtered_domains, 'r') as f:
            strings = f.readlines()
        filtered_domains = [line.strip() for line in strings]

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO \
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Get flow with DNS elements
    dns_stream = parsed_input_stream.filter(lambda flow_json: ("ipfix.DNSName" in flow_json.keys()))

    # Get mapping of DNS statistics
    dns_stream_map = dns_stream \
        .flatMap(lambda record: get_dns_stats_mapping(record, args.local_network, filtered_domains))

    # Get statistics within given window
    dns_statistics = dns_stream_map.reduceByKey(lambda actual, update: (actual + update)) \
        .window(args.window, args.window) \
        .reduceByKey(lambda actual, update: (actual + update))

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(dns_statistics, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)