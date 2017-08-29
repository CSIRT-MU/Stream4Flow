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
Computes basic dns statistics within given time interval.

Default output parameters:
    * Address and port of the broker: 10.16.31.200:9092
    * Kafka topic: results.output

Usage:
    dns_top_n.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -lc <local-network>/<subnet-mask>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then you
can run the application
    $ ./run-application.sh ./statistics/dns_stats/spark/dns_stats.py -iz producer:2181\
    -it ipfix.entry -oz producer:9092 -ot results.output -lc 10.10.0.0/16
"""

import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver

from kafka import KafkaProducer  # Kafka Python client
from netaddr import IPNetwork, IPAddress


def send_to_kafka(data, producer, topic):
    """
    Send given data to the specified kafka topic.

    :param data: data to send
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """
    producer.send(topic, str(data))


def get_output_json(key, value):
    """
    Create JSON with correct format.

    :param key: key of particular record
    :param value: value of particular record
    :return: JSON string in desired format
    """

    output_json = "{\"@type\": \"dns_statistics\", \"@stat_type\": \"" + key + "\", \"data_array\": ["

    if key == "queried_by_ip":
        dictionary = get_top_n_records(value, "ips", 100)
        output_json += ', '.join(get_format(dictionary, "ips"))

    elif key == "queried_domain" or key == "nonexisting_domain":
        dictionary = get_top_n_records(value, "value", 100)
        output_json += ', '.join(get_format(dictionary))

    else:
        output_json += ', '.join(get_format(value))

    output_json += "]" + "}\n"
    return output_json


def get_format(dictionary, value_name="value"):
    """
    Gets dictionary with computed statistics and formats it into list
    of key, value pairs that will be sent as part of the output json
    :param dictionary: dictionary with computer statistics
    :param value_name: attribute name for the dictionary value
    :return: List with key, value pairs
    """
    rec_list = []
    if value_name == "ips":
        for k, v in dictionary.iteritems():
            # Converts dictionary to JSON format
            conv_json = "{\"key\": \"" + unicode(k[0]).encode('utf8') + "\", \"value\": " + unicode(v).encode('utf8') \
                        + ", \"" + value_name + "\": \"" + unicode(k[1]).encode('utf8') + "\"}"

            # Appends JSON to the list
            rec_list.append(conv_json)

    else:
        for k, v in dictionary.iteritems():
            # Changes the field names in dictionary
            temp_dict = {"key": k, value_name: v}

            # Converts dictionary to JSON
            conv_json = json.dumps(temp_dict)

            # Appends JSON to the list
            rec_list.append(conv_json)

    return rec_list


def get_top_n_records(data, value_name, n=40):
    """
    Get top n records by value_name.
    :param data: records from which top n is calculated
    :param value_name: name of the attribute with value with which records are sorted
    :param n: top number of records
    :return: Dictionary with top n key, value pairs.
    """
    if value_name == "ips":
        top = sorted(data.items(), key=lambda x: len([x]), reverse=True)[:n]
    else:
        top = sorted(data.items(), key=lambda x: [x], reverse=True)[:n]
    return dict(top)


def process_results(results, producer, topic):
    """
    Format and report computed statistics.

    :param results: computed statistics
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """

    output_json = ""
    # Transform given results into the JSON
    for key, value in results.iteritems():
        output_json += get_output_json(key, value)

    if output_json:
        # Print data to standard output
        print(output_json)

        # Send results to the specified kafka topic
        send_to_kafka(output_json, producer, topic)


def get_query_type(key):
    """
    Translates numerical key into the string value for dns query type
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
    Translates numerical key into the string value for dns response code
    :param key: Numerical value to be translated
    :return: Translated response code
    """
    return {
        0: 'NoError', 1: 'FormErr', 2: 'ServFail', 3: 'NXDomain', 4: 'NotImp', 5: 'Refused', 6: 'YXDomain',
        7: 'YXRRSet', 8: 'NXRRSet', 9: 'NotAuth', 10: 'NotZone', 11: 'BADVERS', 12: 'BADSIG', 13: 'BADKEY',
        14: 'BADTIME'
    }.get(key, 'Other')


def get_statistics(flows_stream, local_network, s_window_duration, s_window_slide):
    """
    Aggregate flows within given time window and return aggregates that fulfill given requirements.

    :param flows_stream:
    :param local_network: address of network that should be considered as local
    :param s_window_duration: window size (in seconds)
    :param s_window_slide: slide interval of the analysis window
    :return: computed dns statistics
    """

    # Check required flow keys
    flows_stream_filtered = flows_stream \
        .filter(lambda flow_json: ("ipfix.protocolIdentifier" in flow_json.keys()) and
                                  ("ipfix.DNSName" in flow_json.keys()) and
                                  ("ipfix.sourceIPv4Address" in flow_json.keys()))

    flow_local_to_external = flows_stream_filtered \
        .filter(lambda flow_json: (IPAddress(flow_json["ipfix.sourceIPv4Address"]) in IPNetwork(local_network)) and
                                  (IPAddress(flow_json["ipfix.destinationIPv4Address"]) not in IPNetwork(local_network)))

    flow_from_local = flows_stream_filtered \
        .filter(lambda flow_json: (IPAddress(flow_json["ipfix.sourceIPv4Address"]) in IPNetwork(local_network)))

    flow_to_local = flows_stream_filtered \
        .filter(lambda flow_json: (IPAddress(flow_json["ipfix.destinationIPv4Address"]) in IPNetwork(local_network)))

    # Record Types
    dns_rec_types = flow_from_local \
        .map(lambda record: (("record_type", record["ipfix.DNSQType"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {get_query_type(record[0][1]): record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Response codes
    dns_res_codes = flow_to_local \
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '1')) \
        .map(lambda record: (("response_code", int((bin(record["ipfix.DNSFlagsCodes"])[2:].zfill(16))[-4:], 2)), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {get_response_code(record[0][1]): record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Top N sent to output
    # Queried domains by domain name
    queried_domains = flow_from_local\
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '0')) \
        .map(lambda record: (("queried_domain", record["ipfix.DNSName"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .filter(lambda record: record[1] >= 2)\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {record[0][1]: record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Top N sent to output
    # Queried domains that do not exist
    dns_nonexisting_domains = flow_to_local \
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '1')) \
        .filter(lambda flow_json: (int((bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16))[-4:], 2) == 3)) \
        .map(lambda record: (("nonexisting_domain", record["ipfix.DNSName"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {record[0][1]: record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Queried external dns servers from local network
    queried_external_dns_servers = flow_local_to_external\
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '0')) \
        .map(lambda record: (("external_dns", record["ipfix.destinationIPv4Address"]), record["ipfix.DNSQuestionCount"])) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {record[0][1]: record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Succesfully queried dns servers on local network from the outside network
    queried_local_dns_from_outside = flow_local_to_external \
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '1')) \
        .filter(lambda flow_json: (int(bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[-4:], 2)) != 5) \
        .map(lambda record: (("queried_local", record["ipfix.sourceIPv4Address"]), record["ipfix.packetDeltaCount"])) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .map(lambda record: (record[0][0], {record[0][1]: record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Top N sent to output
    device_with_most_records = flow_from_local\
        .filter(lambda flow_json: (bin(flow_json["ipfix.DNSFlagsCodes"])[2:].zfill(16)[0] == '0'))\
        .map(lambda record: (("queried_by_ip", record["ipfix.DNSName"], record["ipfix.sourceIPv4Address"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))\
        .window(s_window_duration, s_window_slide) \
        .reduceByKey(lambda actual, update: (actual + update)) \
        .map(lambda record: (record[0][0], {(record[0][1], record[0][2]): record[1]})) \
        .reduceByKey(lambda actual, update: (merge_dicts(actual, update)))

    # Merge all types of statistics together
    all_statistics = dns_rec_types.union(dns_res_codes).union(dns_nonexisting_domains).union(queried_domains)\
        .union(queried_external_dns_servers).union(queried_local_dns_from_outside).union(device_with_most_records)

    # Return computed statistics
    return all_statistics


def merge_dicts(a, b):
    a.update(b)
    return a


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    # parser.add_argument("-w", "--window_size", help="window size (in seconds)", type=int, required=False, default=300)

    # Define Arguments for detection
    parser.add_argument("-lc", "--local_network", help="local network ip address netaddress/mask",
                        type=str, required=True)

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    # window_duration = args.window_size  # Analysis window duration (600 seconds/10 minutes default)
    window_slide = 5  # Slide interval of the analysis window (5 second)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 10)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    flows_json = input_stream.map(lambda line: json.loads(line[1]))

    # Calculate statistics
    statistics = get_statistics(flows_json, args.local_network, 20, 10)

    # Initialize kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=args.output_zookeeper,
                                   client_id="spark-producer-" + application_name)

    # Process computed statistics and send them to the standard output
    statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, args.output_topic))

    # Send any remaining buffered records
    kafka_producer.flush()

    # Start Spark streaming context
    ssc.start()
    ssc.awaitTermination()
