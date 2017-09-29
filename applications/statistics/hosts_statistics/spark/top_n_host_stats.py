# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016 Michal Stefanik <stefanik.m@mail.muni.cz>, Tomas Jirsik <jirsik@ics.muni.cz> Institute of Computer Science, Masaryk University
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
Template application for creating new applications using provided module and Spark operations, with a possibility of
adding more advanced modules. This template simply resends one row of data from given input topic to defined output topic.

Usage:
    top_n_host_stats.py --iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -n <CIDR network range>
    -wd <window duration> -ws <window slide> -c <count of top_n>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
you can run the application as follows:
    $ /run-application.sh statistics/hosts_statistics/spark/application_template.py -iz producer:2181 -it ipfix.entry
    -oz producer:9092 -ot results.output -n 10.10.0.0/16 -wd 10 -ws 10 -c 10

"""


import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
from termcolor import cprint  # Colors in the console output

from modules import kafkaIO  # IO operations with kafka topics

from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network
from collections import namedtuple  # Support for named tuple
import time  # Time handling


def _sort_by_flows(stats_values):
    """
    Sorts the list of StatsItem by flows attribute
    :param stats_values: list of StatsItem
    :return: sorted list
    """
    return sorted(stats_values, key=lambda entry: entry.flows, reverse=True)


def _parse_stats_items_list(stats_values):
    """
    Parses the list of StatsItem into a dict of object for output JSON fromat
    :param stats_values: list of StatsItem
    :return: dict in output JSON format
    """
    item_list = map(lambda stats_item: {stats_item.type: stats_item.key, "flows": stats_item.flows}, stats_values)

    # format the list of parsed StatsItems to a dict with keys of items' ordering
    labeled_item_dict = {}
    for order, item in map(lambda order: (order, item_list[order]), range(len(item_list))):
        labeled_item_dict[int(order)] = item

    return labeled_item_dict




def process_results(data_to_process, producer, output_topic, count_top_n):
    """
    Process analyzed data and modify it into desired output.

    :param data_to_process: analyzed data  in a format: (src IP , IPStats([PortStats], [DstIPStats], [HTTPHostStats]))
    :param producer: Kafka producer
    :param output_topic: Kafka topic through which output is send
    :param count_top_n: integer of N in TopN

    JSON format:
        {"src_ip":"<host src IPv4 address>",
         "@type":"host_stats_topn_ports",
         "stats":{
            "top_n_dst_ports":
                {
                    "0": {"port":<port #1>, "flows":# of flows},
                    ...
                    "n": {"port":<port #n>, "flows":# of flows}
                },
                "top_n_dst_hosts":
                {
                    "0": {"dst_host":<dst_host #1>, "flows":# of flows},
                    ...
                    "n": {"dst_host":<dst_host #n>, "flows":# of flows}
                }
                "top_n_http_dst":
                {
                    "0": {"dst_host":<dst_host #1>, "flows":# of flows},
                    ...
                    "n": {"dst_host":<dst_host #n>, "flows":# of flows}
                }
            }
        }
    """

    for ip, ip_stats in data_to_process.iteritems():
        # Define output keys for particular stats in X_Stats named tuples
        port_data_dict = {"top_n_dst_ports": ip_stats.ports,
                          "top_n_dst_hosts": ip_stats.dst_ips,
                          "top_n_http_dst": ip_stats.http_hosts}

        # Take top n entries from IP's particular stats sorted by flows param
        port_data_dict = {key: _sort_by_flows(val_list)[:count_top_n] for (key, val_list) in port_data_dict.iteritems()}
        # parse the stats from StatsItem to a desirable form
        port_data_dict = {key: _parse_stats_items_list(val_list) for (key, val_list) in port_data_dict.iteritems()}

        # Construct the output object in predefined format
        result_dict = {"@type": "top_n_host_stats",
                       "src_ip": ip,
                       "stats": port_data_dict}

        # Dump results
        results_output = json.dumps(result_dict) + "\n"

    # Logging terminal output
    print("%s: Stats of %s IPs parsed and sent" % (time.strftime("%c"), len(data_to_process.keys())))

    # Send desired output to the output_topic
    kafkaIO.send_data_to_kafka(results_output, producer, output_topic)


def process_input(input_data, window_duration, window_slide, network_filter):
    """
       Main function to count TOP N ports for observed IPs by its activity (= # of flows).

       :param input_data: Initialized spark streaming context, windowed, json_loaded.
       :param window_duration: duration of window for statistics count in seconds
       :param window_slide: slide of window for statistics count in seconds
       :param network_filter: filter for filtration network range in CIDR
    """
    # Optional arguments for named tuple
    IPStats = namedtuple('IPStats', 'ports dst_ips http_hosts')
    StatsItem = namedtuple('StatsItem', 'key flows type')

    # Filter flows with required data, in a given address range
    flow_with_keys = input_data.filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys()) and
                                                        ("ipfix.destinationTransportPort" in json_rdd.keys()))

    # if IP network range input parameter is filled, filter the flows respectively
    if args.network_range is not None:
       flow_with_keys = flow_with_keys.filter(lambda json_rdd: (IPAddress(json_rdd["ipfix.sourceIPv4Address"]) in IPNetwork(network_filter)))

    # Set window and slide duration for flows analysis
    flows_with_keys_windowed = flow_with_keys.window(window_duration, window_slide)

    # Destination ports stats
    # Aggregate the number of flows for all IP-port tuples
    flows_by_ip_port = flows_with_keys_windowed \
        .map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.destinationTransportPort"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))
    # Aggregate the (port: <# of flows>) logs for all IPs
    flows_for_ip_ports = flows_by_ip_port \
        .map(lambda json_rdd: (json_rdd[0][0], list([StatsItem(json_rdd[0][1], json_rdd[1], "port")]))) \
        .reduceByKey(lambda actual, update: actual + update)

    # Destination IP stats
    # Aggregate the number of flows for src_IP-dst_IP tuples
    flows_by_ip_dst_host = flows_with_keys_windowed \
        .map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.destinationIPv4Address"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))
    # Aggregate the (dst_host: <# of flows>) logs for IPs
    flows_for_ip_dst_hosts = flows_by_ip_dst_host \
        .map(lambda json_rdd: (json_rdd[0][0], list([StatsItem(json_rdd[0][1], json_rdd[1], "dst_host")]))) \
        .reduceByKey(lambda actual, update: actual + update)

    # HTTP destination stats
    # Aggregate (http_address: <# of flows>) logs for all IPs
    flow_with_http_keys = flows_with_keys_windowed.filter(lambda json_rdd: "ipfix.HTTPRequestHost" in json_rdd.keys())
    flows_by_ip_http_host = flow_with_http_keys \
        .map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.HTTPRequestHost"]), 1)) \
        .reduceByKey(lambda actual, update: (actual + update))
    # Aggregate the (http_host: <# of flows>) logs for all IPs
    flows_for_ip_http_hosts = flows_by_ip_http_host \
        .map(lambda json_rdd: (json_rdd[0][0], list([StatsItem(json_rdd[0][1], json_rdd[1], "http_host")]))) \
        .reduceByKey(lambda actual, update: actual + update)

    # join the gathered stats on a shared keys (=srcIPs)
    port_host_stats_joined = flows_for_ip_ports.join(flows_for_ip_dst_hosts)
    port_host_stats_joined = port_host_stats_joined.join(flows_for_ip_http_hosts)
    # cast the joined stats to IPStats objects
    port_host_stats_joined_obj = port_host_stats_joined.mapValues(
        lambda values: IPStats(values[0][0], values[0][1], values[1]))

    return port_host_stats_joined_obj


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-n", "--network_range", help="network range to watch", type=str, required=False)
    parser.add_argument("-c", "--count_top_n", help="max. number of ports for a host to retrieve", type=int, required=True)
    parser.add_argument("-wd", "--window_duration", help="analysis window duration in seconds", type=str, required=True)
    parser.add_argument("-ws", "--window_slide", help="analysis window slide in seconds", type=str, required=True)

    # You can add your own arguments here
    # See more at:
    # https://docs.python.org/2.7/library/argparse.html

    # Parse arguments
    args = parser.parse_args()

    # Set microbatch duration to 1 second
    microbatch_duration = int(args.window_slide)

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO.initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, microbatch_duration)

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream, int(args.window_duration), int(args.window_slide), args.network_range)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    processed_input.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, args.output_topic, int(args.count_top_n)))

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
