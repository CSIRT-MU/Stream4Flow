# TODO: placeholder application - see functionality suggestions in the main method

# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2017 Michal Stefanik <stefanik dot m@mail.muni.cz>, Tomas Jirsik <jirsik@ics.muni.cz>
# Institute of Computer Science, Masaryk University
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
Description: A method for computing statistics for hosts in network. Computed statistics
for each host each window contain:
    - a list of top n most active ports as sorted by a number of flows on a given port

Usage:
  top_n_host_stats.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -n <max. # of ports for host> -net <CIDR network range>

  To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
  you can run the example
    $ ./run-application.sh ./statistics/hosts_statistics/spark/top_n_host_stats.py -iz producer:2181 -it ipfix.entry
    -oh consumer:20101 -n 5 -net "10.0.0.0/24"

"""

import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface
import time  # Time handling
import ipaddress  # IP address handling

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver

from collections import namedtuple

IPStats = namedtuple('IPStats', 'ports dst_ips http_hosts')
StatsItem = namedtuple('StatsItem', 'key flows type')


def send_data(data, output_host):
    """
    Send given data to the specified host using standard socket interface.

    :param data: data to send
    :param output_host: data receiver in the "hostname:port" format
    """
    print data

    # Split outputHost hostname and port
    host = output_host.split(':')

    # Prepare a TCP socket.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to the outputHost and send given data
    try:
        sock.connect((host[0], int(host[1])))
        sock.send(data)

        # Print message of sent
        now = time.strftime("%c")
        print("Data sent at: %s" % now)

    except socket.error:
        cprint("[warning] Unable to connect to host " + output_host, "blue")
    finally:
        sock.close()


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


def process_results(json_rdd, n=10):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
    {"src_ipv4":"<host src IPv4 address>",
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

    :param json_rrd: Map in a format: (src IP , IPStats([PortStats], [DstIPStats], [HTTPHostStats]))
    :return: json with selected TOP n ports by # of flows for each src IP
    """

    # fill in n from input params
    if args.top_n is not None:
        n = int(args.top_n)

    for ip, ip_stats in json_rdd.iteritems():
        # define output keys for particular stats in X_Stats named tuples
        port_data_dict = {"top_n_dst_ports": ip_stats.ports,
                          "top_n_dst_hosts": ip_stats.dst_ips,
                          "top_n_http_dst": ip_stats.http_hosts}

        # take top n entries from IP's particular stats sorted by flows param
        port_data_dict = {key: _sort_by_flows(val_list)[:n] for (key, val_list) in port_data_dict.iteritems()}
        # parse the stats from StatsItem to a desirable form
        port_data_dict = {key: _parse_stats_items_list(val_list) for (key, val_list) in port_data_dict.iteritems()}

        # construct the output object in predefined format
        result_dict = {"@type": "top_n_host_stats",
                       "src_ipv4": ip,
                       "stats": port_data_dict}

        # send the processed data in json form
        send_data(json.dumps(result_dict)+"\n", args.output_host)


def count_host_stats(flow_json):
    """
    TODO

    :type flow_json: Initialized spark streaming context, windowed, json_loaded.
    """
    # TODO: placeholder for host profiling app based on windowed info from host_statistics applications
    # TODO: in the selected big window (supposedly 24h) is suggested to compute
    # * average number of communication partners for given hosts
    # * distinct ports for host, sorted by the frequency of activity
    # * frequency of communication times for host
    # * probability of the host being the server/client machine/network infrastructure based on
    # the distance of the selected attributes' values to mean values of the selected categories
    # flow_json.pprint(200)

    # Filter flows with required data, in a given address range
    flow_with_keys = flow_json.filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys()) and
                                                       ("ipfix.destinationTransportPort" in json_rdd.keys()) and
                                                       (ipaddress.ip_address(json_rdd["ipfix.sourceIPv4Address"]) in network_filter)
                                      )
    # Set window and slide duration for flows analysis
    flow_with_keys_windowed = flow_with_keys.window(window_duration, window_slide)

    return None


if __name__ == "__main__":
    # Prepare arguments parser (automatically creates -h argument).
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-net", "--network_range", help="network range to watch", type=str, required=True)
    parser.add_argument("-n", "--top_n", help="max. number of ports for a host to retrieve", type=int, required=False)

    # Parse arguments.
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = 10  # Analysis window duration (10 seconds)
    window_slide = 10  # Slide interval of the analysis window (10 seconds)
    # Filter for network for detection (regex filtering), e.g. "10\.10\..+"
    network_filter = ipaddress.ip_network(unicode(args.network_range, "utf8"))

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    input_stream_json = input_stream.map(lambda x: json.loads(x[1]))

    # Process data to the defined function.
    host_statistics = count_host_stats(input_stream_json)

    # Transform computed statistics into desired json format and send it to output_host as given in -oh input param
    stats_json = host_statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap()))

    # Start input data processing
    ssc.start()
    ssc.awaitTermination()
