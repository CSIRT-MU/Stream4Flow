# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016  Tomas Jirsik <jirsik@ics.muni.cz>
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
for each host each window are following:
    - sum of flows, packets and bytes
    - average duration of flows
    - number of distinct destination ports
    - number of distinct communication peers

Usage:
  detection_ddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -net <regex for network range>

  To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
  you can run the example
    $ ./run-application.sh ./host_statistics/host_statistics.py -iz producer:2181 -it ipfix.entry -oh consumer:20101 -net "10\.10\..+"

"""

import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface
import re  # Parsing and matching regular expression
import time  # Time handling

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver

def map_tcp_flags(bitmap):
    """
    Maps text names of tcp flags to values in bitmap

    :param bitmap: array[8]
    :return: dictionary with keynames as names of the flags
    """
    result=dict()
    result["FIN"] = bitmap[7]
    result["SYN"] = bitmap[6]
    result["RST"] = bitmap[5]
    result["PSH"] = bitmap[4]
    result["ACK"] = bitmap[3]
    result["URG"] = bitmap[2]
    result["ECE"] = bitmap[1]
    result["CRW"] = bitmap[0]
    return result




def decimal_to_bitmap(decimal):
    """
    Trasfers decimal number into a 8bit bitmap

    :param decimal: decimal number
    :return: bitmap of 8bits
    """
    bitmap = map(int, list('{0:08b}'.format(decimal)))
    return bitmap

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

        # Print message of sent
        now = time.strftime("%c")
        print("Data sent at: %s" % now)

    except socket.error:
        cprint("[warning] Unable to connect to host " + output_host, "blue")
    finally:
        sock.close()


def process_results(json_rrd, output_host):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
    {"src_ipv4":"<host src IPv4 address>",
     "@type":"host_stats",
     "stats":{
        "total":{"packets":<# of packets>,"bytes":# of bytes,"flow":<# of flows>},
        "avg_flow_duration":<avg. duration of flows>,
        "dport_count":<number of distinct destination ports>,
        "peer_number":<number of distinct communication peers>
        }
    }

    :param json_rrd: Map in following format  (src IP , (('total_stats', <# of flows>, <# of packets>, <# of bytes>),
                                                         ('peer_number', <# of peers>),
                                                         ('dport_count',  <# number of distinct ports>),
                                                         ('avg_flow_duration',<average flow duration>)
                                                        )
                                               )
    :param output_host: results receiver in the "hostname:port" format
    :return:
    """

    results = ""

    for ip, data in json_rrd.iteritems():
        #print(data)
        total_dict = {}
        stats_dict = {"total": total_dict}
        result_dict = {"@type": "host_stats", "src_ipv4": ip, "stats": stats_dict}

        # Process total stats
        total_dict["flow"] = data[statistics_position["total_stats"]][total_stats_position["total_flows"]]
        total_dict["packets"] = data[statistics_position["total_stats"]][total_stats_position["total_packets"]]
        total_dict["bytes"] = data[statistics_position["total_stats"]][total_stats_position["total_bytes"]]

        # Process peer_number stats
        stats_dict["peer_number"] = data[statistics_position["peer_number"]][peer_number_position["peer_number"]]

        # Process dport_number stats
        stats_dict["dport_count"] = data[statistics_position["dport_count"]][dport_count_position["dport_number"]]

        # Process average flow duration stats
        stats_dict["avg_flow_duration"] = data[statistics_position["average_flow_duration"]][avg_flow_duration_postion["avg_duration"]]

        # Process tcp flags sums
        if data[statistics_position["tcp_flags"]]: # if exists statistics for a given host
            stats_dict["tcp_flags"] = map_tcp_flags(data[statistics_position["tcp_flags"]][tcp_flags_position["tcp_flags_array"]])

        results += json.dumps(result_dict) + "\n"

    # Sent results to a given socket
    # print(results)  # Controll print
    send_data(results, output_host)


def count_host_stats(flow_json):
    """
    Main function to count TOP N statistics for flow data in the JSON format.

    :type flow_json: Initialized spark streaming context, windowed, json_loaded.
    """

    # Create regex for monitored network
    local_ip_pattern = re.compile(network_filter)

    # Filter flows with relevant keys
    flow_with_keys = flow_json.filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys()) and
                                                       ("ipfix.destinationTransportPort" in json_rdd.keys()) and
                                                       ("ipfix.flowStartMilliseconds" in json_rdd.keys()) and
                                                       ("ipfix.flowEndMilliseconds" in json_rdd.keys()) and
                                                       ("ipfix.protocolIdentifier" in json_rdd.keys()) and
                                                       (re.match(local_ip_pattern, json_rdd["ipfix.sourceIPv4Address"]))
                                      )

    # Compute basic hosts statistics - number of flows, packets, bytes sent by a host
    flow_ip_total_stats = flow_with_keys.map(lambda json_rdd: (json_rdd["ipfix.sourceIPv4Address"], ("total_stats", 1, json_rdd["ipfix.packetDeltaCount"], json_rdd["ipfix.octetDeltaCount"])))\
                                        .reduceByKey(lambda actual, update: (
                                            actual[total_stats_position["type"]],
                                            actual[total_stats_position["total_flows"]] + update[total_stats_position["total_flows"]],
                                            actual[total_stats_position["total_packets"]] + update[total_stats_position["total_packets"]],
                                            actual[total_stats_position["total_bytes"]] + update[total_stats_position["total_bytes"]]
                                                    ))

    # Compute a number of distinct communication peers with a host
    flow_communicating_pairs = flow_with_keys.map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.sourceIPv4Address"]+"-"+json_rdd["ipfix.destinationIPv4Address"]), 1))\
                                             .reduceByKey(lambda actual, update: actual+update)\
                                             .map(lambda json_rdd: (json_rdd[0][0], ("peer_number", 1)))\
                                             .reduceByKey(lambda actual, update: (
                                                      actual[0],
                                                      actual[1] + update[1]))

    # Compute a number of distinct destination ports for each host
    flow_dst_port_count = flow_with_keys.map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.sourceIPv4Address"]+"-"+str(json_rdd["ipfix.destinationTransportPort"])), 1))\
                                        .reduceByKey(lambda actual, update: actual+update)\
                                        .map(lambda json_rdd: (json_rdd[0][0], ("dport_count", 1)))\
                                        .reduceByKey(lambda actual, update: (
                                               actual[0],
                                               actual[1] + update[1]))

    # Compute an average duration of a flow in seconds for each host
    flow_average_duration = flow_with_keys.map(lambda json_rdd: (json_rdd["ipfix.sourceIPv4Address"], (1, (json_rdd["ipfix.flowEndMilliseconds"]-json_rdd["ipfix.flowStartMilliseconds"]))))\
                                          .reduceByKey(lambda actual, update: (
                                                        actual[0] + update[0],  # number of flow
                                                        actual[1] + update[1]  # sum of flow duration
                                                        ))\
                                          .map(lambda json_rdd: (json_rdd[0], ("avg_flow_duration", (json_rdd[1][1]/float(1000))/float(json_rdd[1][0]))))  # compute the average

    # Compute TCP Flags
    ## Filter out TCP traffic
    flow_tcp = flow_with_keys.filter(lambda json_rdd: (json_rdd["ipfix.protocolIdentifier"] == 6))
    ## Compute flags statistics
    flow_tcp_flags = flow_tcp.map(lambda json_rdd: (json_rdd["ipfix.sourceIPv4Address"],("tcp_flags", decimal_to_bitmap(json_rdd["ipfix.tcpControlBits"]))))\
                             .reduceByKey(lambda actual,update: (
                               actual[0],
                               [x + y for x, y in zip(actual[1], update[1])]
    ))

    # Compute a protocol distribution for each host
    # flow_protocol_stats_unmapped = flow_with_keys.map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.protocolIdentifier"]), ("protocol_stats", 1, json_rdd["ipfix.packetDeltaCount"], json_rdd["ipfix.octetDeltaCount"]))) \
    #                                     .reduceByKey(lambda actual, update: (
    #                                     actual[total_stats_position["type"]],
    #                                     actual[total_stats_position["total_flows"]] + update[total_stats_position["total_flows"]],
    #                                     actual[total_stats_position["total_packets"]] + update[total_stats_position["total_packets"]],
    #                                     actual[total_stats_position["total_bytes"]] + update[total_stats_position["total_bytes"]]
    #                                 ))
    # flow_protocol_stats_unmapped.pprint(20)
    # flow_protocol_stats = flow_protocol_stats_unmapped.map(lambda json_rrd: (json_rrd[0][0],(json_rrd[1][0],(json_rrd[0][1], json_rrd[1][1], json_rrd[1][2], json_rrd[1][3]))))\
    #                                                   .reduceByKey(lambda actual, update: (
    #                                                   actual[protocol_stats_position["type"]],
    #                                                   actual.add(update[0])
    #                                             ))
    # flow_protocol_stats.pprint(20)

    # Union the DStreams to be able to process all in one foreachRDD
    # The structure of DSstream is
    # (src IP, (((('total_stats', <# of flows>, <# of packets>, <# of bytes>), ('peer_number', <# of peers>)),
    # ('dport_count', <# number of distinct ports>)),
    # ('avg_flow_duration', <average flow duration>))
    # )
    union_stream = flow_ip_total_stats.fullOuterJoin(flow_communicating_pairs)\
                                    .fullOuterJoin(flow_dst_port_count)\
                                    .fullOuterJoin(flow_average_duration)\
                                    .fullOuterJoin(flow_tcp_flags)

    # Transform union_stream to parsable Dstream
    # (src IP , (('total_stats', <# of flows>, <# of packets>, <# of bytes>), ('peer_number', <# of peers>),
    # ('dport_count',  <# number of distinct ports>), ('avg_flow_duration',<average flow duration>),("tcp_flags",<bitarray of tcpflags>)))
    parsable_union_stream = union_stream.map(lambda json_rdd: (json_rdd[0], (json_rdd[1][0][0][0][0], json_rdd[1][0][0][0][1], json_rdd[1][0][0][1], json_rdd[1][0][1], json_rdd[1][1])))

    return parsable_union_stream


if __name__ == "__main__":
    # Prepare arguments parser (automatically creates -h argument).
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-net", "--network_range", help="network range to watch", type=str, required=True)

    # Parse arguments.
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = 10  # Analysis window duration (10 seconds)
    window_slide = 10  # Slide interval of the analysis window (10 seconds)
    network_filter = args.network_range  # Filter for network for detection (regex filtering), e.g. "10\.10\..+"

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Position of statistics in DStream.
    # Overall structure of a record
    statistics_position = {"total_stats": 0, "peer_number": 1, "dport_count": 2, "average_flow_duration": 3, "tcp_flags": 4}
    # Structure of basic characteristics
    total_stats_position = {"type": 0, "total_flows": 1, "total_packets": 2, "total_bytes": 3}
    # Structure of peer number count characteristics
    peer_number_position = {"type": 0, "peer_number": 1}
    # Structure of destination port count characteristics
    dport_count_position = {"type": 0, "dport_number": 1}
    # Structure of average flow duration characteristics
    avg_flow_duration_postion = {"type": 0, "avg_duration": 1}
    # Structure of protocol characteristics
    tcp_flags_position = {"type": 0, "tcp_flags_array": 1}

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name, {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    input_stream_json = input_stream.map(lambda x: json.loads(x[1]))

    # Set window and slide duration for flows analysis
    input_stream_json_windowed = input_stream_json.window(window_duration, window_slide)

    # Process data to the defined function.
    host_statistics = count_host_stats(input_stream_json_windowed)

    # Process computed statistics and send them to the specified host
    host_statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), args.output_host))

    # Start input data processing
    ssc.start()
    ssc.awaitTermination()


