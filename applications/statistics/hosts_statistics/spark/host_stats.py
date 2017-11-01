# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016  Tomas Jirsik <jirsik@ics.muni.cz>, Michal Stefanik <stefanik dot m@mail.muni.cz>
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
  host_stats.py --iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -ln <CIDR network range> -w <window duration> -m <microbatch>

  To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
  you can run the example
    $ ./run-application.sh ./statistics/hosts_statistics/spark/host_stats.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output -ln "10.0.0.0/24 -w 10 -m 10"

"""

import argparse  # Arguments parser
import ujson as json  # Fast JSON parser

from netaddr import IPNetwork, IPAddress  # Checking if IP is in the network
from modules import kafkaIO  # IO operations with kafka topics

from kafka import KafkaProducer  # Kafka Python client


def map_tcp_flags(bitmap):
    """
    Maps text names of tcp flags to values in bitmap

    :param bitmap: array[8]
    :return: dictionary with keynames as names of the flags
    """

    result = {}
    result["FIN"] = bitmap[7]
    result["SYN"] = bitmap[6]
    result["RST"] = bitmap[5]
    result["PSH"] = bitmap[4]
    result["ACK"] = bitmap[3]
    result["URG"] = bitmap[2]
    result["ECE"] = bitmap[1]
    result["CRW"] = bitmap[0]
    return result


def process_results(data_to_process, producer, output_topic):

    """
    Transform given computation results into the JSON format and send them to the specified kafka instance.

    JSON format:
    {"src_ip":"<host src IPv4 address>",
     "@type":"host_stats",
     "stats":{
        "total":{"packets":<# of packets>,"bytes":# of bytes,"flow":<# of flows>},
        "avg_flow_duration":<avg. duration of flows>,
        "dport_count":<number of distinct destination ports>,
        "peer_number":<number of distinct communication peers>
        "tcp_flags":{"FIN":<number of FIN flags>, "SYN":<number of SYN flags>, ...}
        }
    }

    :param data_to_process: Map in following format  (src IP , (('total_stats', <# of flows>, <# of packets>, <# of bytes>),
                                                         ('peer_number', <# of peers>),
                                                         ('dport_count',  <# number of distinct ports>),
                                                         ('avg_flow_duration',<average flow duration>),
                                                         ('tcp_flags',<bitarray of tcpflags>)
                                                        )
                                               )
    :param producer : Initialized Kafka producer
    :param output_topic: Name of the output topic for Kafka
    :return:
    """

    results = ""

    for ip, data in data_to_process.iteritems():

        total_dict = {}
        stats_dict = {"total": total_dict}
        result_dict = {"@type": "host_stats", "src_ip": ip, "stats": stats_dict}

        # Process total stats

        total_dict["flow"] = data[statistics_position["total_stats"]][total_stats_position["total_flows"]]
        total_dict["packets"] = data[statistics_position["total_stats"]][total_stats_position["total_packets"]]
        total_dict["bytes"] = data[statistics_position["total_stats"]][total_stats_position["total_bytes"]]
        # TODO: There is no total_stats_position. If you need this set this as a function or pass it as an argument.

        # Process peer_number stats
        stats_dict["peer_number"] = data[statistics_position["peer_number"]][peer_number_position["peer_number"]]

        # Process dport_number stats
        stats_dict["dport_count"] = data[statistics_position["dport_count"]][dport_count_position["dport_number"]]

        # Process average flow duration stats
        stats_dict["avg_flow_duration"] = data[statistics_position["average_flow_duration"]][
            avg_flow_duration_postion["avg_duration"]]

        # Process tcp flags sums
        if data[statistics_position["tcp_flags"]]:  # if exists statistics for a given host
            stats_dict["tcp_flags"] = map_tcp_flags(
                data[statistics_position["tcp_flags"]][tcp_flags_position["tcp_flags_array"]])

        # send the processed data in json format to the given kafka producer under given topic
        send_to_kafka(json.dumps(result_dict) + "\n", producer, topic)

    # test print
    # print(results)


    # Send desired output to the output_topic
    kafkaIO.send_data_to_kafka(results, producer, output_topic)


def process_input(input_data,window_duration, window_slide, network_filter):
    """
    Process raw data and do MapReduce operations.
    :param input_data: input data in JSON format to process
    :return: processed data
    """
    # Filter flows with relevant keys
    flow_with_keys = input_data\
        .filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys()) and
                                 ("ipfix.destinationTransportPort" in json_rdd.keys()) and
                                 ("ipfix.protocolIdentifier" in json_rdd.keys()) and
                                 (IPAddress(json_rdd["ipfix.sourceIPv4Address"]) in IPNetwork(network_filter))
               )

    # Set window and slide duration for flows analysis
    flow_with_keys_windowed = flow_with_keys.window(window_duration, window_slide)

    # Compute basic hosts statistics - number of flows, packets, bytes sent by a host
    flow_ip_total_stats_no_window = flow_with_keys\
        .map(lambda json_rdd: (
            json_rdd["ipfix.sourceIPv4Address"],
            ("total_stats", 1, json_rdd["ipfix.packetDeltaCount"], json_rdd["ipfix.octetDeltaCount"]))
        )\
        .reduceByKey(lambda actual, update: (
            actual[total_stats_position["type"]],
            actual[total_stats_position["total_flows"]] + update[total_stats_position["total_flows"]],
            actual[total_stats_position["total_packets"]] + update[total_stats_position["total_packets"]],
            actual[total_stats_position["total_bytes"]] + update[total_stats_position["total_bytes"]]
        ))
    #TODO in map transfer value to dict {"total_stats":( 1, json_rdd["ipfix.packetDeltaCount"], json_rdd["ipfix.octetDeltaCount")}

    flow_ip_total_stats = flow_ip_total_stats_no_window\
        .window(window_duration, window_slide) \
        .reduceByKey(lambda actual, update: (
            actual[total_stats_position["type"]],
            actual[total_stats_position["total_flows"]] + update[total_stats_position["total_flows"]],
            actual[total_stats_position["total_packets"]] + update[total_stats_position["total_packets"]],
            actual[total_stats_position["total_bytes"]] + update[total_stats_position["total_bytes"]]
        ))

    # Compute a number of distinct communication peers with a host
    flow_communicating_pairs_no_window = flow_with_keys\
        .map(lambda json_rdd: (
                (json_rdd["ipfix.sourceIPv4Address"],
                 json_rdd["ipfix.sourceIPv4Address"] + "-" + json_rdd["ipfix.destinationIPv4Address"]), 1)
            ) \
        .reduceByKey(lambda actual, update: actual)

    flow_communicating_pairs = flow_communicating_pairs_no_window\
        .window(window_duration, window_slide) \
        .reduceByKey(lambda actual, update: actual + update) \
        .map(lambda json_rdd: (json_rdd[0][0], ("peer_number", 1))) \
        .reduceByKey(lambda actual, update: (
            actual[0],
            actual[1] + update[1]))

    # Compute a number of distinct destination ports for each host
    flow_dst_port_count_no_window = flow_with_keys_windowed\
        .map(lambda json_rdd: (
            (json_rdd["ipfix.sourceIPv4Address"],
             json_rdd["ipfix.sourceIPv4Address"] + "-" + str(json_rdd["ipfix.destinationTransportPort"])), 1)
             ) \
        .reduceByKey(lambda actual, update: actual)

    flow_dst_port_count = flow_dst_port_count_no_window.window(window_duration, window_slide) \
                                                       .reduceByKey(lambda actual, update: actual) \
                                                       .map(lambda json_rdd: (json_rdd[0][0], ("dport_count", 1))) \
                                                       .reduceByKey(lambda actual, update: (
                                                            actual[0],
                                                            actual[1] + update[1]))

    # Compute an average duration of a flow in seconds for each host
    flow_average_duration = flow_with_keys_windowed\
        .map(lambda json_rdd: (
            json_rdd["ipfix.sourceIPv4Address"],
            (1, (json_rdd["ipfix.flowEndMilliseconds"] - json_rdd["ipfix.flowStartMilliseconds"]))
            )
        ) \
        .reduceByKey(lambda actual, update: (
            actual[0] + update[0],  # number of flow
            actual[1] + update[1]  # sum of flow duration
            )
        ) \
        .map(lambda json_rdd: (
            json_rdd[0],("avg_flow_duration", (json_rdd[1][1] / float(1000)) / float(json_rdd[1][0])))
        )  # compute the average

    # Compute TCP Flags
    # Filter out TCP traffic
    flow_tcp = flow_with_keys.filter(lambda json_rdd: (json_rdd["ipfix.protocolIdentifier"] == 6))
    # Compute flags statistics
    flow_tcp_flags_no_window = flow_tcp\
        .map(lambda json_rdd: (
            json_rdd["ipfix.sourceIPv4Address"],
            ("tcp_flags",  map(int, list('{0:08b}'.format(json_rdd["ipfix.tcpControlBits"]))))
            )
        ) \
        .reduceByKey(lambda actual, update: (actual[0], [x + y for x, y in zip(actual[1], update[1])]))

    flow_tcp_flags = flow_tcp_flags_no_window.window(window_duration, window_slide) \
                                             .reduceByKey(lambda actual, update: (
                                                            actual[0],[x + y for x, y in zip(actual[1], update[1])]
                                             ))

    # Join the DStreams to be able to process all in one foreachRDD
    # The structure of DSstream is
    # (src IP, ((((('total_stats', <# of flows>, <# of packets>, <# of bytes>), ('peer_number', <# of peers>)),
    # ('dport_count', <# number of distinct ports>)),
    # ('avg_flow_duration', <average flow duration>)),
    # ('tcp_flags',<bitarray of tcpflags>))
    # )
    join_stream = flow_ip_total_stats.fullOuterJoin(flow_communicating_pairs) \
                                     .fullOuterJoin(flow_dst_port_count) \
                                     .fullOuterJoin(flow_average_duration) \
                                     .fullOuterJoin(flow_tcp_flags)
    # TODO: Consider to use union instead of join (the application could be faster). For reduce use append - linked to the changes of map approach

    # Transform join_stream to parsable Dstream
    # (src IP , (('total_stats', <# of flows>, <# of packets>, <# of bytes>), ('peer_number', <# of peers>),
    # ('dport_count',  <# number of distinct ports>), ('avg_flow_duration',<average flow duration>),("tcp_flags",<bitarray of tcpflags>)))
    parsable_join_stream = join_stream.map(lambda json_rdd: (json_rdd[0], (
        json_rdd[1][0][0][0][0], json_rdd[1][0][0][0][1], json_rdd[1][0][0][1], json_rdd[1][0][1], json_rdd[1][1])))

    return parsable_join_stream


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-ln", "--local_network", help="network range to watch", type=str, required=True)
    parser.add_argument("-w", "--window", help="analysis window duration (in seconds)", type=int, required=False, default=10)
    parser.add_argument("-m", "--microbatch", help="microbatch (in seconds)", type=int, required=False, default=10)




    # You can add your own arguments here
    # See more at:
    # https://docs.python.org/2.7/library/argparse.html

    # Parse arguments
    args = parser.parse_args()

    # Position of statistics in DStream.
    #  Overall structure of a record
    statistics_position = {"total_stats": 0, "peer_number": 1, "dport_count": 2, "average_flow_duration": 3,
                           "tcp_flags": 4}
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
    # TODO: EGH :( Transform it to the function.

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO.initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic,
                                                                         args.microbatch)

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream, args.window, args.window, args.local_network)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
