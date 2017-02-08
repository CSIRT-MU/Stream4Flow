# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016  Michal Stefanik <stefanik.m@mail.muni.cz>, Milan Cermak <cermak@ics.muni.cz>
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
Description: Detection method to unveil the Reflected DoS/DDoS attacks relying on expectation of that a victim sends
responses significantly greater than a size of the requests. The method is aimed to protect DNS known servers in local
network infrastructure (only DNS traffic on UDP is considered)

Usage:
  detection_reflectddos.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -dns <comma separated list of DNS servers IP addresses>

  To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
  you can run the example
    $ /home/spark/applications/run-application.sh  /home/spark/applications/detection/reflected_ddos/spark/detection_reflectddos.py
    -iz producer:2181 -it ipfix.entry -oh consumer:20101 -dns "10.10.0.1,10.10.0.2

"""

import sys  # Common system functions
import os  # Common operating system functions
import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
import socket  # Socket interface

from termcolor import cprint  # Colors in the console output

from pyspark import SparkContext  # Spark API
from pyspark.streaming import StreamingContext  # Spark streaming API
from pyspark.streaming.kafka import KafkaUtils  # Spark streaming Kafka receiver


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
    except socket.error:
        cprint("[warning] Unable to connect to host " + output_host, "blue")
    finally:
        sock.close()


def print_and_send(rdd, output_host):
    """
    Transform given computation results into the JSON format and send them to the specified host.

    JSON format:
        {"@type": "detection.reflectdos", "@sub_type": "DNS", "data_received": <volume of recieved data>,
         "data_sent": <volume of sent data>, "outgoing_connections": <number of outgoing connections>,
         "src_ipv4": <list of attack targets>, "dns_ip": <IP address of attacked DNS server>}
    
    :param rdd: map of detected dns servers
    :param output_host: results receiver in the "hostname:port" format

    """  

    results = ""
    dns_transfers = rdd.collectAsMap()

    print("size of detected transfers: %s" % len(dns_transfers))

    # Generate JSON response for each aggregated rdd
    for source_dns, stats in dns_transfers.iteritems():

        new_entry = {"@type": "detection.reflectdos",
                     "@sub_type": "DNS",
                     "data_received": stats[0],
                     "data_sent": stats[1],
                     "outgoing_connections": stats[3],
                     "src_ipv4": source_dns[0],
                     "dns_ip": source_dns[1]}
        results += ("%s\n" % json.dumps(new_entry))

    # Print results to stdout
    print("sending json: \n%s" % results)

    # Send results to the given host.
    send_data(results, output_host)


def inspect_reflectdos(stream_data):
    """
    Main function to count incoming and outgoing volumes of flows for given DNS servers
    filter the transfers with I/O ratio overreaching the given threshold
    and send json log of suspicious activity onwards

    :type stream_data: Initialized spark streaming context.
    """

    # Filters only the data with known source and destination IP address on UDP protocol (=17) on DNS port (=53)
    flow_with_ips = stream_data.filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys() and
                                                         "ipfix.destinationIPv4Address" in json_rdd.keys() and
                                                         "ipfix.protocolIdentifier" in json_rdd.keys() and
                                                         "ipfix.destinationTransportPort" in json_rdd.keys() and
                                                         json_rdd["ipfix.protocolIdentifier"] == 17 and
                                                         json_rdd["ipfix.destinationTransportPort"] == 53))

    # Filter incoming communication to the specified DNS server and get bytes volume
    incoming_dns_flows_stats = flow_with_ips\
        .filter(lambda json_rdd: json_rdd["ipfix.destinationIPv4Address"] in dns_servers)\
        .map(lambda json_rdd: ((json_rdd["ipfix.sourceIPv4Address"], json_rdd["ipfix.destinationIPv4Address"]),
                               (json_rdd["ipfix.octetDeltaCount"], 0, 0, 0))) \

    # Filter outgoing communication from the specified DNS server
    # Get bytes volume, number of packets and flows
    outgoing_dns_flows_stats = flow_with_ips \
        .filter(lambda json_rdd: json_rdd["ipfix.sourceIPv4Address"] in dns_servers) \
        .map(lambda json_rdd: ((json_rdd["ipfix.destinationIPv4Address"], json_rdd["ipfix.sourceIPv4Address"]),
                               (0, json_rdd["ipfix.octetDeltaCount"], json_rdd["ipfix.packetDeltaCount"], 1)))

    # Merge incoming and outgoing dns flows
    # DStream format: (Peer IP, DNS server)(incoming bytes, outgoing bytes, outgoing packets, outgoing flows)
    union_dns_flows = incoming_dns_flows_stats.union(outgoing_dns_flows_stats)

    # For each src-dns IP pair, aggregate incoming and outgoing data volume, number of packets and flows
    union_dns_flows_aggregated = union_dns_flows.reduceByKey(lambda actual, update:
                                                             (actual[0] + update[0],
                                                              actual[1] + update[1],
                                                              actual[2] + update[2],
                                                              actual[3] + update[3]))

    # Compare incoming and outgoing transfers volumes and filter only those suspicious
    # (overreaching the specified methods parameters)
    union_dns_flows_filtered = union_dns_flows_aggregated.filter(lambda rdd: float(rdd[1][0]) != 0 and
                                                                 float(rdd[1][1]) / float(rdd[1][0]) > threshold_change and
                                                                 rdd[1][2] > minimal_replies)
    # Control print
    # union_dns_flows_aggregated.pprint()

    # Return DStream with detected flows
    return union_dns_flows_filtered


if __name__ == "__main__":
    # Prepare arguments parser (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oh", "--output_host", help="output hostname:port", type=str, required=True)
    parser.add_argument("-dns", "--dns_servers", help="adreses of DNS servers to watch", type=str, required=True)

    # Parse arguments
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic

    # Method parameters:
    minimal_replies = 10  # Minimal replies sent by a server
    threshold_change = 3  # Threshold for a ration to trigger attack
    dns_servers = args.dns_servers.split(",")  # Comma separated list of monitored DNS servers
    window_duration = 20  # Window duration specification in seconds
    window_slide = 20  # Window slide specification in seconds

    # Print list of monitored DNS servers
    print("Watching DNS servers: %s" % dns_servers)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    flows_json = input_stream.map(lambda x: json.loads(x[1]))

    # Set window and slide duration for flows analysis
    flows_json_windowed = flows_json.window(window_duration, window_slide)

    # Process data to the defined function.
    reflectdos_result = inspect_reflectdos(flows_json_windowed)

    # Process the results of the detection and send them to the specified host
    reflectdos_result.foreachRDD(lambda rdd: print_and_send(rdd, args.output_host))

    # Start input data processing
    ssc.start()
    ssc.awaitTermination()
