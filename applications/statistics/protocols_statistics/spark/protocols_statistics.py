# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016 Milan Cermak <cermak@ics.muni.cz>, Institute of Computer Science, Masaryk University
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
Counts number of flows, packets, and bytes for TCP, UDP, and other flows received from Kafka every 10 seconds.

Usage:
    protocols_statistics.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic> -m <microbatch-duration>
    -w <window-duration>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
you can run the application as follows:
    $ ~/applications//run-application.sh ./statistics/protocols_statistics/spark/protocols_statistics.py -iz producer:2181 
    -it ipfix.entry -oz producer:9092 -ot results.output
"""


import argparse  # Arguments parser
from termcolor import cprint  # Colors in the console output

from modules import kafkaIO  # IO operations with kafka topics


def process_results(data_to_process, producer, output_topic):
    """
    Process analyzed data and modify it into desired output.

    JSON format:
    {
        "@type": "protocols_statistics",
        "protocol" : <protocol>,
        "flows": <#flows>,
        "packets": <#packets>,
        "bytes": <#bytes>
    }

    :param data_to_process: analyzed data
    :param producer: Kafka producer
    :param output_topic: Kafka topic through which output is send
    """

    # Transform given results into the JSON
    results_output = ""
    for key, value in data_to_process.iteritems():
        results_output += "{\"@type\": \"protocols_statistics\", \"protocol\": \"" + key + \
                          "\", \"flows\": " + str(value[0]) + ", \"packets\": " + str(value[1]) + \
                          ", \"bytes\": " + str(value[2]) + "}\n"

    # Check if there are any results
    if results_output:
        # Print results to standard output
        cprint(results_output)

        # Send desired output to the output_topic
        kafkaIO.send_data_to_kafka(results_output, producer, output_topic)


def get_protocol_name(protocol_identifier):
    """
    Returns protocol name for the given identifier.

    :param protocol_identifier: Number representing the protocol.
    :return: string "tcp" if protocol_identifier is 6, "udp" if protocol_identifier is 17, and "other" otherwise
    """

    # Check identifier and return corresponfing string
    if protocol_identifier == 6:
        return "tcp"
    elif protocol_identifier == 17:
        return "udp"
    else:
        return "other"


def process_input(input_data, window_duration):
    """
    Count number of transferred flows, packets, and bytes of TCP, UDP, and other protocols using
    Spark Streaming functions.

    :param input_data: input data in JSON format to process
    :param window_duration: duration of the time window for statistics count (in seconds)
    :return: union DStream of UDP, TCP, and other protocols statistics
    """
    # Check required flow keys
    flows_stream_checked = input_data.filter(lambda flow_json: ("ipfix.protocolIdentifier" in flow_json.keys()))

    # Set protocol name as a key and map number of flows, packets, and bytes
    flows_mapped = flows_stream_checked.map(lambda flow_json: (get_protocol_name(flow_json["ipfix.protocolIdentifier"]),
                                                               (1, flow_json["ipfix.packetDeltaCount"],
                                                                flow_json["ipfix.octetDeltaCount"])))

    # Reduce mapped flows to get statistics for smallest analysis interval and reduce volume of processed data
    flows_reduced = flows_mapped.reduceByKey(lambda actual, update: (
                                                 actual[0] + update[0],
                                                 actual[1] + update[1],
                                                 actual[2] + update[2]
                                             ))

    # Set time window and compute statistics over the window using the same reduce
    flows_statistics = flows_reduced.window(window_duration, window_duration)\
                                    .reduceByKey(lambda actual, update: (
                                                     actual[0] + update[0],
                                                     actual[1] + update[1],
                                                     actual[2] + update[2]
                                                 ))

    # Return computed statistics
    return flows_statistics


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-m", "--microbatch", help="microbatch duration", type=int, required=False, default=5)
    parser.add_argument("-w", "--window", help="analysis window duration", type=int, required=False, default=10)

    # Parse arguments
    args = parser.parse_args()

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream, args.window)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
