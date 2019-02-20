# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2018 Milan Cermak <cermak@ics.muni.cz>, Institute of Computer Science, Masaryk University
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
To run this on the Stream4Flow, you need to receive TLS extended flows by IPFIXCol and make them available via Kafka
topic. Then you can run the example
    $ ./run-application.sh ./tls_classification.py -iz producer:2181 -it ipfix.entry -oz producer:9092
      -ot app.pattern-finder -d tls_classification_dictionary.csv
"""


import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
from termcolor import cprint  # Colors in the console output
import csv

from modules import kafkaIO  # IO operations with kafka topics
import json


def format_classification_dictionary(csv_dictionary):
    """
    Convert loaded CSV classificator to the dictionary with the following format
    {suites:{device_type,operating_system,application,browser}}.

    :param csv_dictionary: CSV object with loaded classification dictionary
    :return: Dictionary with suites as a key and rest columns as a value
    """
    classificator = {}
    for row in csv_dictionary:
        classificator[row["suites"]] = {
            "device_type": row["device_type"] if row["device_type"] != "" else "unknown",
            "operating_system": row["operating_system"] if row["operating_system"] != "" else "unknown",
            "application": row["application"] if row["application"] != "" else "unknown",
            "browser": row["browser"] if row["browser"] != "" else "unknown"
        }
    return classificator


def initialize_tls_classificator(dictionary_file_handler, sparkContext):
    """
    Load given classification dictionary file into the global variable and broadcast it to all Spark workers.

    :param dictionary_file_handler: File handler with opened dictionary file
    :param sparkContext: Initialized Spark context
    """
    csv_dictionary = csv.DictReader(dictionary_file_handler, delimiter=';')
    classificator = format_classification_dictionary(csv_dictionary)
    # Set global variable and broadcast it using sparkContext
    globals()["tls_classificator"] = sparkContext.broadcast(classificator)


def get_tls_classificator():
    """
    Return value of the broadcasted global variable tls_classificator.

    :return: Content of the tls_classificator variable
    """
    global tls_classificator
    return tls_classificator.value


def process_results(data_to_process, producer, output_topic):
    """
    Process analyzed data and modify it into desired output.

    :param data_to_process: analyzed data
    :param producer: Kafka producer
    :param output_topic: Kafka topic through which output is send
    """

    # Here you can format your results output and send it to the kafka topic
    # <-- INSERT YOUR CODE HERE

    # Example of a transformation function that selects values of the dictionary and dumps them as a string
    results_output = '\n'.join(map(json.dumps, data_to_process.values()))

    # Send desired output to the output_topic
    #kafkaIO.send_data_to_kafka(results_output, producer, output_topic)


def format_cipher_suites(suites):
    """
    Correct suites formatting to fit the classification dictionary.

    :param suites: TLS cipher suites string produced by IPFIXcol
    :return: TLS cipher suites list in the "0x...,0x..." format.
    """
    cipher_suites = ""

    # Remove "0x" att the beginning of the suites string
    if suites[:2] == "0x":
        suites = suites[2:]

    # Swap each two character pairs (fix of the wrong byte order)
    for i in range(0, len(suites), 4):
        cipher = "0x" + suites[i+2] + suites[i+3] + suites[i] + suites[i+1] + ","
        # Append only if the cipher is not empty
        if cipher != "0x0000,":
            cipher_suites += cipher

    # Return formatted cipher suites without the last coma
    return cipher_suites[:-1]


def translate_cipher_suite(suite):
    classificator = get_tls_classificator()
    if suite in classificator.keys():
        return classificator[suite]
    return None


def parse_classificated(classificated):
    browsers = ("browser" + ";" + classificated["classification"]["browser"], classificated["count"])
    operating_systems = ("os" + ";" + classificated["classification"]["operating_system"], classificated["count"])
    applications = ("application" + ";" + classificated["classification"]["device_type"] + ":" + classificated["classification"]["application"], classificated["count"])
    return [browsers, operating_systems, applications]


def prepare_unknown(unknown):
    count = unknown[1][0] - unknown[1][1]
    browsers = ("browser" + ";" + "unknown", count)
    operating_systems = ("os" + ";" + "unknown", count)
    applications = ("application" + ";" + "unknown:unknown", count)
    return [browsers, operating_systems, applications]


def process_input(input_data):
    """
    Process raw data and do MapReduce operations.

    :param input_data: input data in JSON format to process
    :return: processed data
    """
    # Select flow with ipfix.TLSClientCipherSuites key only
    filtered = input_data.filter(lambda flow_json: "ipfix.TLSClientCipherSuites" in flow_json.keys() and
                                                   flow_json["ipfix.TLSClientCipherSuites"] != "0x00000000000000000000000000000000")

    # Map and reduce cipher suites to get unique cipher suites and their count
    cipher_suites = filtered.map(lambda flow_json: (format_cipher_suites(flow_json["ipfix.TLSClientCipherSuites"]), 1))\
                            .reduceByKey(lambda actual, update: actual + update)

    # Classify cipher suites
    classificated = cipher_suites.map(lambda suites_count: {"count": suites_count[1], "classification": translate_cipher_suite(suites_count[0])})

    # Filter out unrecognized cipher suites
    classificated_filtered = classificated.filter(lambda suites_class: suites_class["classification"] is not None)


    # Get number of unclassified flows
    counted_initial = filtered.count().map(lambda count: ("count", count))


    classification_sums = classificated_filtered.flatMap(lambda suites_class: parse_classificated(suites_class))\
                                                .reduceByKey(lambda actual, update: actual + update)\
                                                .union(counted_initial)
    classification_sums.pprint(20)


    return classificated_filtered


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-m", "--microbatch", help="microbatch duration", type=int, required=False, default=5)
    parser.add_argument("-d", "--dictionary", help="path to the dictionary file", type=argparse.FileType('r'),
                        required=False, default='tls_classification_dictionary.csv')

    # You can add your own arguments here
    # See more at:
    # https://docs.python.org/2.7/library/argparse.html

    # Parse arguments
    args = parser.parse_args()

    # Initialize input stream and parse it into JSON
    sc, ssc, parsed_input_stream = kafkaIO\
        .initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, args.microbatch)

    # Load CSV classificator dictionary file and broadcast it to all workers
    initialize_tls_classificator(args.dictionary, sc)

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
