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
Template application for creating new applications using provided module and Spark operations, with a possibility of
adding more advanced modules. This template simply resends one row of data from given input topic to defined output topic.

Usage:
    application_template.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic>
    -oz <output-zookeeper-hostname>:<output-zookeeper-port> -ot <output-topic>

To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
you can run the application as follows:
    $ ./run-application.sh ./application_template.py -iz producer:2181 -it ipfix.entry -oz producer:9092 -ot results.output
"""


import argparse  # Arguments parser
import ujson as json  # Fast JSON parser
from termcolor import cprint  # Colors in the console output

from modules import kafkaIO  # IO operations with kafka topics


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
    kafkaIO.send_data_to_kafka(results_output, producer, output_topic)


def process_input(input_data):
    """
    Process raw data and do MapReduce operations.

    :param input_data: input data in JSON format to process
    :return: processed data
    """
    # Here you can process input stream with MapReduce operations
    # <-- INSERT YOUR CODE HERE

    # Example of the map function that transform all JSONs into the key-value pair with the JSON as value and static key
    modified_input = input_data.map(lambda json: (1, json))

    return modified_input


if __name__ == "__main__":
    # Define application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)

    # You can add your own arguments here
    # See more at:
    # https://docs.python.org/2.7/library/argparse.html

    # Parse arguments
    args = parser.parse_args()

    # Set microbatch duration to 1 second
    microbatch_duration = 1

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO.initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, microbatch_duration)

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
