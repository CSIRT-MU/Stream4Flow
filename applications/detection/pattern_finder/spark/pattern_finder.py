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
To run this on the Stream4Flow, you need to receive flows by IPFIXCol and make them available via Kafka topic. Then
you can run the example
    $ ./run-application.sh ./pattern_finder.py -iz producer:2181 -it ipfix.entry -oz producer:9092
      -ot app.pattern-finder -c configuration.yml
"""

import sys                                # Common system functions
import argparse                           # Arguments parser
import ujson as json                      # Fast JSON parser
import yaml                               # YAML configuration parser
import importlib                          # Allows dynamic import of modules
from termcolor import cprint              # Colors in the console output
from netaddr import IPAddress, IPNetwork  # IP address handling
import operator                           # Fast mathematical operators
from datetime import datetime             # Datetime operations
# Stream4Flow modules
from modules import kafkaIO               # IO operations with kafka topics


# Global variable for a checking if obtained result has been already reported
reported_detections = {}
# Global variable defining last clearing of reported_detections
last_cleaned = datetime.now()


def flow_filter(flow, configuration):
    """
    Checks if given flow record should be filtered based on given configuration.

    :param flow: flow record in JSON format to be filtered
    :param configuration: loaded application configuration
    :return: False if record should be filtered, True otherwise (or if a filter is not set)
    """
    filter_result = True
    for filter in configuration['filter']:
        internal_state = False
        for element_name in filter['element_names']:
            try:
                if filter['type'] == 'ip':
                    ip_address = IPAddress(flow[element_name])
                    for value in filter['values']:
                        internal_state = internal_state or (ip_address in IPNetwork(value))
                elif filter['type'] == 'exists':
                    internal_state = internal_state or (element_name in flow.keys())
                elif filter['type'] == 'int':
                    internal_state = internal_state or (flow[element_name] in filter['values'])
                elif filter['type'] == 'ge':
                    internal_state = internal_state or (flow[element_name] >= filter['value'][0])
                elif filter['type'] == 'nin':
                    internal_state = internal_state or (flow[element_name] not in filter['values'])
                elif filter['type'] == 'lt':
                    internal_state = internal_state or (flow[element_name] < filter['value'][0])
                elif filter['type'] == 'le':
                    internal_state = internal_state or (flow[element_name] <= filter['value'][0])
                elif filter['type'] == 'eq':
                    internal_state = internal_state or (flow[element_name] == filter['value'][0])
                elif filter['type'] == 'ne':
                    internal_state = internal_state or (flow[element_name] != filter['value'][0])
                elif filter['type'] == 'ge':
                    internal_state = internal_state or (flow[element_name] >= filter['value'][0])
                elif filter['type'] == 'gt':
                    internal_state = internal_state or (flow[element_name] > filter['value'][0])
                else:
                    cprint('[warning] Unknown filter type: ' + filter['type'] + '.', 'blue')
                    internal_state = internal_state or True
            except KeyError:
                internal_state = internal_state or False
        filter_result = filter_result and internal_state
        if not filter_result:
            break
    return filter_result


def get_flow_vector_value(flow, configuration, vector_definition_functions):
    """
    Extracts values from the flow record based on the configuration.

    :param flow: flow record in JSON format
    :param configuration: loaded application configuration
    :param vector_definition_functions: loaded functions for a vector value definition
    :return: Array of selected flow record values
    """
    flow_values = []
    # Generate a value and append it to the flow_values
    for configuration_value in configuration['vectors']['values']:
        if configuration_value['type'] == 'element':
            flow_values.append(flow[configuration_value['element']])
        elif configuration_value['type'] == 'direct':
            flow_values.append(configuration_value['value'])
        elif configuration_value['type'] == 'operation':
            # Get operator function specified in the configuration
            function = getattr(operator, configuration_value['operator'])
            value = flow[configuration_value['elements'][0]]
            # Apply selected operation to all elements in the configuration
            for element_position in range(1, len(configuration_value['elements'])):
                value = float(function(value, flow[configuration_value['elements'][element_position]]))
            flow_values.append(value)
        elif configuration_value['type'] == 'module':
            flow_values.append(vector_definition_functions[configuration_value['name']](*[flow[element_name] for element_name in configuration_value['elements']]))
        elif configuration_value['type'] == 'default_function':
            flow_values.append(eval(configuration_value['name'])(*[flow[element_name] for element_name in configuration_value['elements']]))
        else:
            cprint('[warning] Unknown value type: ' + configuration_value['type'] + '.', 'blue')
    return flow_values


def get_flow_output_value(flow, configuration):
    """
    Extracts outputs from the flow record based on the configuration.

    :param flow: flow record in JSON format
    :param configuration: loaded application configuration
    :return: dictionary of outputs defined by name as a key and element as a value
    """
    output = {'simple': {}, 'request': {}, 'response': {}}
    for configuration_output in configuration['output']:
        output[configuration_output['type']][configuration_output['name']] = flow[configuration_output['element']]
    return output


def create_flow_vectors(dstream_flows, configuration, vector_definition_functions):
    """
    Aggregates flows in DStream into vectors specified by configuration.

    :param dstream_flows: DStream of flows in JSON format
    :param configuration: loaded application configuration
    :param vector_definition_functions: loaded functions for a vector value definition
    :return: Updated DStream with created vectors (if no mapping is specified then return the same DStream)
    """
    if configuration['vectors']['key']['type'] == 'simple':
        flows_vectors = dstream_flows.map(lambda flow: ('-'.join(map(lambda element: str(flow[element]), configuration['vectors']['key']['elements'])),
                                                        {
                                                            'vector': get_flow_vector_value(flow, configuration, vector_definition_functions),
                                                            'output': get_flow_output_value(flow, configuration)['simple']
                                                        }))
    elif configuration['vectors']['key']['type'] == 'biflow':
        element_names = configuration['vectors']['key']['elements']

        # Generate key-value pair in following format:
        # ('(src_port:src_ip-dst_port:dst_ip', {'start': flow_start, 'src_port': src_port, 'src_ip': src_ip, 'values': [values]})
        flow_initial_mapping = dstream_flows.map(lambda flow: ('-'.join(sorted([str(flow[element_names['src_port']]) + ':' + flow[element_names['src_ip']], str(flow[element_names['dst_port']]) + ':' + flow[element_names['dst_ip']]])),
                                                               {
                                                                   'flow_start': flow[element_names['flow_start']],
                                                                   'src_port': flow[element_names['src_port']],
                                                                   'src_ip': flow[element_names['src_ip']],
                                                                   'values': get_flow_vector_value(flow, configuration, vector_definition_functions),
                                                                   'output': get_flow_output_value(flow, configuration)
                                                               }))
        # Join generated key-value DStream
        flow_joined = flow_initial_mapping.join(flow_initial_mapping)

        # Remove joining of same flows (same value or IP)
        flow_joined_not_same = flow_joined.filter(lambda flow: (flow[1][0] != flow[1][1]) and (flow[1][0]['src_ip'] != flow[1][1]['src_ip']))

        # Select only closed flows (difference between in and out start of the flow is less then given time difference)
        biflows_close = flow_joined_not_same.filter(lambda biflow: abs(biflow[1][0]['flow_start'] - biflow[1][1]['flow_start']) < configuration['vectors']['key']['time_difference'])

        # Try to select lower ports of the biflow or the first flow if the ports are the same
        biflows_filtered = biflows_close.filter(lambda biflow: (biflow[1][0]['src_port'] >= biflow[1][1]['src_port']))\
                                        .filter(lambda biflow: (biflow[1][0]['flow_start'] < biflow[1][1]['flow_start']) if (biflow[1][0]['src_port'] == biflow[1][1]['src_port']) else True)

        # Transform biflows to the right key and value
        flows_vectors = biflows_filtered.map(lambda biflow: ((biflow[1][0]['src_ip'] + '-' + biflow[1][1]['src_ip']),
                                                             {
                                                                 'vector': {'request': biflow[1][0]['values'], 'response': biflow[1][1]['values']},
                                                                 'output': biflow[1][0]['output']['request'].update(biflow[1][1]['output']['response']) or biflow[1][0]['output']['request']
                                                             }))
    else:
        flows_vectors = dstream_flows
    return flows_vectors


def get_distances_distribution(distances_vector, configuration):
    """
    Transforms distances to the weighted distance distribution based on the given configuration.

    :param distances_vector: dictionary with distances and original vector
    :param configuration: loaded application configuration
    :return: Dictionary with distances transformed to the distance distribution.
    """
    distributions = {}
    for name, distance in distances_vector['distances'].items():
        intervals_length = len((configuration['distance']['distribution'].get(name) or configuration['distance']['distribution']['default']).get('intervals') or configuration['distance']['distribution']['default'].get('intervals'))
        position = intervals_length - 1  # default position set to the last value

        # Find position of the array based on the intervals
        for index, interval_value in enumerate((configuration['distance']['distribution'].get(name) or configuration['distance']['distribution']['default']).get('intervals') or configuration['distance']['distribution']['default'].get('intervals')):
            if distance < interval_value:
                position = index - 1
                break

        # Add weight to the specified position in the distribution
        distribution = [0] * intervals_length
        distribution[position] = ((configuration['distance']['distribution'].get(name) or configuration['distance']['distribution']['default']).get('weights') or configuration['distance']['distribution']['default'].get('weights'))[position]
        distributions[name] = distribution
    return {'distributions': distributions}


def sum_distributions(actual, update):
    """
    Sum distributions of two distribution arrays

    :param actual: first dict with distribution array
    :param update: second dict with distribution array
    :return: Dictionary with sum of distribution as a value of "distributions" key and output information
    """
    for key in actual['distributions'].keys():
        actual['distributions'][key] = [x + y for x, y in zip(actual['distributions'][key], update['distributions'][key])]
    return {'distributions': actual['distributions'], 'output': actual['output']}


def get_distributions_sum(dstream_distributions, configuration):
    """
    Sum wighted distributions within specified window size with microbatch slice.

    :param dstream_distributions: DStream of weighted distributions
    :param configuration: loaded application configuration
    :return: DStream of distributions sum.
    """
    window_size = configuration['configuration']['window']
    window_slice = configuration['configuration']['slice']
    distributions_window = dstream_distributions.window(window_size, window_slice)

    # Sum weighted distributions
    distributions_sum = distributions_window.reduceByKey(lambda actual, update: sum_distributions(actual, update))
    return distributions_sum


def anomaly_filter(distributions, configuration):
    """
    Select those distributions that are close to the given patterns.

    :param distributions: computed distributions sum
    :param configuration: loaded application configuration
    :return: True if some distribution is similar, False otherwise
    """
    # Get half size of the distribution array
    mid = int(len(configuration['distance']['distribution']['default']['intervals'])/2)
    # Get limit that must be fulfilled by left side of the distribution
    limit = configuration['distance']['distribution']['default']['limit']

    for distribution in distributions['distributions'].values():
        # Get sum of the left and right side of the distribution
        left = sum(distribution[:mid])
        right = sum(distribution[mid:])
        # Check if sum of the left side is bigger or equal to given limit (check if desired number of flows were observed)
        if left >= limit:
            if left > right:
                return True
    return False


def sum_with_previous_distributions(json_result):
    """
    Sum of given distributions with previous report sum.

    :param json_result: JSON containing detection result
    :return: Dict with distributions sum
    """
    # Get defined outputs values as a key
    result_key = '-'.join(json_result['output'].values())

    if result_key in reported_detections:
        distributions_sum = {}
        # Get reference to last distributions
        last_distributions = reported_detections[result_key][1]

        for name, distribution in json_result['distributions'].items():
            distributions_sum[name] = [a + b for a, b in zip(distribution, last_distributions[name])]
        reported_detections[result_key][1] = distributions_sum

        return distributions_sum
    else:
        return json_result['distributions']


def check_if_report(json_result, configuration):
    """
    Check if given detection should be reported (after interval) and update distributions sum.

    :param json_result: JSON containing detection result
    :param configuration: loaded application configuration
    :return: True and new distributions sum if result should be reported, False and None otherwise
    """
    # Load global variables
    global reported_detections
    global last_cleaned

    current_time = datetime.now()
    if (current_time - last_cleaned).total_seconds() > 3600:
        # Keep all keys with time lower than one hour
        reported_detections = {key:timestamp_distributions for (key, timestamp_distributions) in reported_detections.items() if (current_time - timestamp_distributions[0]).total_seconds() < 3600}
        last_cleaned = current_time

    # Get defined outputs values as a key
    result_key = '-'.join(json_result['output'].values())
    if result_key in reported_detections:
        # Check if difference between current time and report time is lower or equal to window size (if is bigger than the result should be reported again)
        if (current_time - reported_detections[result_key][0]).total_seconds() <= configuration['configuration']['window']:
            return False, None

    # Add the key to reported detections and append current timestamp with distributions
    reported_detections[result_key] = [current_time, sum_with_previous_distributions(json_result)]
    return True, reported_detections[result_key][1]


def process_results(data_to_process, producer, output_topic):
    """
    Process analyzed data and modify it into desired output.

    :param data_to_process: analyzed data
    :param producer: Kafka producer
    :param output_topic: Kafka topic through which output is send
    """
    # Get half size of the distribution array
    mid = int(len(configuration['distance']['distribution']['default']['intervals']) / 2)

    # Here you can format your results output and send it to the kafka topic
    output_jsons = ""
    for result in data_to_process.values():
        # Check if result should be reported and get distributions sum if yes
        report, distributions_sum = check_if_report(result, configuration)
        if not report:
            continue

        output_json = {}
        output_json['@type'] = 'pattern_finder'
        output_json['configuration'] = configuration['configuration']['name']
        output_json.update(result['output'])
        output_json['data_array'] = []
        highest_distribution_sum = 0
        closest_patterns = []

        for name, distribution in distributions_sum.items():
            output_json['data_array'].append({'name': name, 'distribution': distribution})

            left = sum(distribution[:mid])
            right = sum(distribution[mid:])
            limit = (configuration['distance']['distribution'].get(name) or configuration['distance']['distribution']['default']).get('limit') or configuration['distance']['distribution']['default'].get('limit')

            # Check if sum of the left side is bigger or equal to given limit and compare both distribution sides
            if left >= limit and left > right:
                if left == highest_distribution_sum:
                    closest_patterns.append(name)
                    highest_distribution_sum = left
                elif left > highest_distribution_sum:
                    closest_patterns = [name]
                    highest_distribution_sum = left

        output_json['closest_patterns'] = closest_patterns
        output_jsons += json.dumps(output_json) + '\n'

    # Check if there are any results
    if output_jsons:
        # Print current time in same format as Spark
        cprint('-------------------------------------------')
        cprint('Time: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        cprint('-------------------------------------------')

        # Print results to standard output
        cprint(output_jsons)

        # Send desired output to the output_topic
        kafkaIO.send_data_to_kafka(output_jsons, producer, output_topic)


def process_input(input_data, configuration, vector_definition_functions, distance_function):
    """
    Process raw data and do MapReduce operations.

    :param input_data: input data in JSON format to process
    :param configuration: loaded application configuration
    :param vector_definition_functions: loaded functions for a vector value definition
    :param distance_function: loaded distance function
    :return: processed data
    """
    # Filter flow data based on configuration
    flows_filtered = input_data.filter(lambda flow: flow_filter(flow, configuration))

    # Create flow vectors based on the configuration
    flows_vectors = create_flow_vectors(flows_filtered, configuration, vector_definition_functions)

    # Get distances based on configuration and append computed distances to the current value
    flows_distances = flows_vectors.mapValues(lambda vector: vector.update(distance_function(vector, configuration)) or vector)

    # Transform computed distances to the specified distribution
    flows_distribution = flows_distances.mapValues(lambda distances_vector: distances_vector.update(get_distances_distribution(distances_vector, configuration)) or distances_vector)

    # Sum distributions by same key (SrcIP-DstIP)
    distributions_sum = get_distributions_sum(flows_distribution, configuration)

    # Filter distributions that are similar to given patterns
    anomalies = distributions_sum.filter(lambda distributions: anomaly_filter(distributions[1], configuration))

    return anomalies


if __name__ == '__main__':
    # Definition of application arguments (automatically creates -h argument)
    parser = argparse.ArgumentParser()
    parser.add_argument('-iz', '--input_zookeeper', help='input zookeeper hostname:port', type=str, required=True)
    parser.add_argument('-it', '--input_topic', help='input kafka topic', type=str, required=True)
    parser.add_argument('-oz', '--output_zookeeper', help='output zookeeper hostname:port', type=str, required=True)
    parser.add_argument('-ot', '--output_topic', help='output kafka topic', type=str, required=True)
    parser.add_argument('-c', '--configuration', help='path to the configuration file', type=argparse.FileType('r'),
                        required=False, default='configuration.yml')
    args = parser.parse_args()

    # Load the configuration file
    try:
        configuration = yaml.load(args.configuration)
    except yaml.YAMLError as exc:
        cprint('[error] YAML configuration not correctly loaded: ' + str(exc), 'red')
        sys.exit(1)

    # Load distance function based on the configuration file
    distance_function_module = importlib.import_module('modules.distance_functions.' + configuration['distance']['distance_module'])
    distance_function = getattr(distance_function_module, 'get_distance')

    # Load module functions
    vector_definition_functions = {}
    for configuration_value in configuration['vectors']['values']:
        if configuration_value['type'] == 'module':
            vector_definition_functions[configuration_value['name']] = getattr(importlib.import_module('modules.vector_definition.' + configuration_value['name']), configuration_value['function'])
        else:
            continue

    # Initialize input stream and parse it into JSON
    ssc, parsed_input_stream = kafkaIO.initialize_and_parse_input_stream(args.input_zookeeper, args.input_topic, configuration['configuration']['slice'])

    # Process input in the desired way
    processed_input = process_input(parsed_input_stream, configuration, vector_definition_functions, distance_function)

    # Initialize kafka producer
    kafka_producer = kafkaIO.initialize_kafka_producer(args.output_zookeeper)

    # Process computed data and send them to the output
    kafkaIO.process_data_and_send_result(processed_input, kafka_producer, args.output_topic, process_results)

    # Start Spark streaming context
    kafkaIO.spark_start(ssc)
