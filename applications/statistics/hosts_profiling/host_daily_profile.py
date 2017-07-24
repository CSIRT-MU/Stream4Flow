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
TODO
  top_n_host_stats.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oh
    <output-hostname>:<output-port> -n <max. # of ports for host>

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

from kafka import KafkaProducer  # Kafka Python client

from collections import namedtuple


# casting structures
IPStats = namedtuple('IPStats', 'ports dst_ips http_hosts')
StatsItem = namedtuple('StatsItem', 'packets bytes flows')

ZERO_ITEM = StatsItem(0, 0, 0)  # neutral item used if no new data about the IP was collected in recent interval

# temporal constants
HOURLY_INTERVAL = 5  # aggregation interval for one item of temporal output array
DAILY_INTERVAL = 120   # collection interval of aggregations as items in output array

TIME_DIMENSION = DAILY_INTERVAL / HOURLY_INTERVAL
print("Time dimension: %s" % TIME_DIMENSION)

EMPTY_TIME_DIMENSIONS = [0] * TIME_DIMENSION

INCREMENT = 0

# temporal methods reassuring the temporal array consistency


def increment():
    """
    increments the global counter that should keep consistent with the duration of the app run in hours
    """
    global INCREMENT
    INCREMENT += 1


def modulate_position(timestamp):
    """
    counts the position in time-sorted log of IP activity as based on the timestamp attached to
    the particular log in rdd
    timestamp: attached timestamp
    """
    result = (INCREMENT - timestamp) % TIME_DIMENSION
    return result


def update_array(array, position, value):
    """
    updates an array inserting a _value_ to a chosen _position_ in an _array_
    overcomes a general restriction disabling to use an assignment in lambda statements
    :param array: _array_
    :param position: _position_
    :param value: _value_
    """
    array[int(position)] = value
    return array


def initialize_array(value, timestamp):
    """
    initializes an empty array of default log length (=TIME_DIMENSION) with a _value_
    inserted on a position at a given _timestamp_
    :param value: _value_
    :param timestamp: _timestamp_
    """
    return update_array(list([ZERO_ITEM] * TIME_DIMENSION), modulate_position(timestamp), value)


def align_to_long_window(rdd_set):
    """
    transforms all RDDs from _rdd_set_ of a format (data, timestamp) into an initial log
    of the size of the default log length (=TIME_DIMENSION)
    the log contains TIME_DIMENSION of zeros except on a modulated timestamp position
    filling zeros as missing values
    :param rdd_set: _rdd_set_
    """
    return rdd_set.map(lambda rdd: (rdd[0], initialize_array(rdd[1][0], rdd[1][1])) \
        if len(rdd[1]) == 2
    else (rdd[0], rdd[1]))


def merge_init_arrays(a1, a2):
    """ Merges the given arrays so that the output array contains either value of a1, or a2 for each nonzero value
    Arrays should be in disjunction append -1 when both arrays are filled, so the error is traceable
    :param a1 array of the size of a2
    :param a2 array of the size of a1
    :return Merges arrays
    """
    merge = []
    for i in range(len(a1)):
        if a1[i] != ZERO_ITEM and a2[i] != ZERO_ITEM:
            # should not happen
            merge.append(-1)
        else:
            merge.append(a1[i] if a1[i] != ZERO_ITEM else a2[i])

    return merge


def send_to_kafka(data, producer, topic):
    """
    Send given data to the specified kafka topic.
    :param data: data to send
    :param producer: producer that sends the data
    :param topic: name of the receiving kafka topic
    """
    producer.send(topic, str(data))


def process_results(json_rdd, producer, topic):
    """
    Transform given computation results into the JSON format and send them to the specified kafka instance.

    JSON format:
    {"src_ipv4":"<host src IPv4 address>",
     "@type":"host_stats_profile_24h",
     "stats":{
        {
            <t=1>: {"packets": <val>, "bytes": <val>, "flows": <val>},
            <t=2>: {"packets": <val>, "bytes": <val>, "flows": <val>},
            ...
            <t=TIME_DIMENSION>: {"port":<port #n>, "flows":# of flows}
        }
    }

    Where <val> is aggregated sum of the specified attribute in an interval of HOURLY_INTERVAL length
    that has ended in time: <current time> - (<entry's t> * HOURLY_INTERVAL)

    :param json_rrd: Map in a format: (src IP , [ IPStats(packets, bytes, flows), ..., IPStats(packets, bytes, flows) ])
    :return:
    """

    for ip, ip_stats in json_rdd.iteritems():
        stats_dict = dict()
        for stat_idx in range(len(ip_stats)):
            temporal_stats = {"packets": ip_stats[stat_idx].packets,
                              "bytes": ip_stats[stat_idx].bytes,
                              "flows": ip_stats[stat_idx].flows}
            stats_dict[stat_idx] = temporal_stats

        # construct the output object in predefined format
        result_dict = {"@type": "top_n_host_stats",
                       "src_ipv4": ip,
                       "stats": stats_dict}

        # send the processed data in json form
        send_to_kafka(json.dumps(result_dict)+"\n", producer, topic)

    # logging terminal output
    print("%s: Stats of %s IPs parsed and sent" % (time.strftime("%c"), len(json_rdd.keys())))


def collect_hourly_stats(stats_json):
    """
    Performs a hourly aggregation on input data, whose result is to be collected in items of daily aggregation
    :type stats_json: Initialized spark streaming context, with data in json format as in host_stats application
    """

    stats_windowed = stats_json.window(HOURLY_INTERVAL, HOURLY_INTERVAL)

    stats_windowed_keyed = stats_windowed.map(lambda json_rdd: (json_rdd["src_ipv4"],
                                                                (json_rdd["stats"]["total"]["packets"],
                                                                 json_rdd["stats"]["total"]["bytes"],
                                                                 json_rdd["stats"]["total"]["flow"])
                                                                ))
    ip_stats_sumed = stats_windowed_keyed.reduceByKey(lambda current, update: (current[0] + update[0],
                                                                               current[1] + update[1],
                                                                               current[2] + update[2]))

    ip_stats_objected = ip_stats_sumed.mapValues(lambda avg_vals: (StatsItem(*avg_vals), INCREMENT))

    return ip_stats_objected


def collect_daily_stats(hourly_stats):
    """
    Aggregation of the time stats of _small_window_data_ in a tuple format (data, timestamp) into a log vector
    in format [data_t_n, data_t_n-1, ... , data_t_n-k] containing the entries of the most k recent
    _small_window_data_ rdd-s where k = TIME_DIMENSION (= DAILY_INTERVAL/HOURLY_INTERVAL)
    :param hourly_stats: _hourly_stats_ aggregated in HOURLY_INTERVAL window
    """
    global INCREMENT

    # set a window of DAY_WINDOW_INTERVAL upon small window RDDs
    long_window_base = hourly_stats.window(DAILY_INTERVAL, HOURLY_INTERVAL)

    # Debug print - see how recent incoming RDDs are transformed after each HOUR_WINDOW_INTERVAL
    # long_window_debug = long_window_base.map(lambda rdd: {"ip": rdd[0],
    #                                                           "rdd_timestamp": rdd[1][1],
    #                                                           "current_inc": INCREMENT,
    #                                                           "mod_pos": modulate_position(int(rdd[1][1])),
    #                                                           "value": rdd[1][0]})
    # long_window_debug.pprint()

    # first logs of small window in format IP: (data, timestamp) are mapped into sparse vector=[0, 0, .. , volume, 0]
    # where vector has a size of TIME_DIMENSION and data inserted on modulated position (see modulate_position())
    # then sparse vectors are combined by merge - "summing-up": nonzero positions (see merge_init_arrays())
    long_window_data_stream = long_window_base.map(lambda rdd: (rdd[0], initialize_array(rdd[1][0], rdd[1][1]))) \
        .reduceByKey(lambda current, update: merge_init_arrays(current, update))

    # current position counter update should keep consistent with small window counter - increment on each new data
    long_window_data_stream.reduce(lambda current, update: 1).foreachRDD(lambda rdd: increment())

    # Debug print in interval of a small window
    # long_window_data_stream.pprint(5)

    # return the vector logs windowed in a daily interval
    return long_window_data_stream.window(HOURLY_INTERVAL, DAILY_INTERVAL)


if __name__ == "__main__":
    # Prepare arguments parser (automatically creates -h argument).
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)

    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)

    # Parse arguments.
    args = parser.parse_args()

    # Set variables
    application_name = os.path.basename(sys.argv[0])  # Application name used as identifier
    kafka_partitions = 1  # Number of partitions of the input Kafka topic
    window_duration = 10  # Analysis window duration (10 seconds)
    window_slide = 10  # Slide interval of the analysis window (10 seconds)

    # Spark context initialization
    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  # Application name used as the appName
    ssc = StreamingContext(sc, 1)  # Spark microbatch is 1 second

    # Initialize input DStream of flows from specified Zookeeper server and Kafka topic
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    # Parse flows in the JSON format
    input_stream_json = input_stream.map(lambda x: json.loads(x[1]))

    # Process data to the defined function.
    hourly_host_statistics = collect_hourly_stats(input_stream_json)
    daily_host_statistics = collect_daily_stats(hourly_host_statistics)

    kafka_producer = KafkaProducer(bootstrap_servers=args.output_zookeeper,
                                   client_id="spark-producer-" + application_name)

    # Transform computed statistics into desired json format and send it to output_host as given in -oh input param
    daily_host_statistics.foreachRDD(lambda rdd: process_results(rdd.collectAsMap(), kafka_producer, args.output_topic))
    # daily_host_statistics.pprint(100)

    # Start input data processing
    ssc.start()
    ssc.awaitTermination()
