# -*- coding: utf-8 -*-

# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import global functions
from global_functions import escape


#----------------- Main Functions -------------------#


def protocols_statistics():
    """
    Show the main page of the Protocols Statistics section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/protocols_statistics.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_statistics():
    """
    Obtains statistics about TCP, UDP a other protocols.

    :return: JSON with status "ok" or "error" and requested data.
    """

    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.aggregation and request.get_vars.type):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    aggregation = escape(request.get_vars.aggregation)
    type = escape(request.get_vars.type)  # name of field to create sum from, one of {flows, packets, bytes }

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'protocols_statistics'}})

        qx = Q({'bool': {'must': elastic_bool}})
        s = Search(using=client, index='_all').query(qx)
        s.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation)\
              .bucket('by_type', 'terms', field='protocol.raw')\
              .bucket('sum_of_flows', 'sum', field=type)
        s.sort('@timestamp')
        result = s.execute()

        # Result Parsing into CSV in format: timestamp, tcp protocol value, udp protocol value, other protocols value
        data_raw = {}
        data = "Timestamp,TCP protocol,UDP protocol,Other protocols;"  # CSV header
        for interval in result.aggregations.by_time.buckets:
            timestamp = interval.key
            timestamp_values = [''] * 3
            data_raw[timestamp] = timestamp_values
            for bucket in interval.by_type.buckets:
                value = bucket.sum_of_flows.value
                if bucket.key == "tcp":
                    data_raw[timestamp][0] = str(int(value))
                elif bucket.key == "udp":
                    data_raw[timestamp][1] = str(int(value))
                elif bucket.key == "other":
                    data_raw[timestamp][2] = str(int(value))

            data += str(timestamp) + "," + str(data_raw[timestamp][0]) + "," + str(data_raw[timestamp][1]) + "," + str(data_raw[timestamp][2]) + ";"

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
