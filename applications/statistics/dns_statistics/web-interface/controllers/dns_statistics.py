# -*- coding: utf-8 -*-

# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import advanced python collections
import collections
# Import global functions
from global_functions import escape

#----------------- Main Functions -------------------#


def dns_statistics():
    """
    Show the main page of the DNS statistics section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/dns_statistics.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_top_n_statistics():
    """
    Obtains TOP N DNS statistics.

    :return: JSON with status "ok" or "error" and requested data.
    """

    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.type and request.get_vars.number):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    type = escape(request.get_vars.type)
    number = int(escape(request.get_vars.number))

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@stat_type': type}})

        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Set query according to the statistic type
        if type == "queried_by_ip":
            search_ip = Search(using=client, index='_all').query(qx)
            search_ip.aggs.bucket('all_nested', 'nested', path='data_array') \
                .bucket('by_key', 'terms', field='data_array.key.raw', size=2147483647)\
                .bucket('by_ip', 'terms', field='data_array.ip.raw', size=1, order={'sum_by_ip': 'desc'}) \
                .bucket('sum_by_ip', 'sum', field='data_array.value')
            search_ip.aggs['all_nested']['by_key'].bucket('sum_total', 'sum', field='data_array.value')
            results = search_ip.execute()
        else:
            search_ip = Search(using=client, index='_all').query(qx)
            search_ip.aggs.bucket('all_nested', 'nested', path='data_array') \
                .bucket('by_key', 'terms', field='data_array.key.raw', size=2147483647) \
                .bucket('stats_sum', 'sum', field='data_array.value')
            results = search_ip.execute()

        # Prepare data variable
        data = ""
        # Prepare ordered collection
        counter = collections.Counter()

        if type == "queried_by_ip":
            for record in results.aggregations.all_nested.by_key.buckets:
                top_ip = record.by_ip.buckets[0]
                counter[(record.key, top_ip.key, int(top_ip.sum_by_ip.value))] = int(record.sum_total.value)

            # Select top N (number) values
            for value, count in counter.most_common(number):
                data += value[0] + "," + value[1] + "," + str(value[2]) + "," + str(count) + ","
        else:
            for all_buckets in results.aggregations.all_nested.by_key:
                counter[all_buckets.key] += int(all_buckets.stats_sum.value)

            # Select top N (number) values
            for value, count in counter.most_common(number):
                data += value + "," + str(count) + ","

        # Remove trailing comma
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_records_list():
    """
    Obtains list of all records for given type given time range.

    :return: JSON with status "ok" or "error" and requested data.
    """

    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.type):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    type = escape(request.get_vars.type)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch(
            [{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@stat_type': type}})

        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Set query according to the statistic type
        search_ip = Search(using=client, index='_all').query(qx)
        search_ip.aggs.bucket('all_nested', 'nested', path='data_array')\
            .bucket('by_key', 'terms', field='data_array.key.raw', size=2147483647)\
            .bucket('stats_sum', 'sum', field='data_array.value')
        results = search_ip.execute()

        data = ""
        for all_buckets in results.aggregations.all_nested.by_key:
            data += all_buckets.key + "," + str(int(all_buckets.stats_sum.value)) + ","

        # Remove trailing comma
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
