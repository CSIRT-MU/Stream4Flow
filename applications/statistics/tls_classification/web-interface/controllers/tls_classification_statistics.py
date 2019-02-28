# -*- coding: utf-8 -*-

# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import advanced python collections
import collections
# Import global functions
from global_functions import escape

#----------------- Main Functions -------------------#


def tls_classification_statistics():
    """
    Show the main page of the TLS classification statistics section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/tls_classification_statistics.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_top_n_statistics():
    """
    Obtains TOP N TLS classification statistics.

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
        search_ip = Search(using=client, index='_all').query(qx)
        search_ip.aggs.bucket('all_nested', 'nested', path='data_array') \
            .bucket('by_key', 'terms', field='data_array.key.raw', size=2147483647) \
            .bucket('stats_sum', 'sum', field='data_array.value')

        # Get result
        results = search_ip.execute()

        # Prepare data variable
        data = ""
        # Prepare ordered collection
        counter = collections.Counter()

        for all_buckets in results.aggregations.all_nested.by_key:
            counter[all_buckets.key] += int(all_buckets.stats_sum.value)

        # Select top N (number) values
        for value, count in counter.most_common(number):
            data += value + "," + str(count) + ","

        # Remove trailing comma
        data = data[:-1]
        
        if data == "":
            json_response = '{"status": "Empty", "data": "No data found"}'
        else:
            json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
