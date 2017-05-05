# -*- coding: utf-8 -*-

#----------------- Imports -------------------------#

# Import global functions
from global_functions import escape
# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import advanced python collections
import collections
# Import JSON operations
import json


#----------------- Main Functions -------------------#

def host_statistics():
    """
    Show the main page of the Host Statistics section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/host_statistics.html'
    return dict()

#----------------- Chart Functions ------------------#

def get_heatmap_statistics():
    """
    Obtains data for headmap chart in a time range

    :return: JSON with status "ok" or "error" and requested data.
    """
    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.aggregation and request.get_vars.filter):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    aggregation = escape(request.get_vars.aggregation)
    filter = escape(request.get_vars.filter)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch(
            [{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'wildcard': {'ip.raw': "147.251." + "*"}})

        qx = Q({'bool': {'must': elastic_bool}})
        s = Search(using=client, index='_all').query(qx)
        s.aggs.bucket('by_host', 'terms', field='ip.raw', size=2147483647) \
              .bucket('sum_of_flows', 'sum', field='stats.total.flow')

        result = s.execute()

        return json.dumps(response)

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
