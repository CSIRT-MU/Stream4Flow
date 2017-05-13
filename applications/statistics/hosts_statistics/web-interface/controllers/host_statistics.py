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
# Import IP address handling
import ipaddress

#----------------- Main Functions -------------------#

def host_statistics():
    """
    Show the main page of the Host Statistics section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/host_statistics.html'
    return dict()

#----------------- Minor Functions ------------------#

def heatmap_matrix(network):
     """
     Creates empty
     Args:
         network:

     Returns:

     """
     network_cidr = ipaddress.ip_network(unicode(network,"utf8"))
     ip_first = str(network_cidr[0])
     ip_last = str(network_cidr[-1])
     ip_first_c = int(ip_first.split(".")[2])
     ip_first_d = int(ip_first.split(".")[3])
     ip_last_c = int(ip_last.split(".")[2])
     ip_last_d = int(ip_last.split(".")[3])
     matrix = {}
     for c in range(ip_first_c,ip_last_c+1,1):
          matrix[c] = []
          for d in range(ip_first_d, ip_last_d+1,1):
               matrix[c].append(0)

     return matrix


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
        elastic_bool.append({'term': {'src_ipv4': filter}})

        qx = Q({'bool': {'must': elastic_bool}})
        s = Search(using=client, index='_all').query(qx)
        s.aggs.bucket('by_host', 'terms', field='src_ipv4', size=2147483647) \
              .bucket('sum_of_flows', 'sum', field='stats.total.flow')

        result = s.execute()

        # Prepare data matrix for heatmap
        data_raw = heatmap_matrix(filter)

        # Fill matrix with data
        for record in result.aggregations.by_host.buckets:
            host_c = int(record.key.split(".")[2])
            host_d = int(record.key.split(".")[3])
            number_of_flows = record.sum_of_flows.value
            data_raw[host_c][host_d] = number_of_flows
        json_response = {"status": "Ok", "data": data_raw}


        return json.dumps(json_response)


    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
