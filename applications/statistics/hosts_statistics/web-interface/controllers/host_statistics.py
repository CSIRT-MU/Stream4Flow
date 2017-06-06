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
#import ipaddress
# Import network operations
from netaddr import IPNetwork

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
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.filter):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response


    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    filter = escape(request.get_vars.filter)



    # Get the first and last IP from given CIDR
    cidr = IPNetwork(filter)
    cidr_first = cidr[0]
    cidr_last = cidr[-1]


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

        # Generate zero values for all IP addresses
        empty_data = ""
        segment_first = str(cidr_first).split(".")
        segment_last = str(cidr_last).split(".")
        for c_segment in range(int(segment_first[2]), int(segment_last[2]) + 1):  # Do at least once
            for d_segment in range(int(segment_first[3]), int(segment_last[3]) + 1):  # Do at least once
                empty_data += str(d_segment) + "," + str(c_segment) + ",0;"


        data = ""
        for bucket in result.aggregations.by_host.buckets:
            ip = bucket.key.split(".")
            # switch D anc C segment of IP to correct view in the chart
            data += ip[3] + "," + ip[2] + "," + str(bucket.sum_of_flows.value) + ";"

        # Create JSON response (combine empty_data and add values)
        json_response = '{"status": "Ok", "data": "' + empty_data + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response

def get_host_flows():
    """
    Gets flows, packet and bytes time series for a given host

    Returns: JSON with status "ok" or "error" and requested data.

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
        s.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation) \
              .metric('sum_of_flows', 'sum', field='stats.total.flow') \
              .metric('sum_of_packets', 'sum', field='stats.total.packets') \
              .metric('sum_of_bytes', 'sum', field='stats.total.bytes')

        result = s.execute()

        data_raw = {}
        data = "Timestamp,Number of flows,Number of packets,Number of bytes;"
        for record in result.aggregations.by_time.buckets:
            timestamp = record.key
            number_of_flows = int(record.sum_of_flows.value)
            number_of_packets = int(record.sum_of_packets.value)
            number_of_bytes = int(record.sum_of_bytes.value)

            data += str(timestamp) + "," + str(number_of_flows) + "," + str(number_of_packets) + "," + str(number_of_bytes) + ";"

        json_response = '{"status": "Ok", "host": "' + filter + '", "data": "' + data + '"}'
        return (json_response)


    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response

def get_host_tcp_flags():
    """
    Gets tcp flags statistics for a given host

    Returns: JSON with status "ok" or "error" and requested data.

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
        s.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation) \
              .metric('sum_of_syn', 'sum', field='stats.tcp_flags.SYN') \
              .metric('sum_of_ack', 'sum', field='stats.tcp_flags.ACK') \
              .metric('sum_of_fin', 'sum', field='stats.tcp_flags.FIN') \
              .metric('sum_of_psh', 'sum', field='stats.tcp_flags.PSH') \
              .metric('sum_of_rst', 'sum', field='stats.tcp_flags.RST') \
              .metric('sum_of_ece', 'sum', field='stats.tcp_flags.ECE') \
              .metric('sum_of_urg', 'sum', field='stats.tcp_flags.URG')

        result = s.execute()

        data_raw = {}
        data = "Timestamp,Sum of SYN, Sum of ACK, Sum of FIN, Sum of PSH, Sum of RST, Sum of ECE, Sum of URG;"
        for record in result.aggregations.by_time.buckets:
            timestamp = record.key
            number_of_syn = int(record.sum_of_syn.value)
            number_of_ack = int(record.sum_of_ack.value)
            number_of_fin = int(record.sum_of_fin.value)
            number_of_psh = int(record.sum_of_psh.value)
            number_of_rst = int(record.sum_of_rst.value)
            number_of_ece = int(record.sum_of_ece.value)
            number_of_urg = int(record.sum_of_urg.value)

            data += str(timestamp) + "," + str(number_of_syn) + "," + str(number_of_ack) + "," + str(number_of_fin) + "," + str(number_of_psh) + "," + str(number_of_rst) + "," + str(number_of_ece) + "," + str(number_of_urg) + ";"

        json_response = '{"status": "Ok", "host": "' + filter + '", "data": "' + data + '"}'
        return (json_response)


    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_host_distinct_ports():
    """
    Gets flows, packet and bytes time series for a given host

    Returns: JSON with status "ok" or "error" and requested data.

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
        s.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation) \
              .metric('dport_avg', 'avg', field='stats.dport_count') \
              .metric('dport_max', 'max', field='stats.dport_count') \
              .metric('dport_min', 'min', field='stats.dport_count')

        result = s.execute()

        data_raw = {}
        data = "Timestamp,Average, Maximum, Minimum;"
        for record in result.aggregations.by_time.buckets:
            timestamp = record.key
            dport_avg = round(record.dport_avg.value,2)
            dport_max = round(record.dport_max.value, 2)
            dport_min = round(record.dport_min.value, 2)

            data += str(timestamp) + "," + str(dport_avg) + "," + str(dport_max) + "," + str(dport_min) + ";"

        json_response = '{"status": "Ok", "host": "' + filter + '", "data": "' + data + '"}'
        return (json_response)


    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response

def get_host_distinct_peers():
    """
    Gets flows, packet and bytes time series for a given host

    Returns: JSON with status "ok" or "error" and requested data.

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
        s.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation) \
              .metric('peer_avg', 'avg', field='stats.peer_number') \
              .metric('peer_max', 'max', field='stats.peer_number') \
              .metric('peer_min', 'min', field='stats.peer_number')

        result = s.execute()

        data_raw = {}
        data = "Timestamp,Average, Maximum, Minimum;"
        for record in result.aggregations.by_time.buckets:
            timestamp = record.key
            peer_avg = round(record.peer_avg.value,2)
            peer_max = round(record.peer_max.value, 2)
            peer_min = round(record.peer_min.value, 2)

            data += str(timestamp) + "," + str(peer_avg) + "," + str(peer_max) + "," + str(peer_min) + ";"

        json_response = '{"status": "Ok", "host": "' + filter + '", "data": "' + data + '"}'
        return (json_response)


    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
