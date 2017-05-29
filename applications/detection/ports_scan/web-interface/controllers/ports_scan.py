# -*- coding: utf-8 -*-

# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import advanced python collections
import collections
# Import JSON operations
import json
# Import global functions
from global_functions import escape


#----------------- Main Functions -------------------#


def ports_scan():
    """
    Show the main page of the TCP Ports Scan section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/ports_scan.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_histogram_statistics():
    """
    Obtains statistics about TCP ports scans for histogram chart.

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
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'terms': {'@type': ['ports_scan_vertical', 'ports_scan_horizontal']}})
        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Get histogram data
        search_histogram = Search(using=client, index='_all').query(qx)
        search_histogram.aggs.bucket('by_time', 'date_histogram', field='@timestamp', interval=aggregation) \
            .bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
            .bucket('sum_of_flows', 'sum', field='flows_increment')
        histogram = search_histogram.execute()

        # Prepare obtained data
        detections = {}
        for interval in histogram.aggregations.by_time.buckets:
            timestamp = interval.key
            for source in interval.by_src.buckets:
                # Create a new key of not exists
                if source.key not in detections:
                    detections[source.key] = []
                # Append array of timestamp and number of flows
                detections[source.key].append([timestamp, source.sum_of_flows.value])

        # Return data as JSON
        response = {"status": "Ok", "data": detections}
        return json.dumps(response)

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_top_n_statistics():
    """
    Obtains TOP N statistics about TCP ports scans.

    :return: JSON with status "ok" or "error" and requested data.
    """

    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.type and request.get_vars.number and request.get_vars.filter):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    type = escape(request.get_vars.type)
    number = int(escape(request.get_vars.number))
    filter = escape(request.get_vars.filter)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        if type == "sources":
            elastic_bool.append({'terms': {'@type': ['ports_scan_vertical', 'ports_scan_horizontal']}})
        elif type == "horizontal-victims":
            elastic_bool.append({'term': {'@type': 'ports_scan_horizontal'}})
        else:
            elastic_bool.append({'term': {'@type': 'ports_scan_vertical'}})
        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Get ordered data (with maximum size aggregation)
        # Get data for the IP
        search_ip = Search(using=client, index='_all').query(qx)
        search_ip.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
                 .bucket('by_dst_ip', 'terms', field='dst_ips.raw', size=2147483647) \
                 .bucket('top_src_ip', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        results_ip = search_ip.execute()
        # Get data for the ports
        search_port = Search(using=client, index='_all').query(qx)
        search_port.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
                   .bucket('by_dst_port', 'terms', field='dst_ports.raw', size=2147483647) \
                   .bucket('top_src_port', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        results_port = search_port.execute()

        # Prepare ordered collection
        counter = collections.Counter()
        if type == "sources":
            for src_buckets_ip in results_ip.aggregations.by_src.buckets:
                counter[src_buckets_ip.key] = len(src_buckets_ip.by_dst_ip.buckets)
            for src_buckets_port in results_port.aggregations.by_src.buckets:
                counter[src_buckets_port.key] = len(src_buckets_port.by_dst_port.buckets)
        elif type == "horizontal-victims":
            for src_buckets_ip in results_ip.aggregations.by_src.buckets:
                for dst_buckets_ip in src_buckets_ip.by_dst_ip.buckets:
                    counter[dst_buckets_ip.key] += 1
        else:
            for src_buckets_port in results_port.aggregations.by_src.buckets:
                for dst_buckets_port in src_buckets_port.by_dst_port.buckets:
                    ports = str(dst_buckets_port.key).split(",")
                    for port in ports:
                        counter[port] += 1

        # Select first N (number) values
        data = ""
        for ip, count in counter.most_common(number):
            data += ip + "," + str(count) + ","
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
