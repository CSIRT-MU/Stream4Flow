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


def pattern_finder():
    """
    Show the main page of the Pattern Finder section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/pattern_finder.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_top_n_statistics():
    """
    Obtains TOP N statistics about Pattern Finder detections specified by type and configuration.

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
    config_filter = escape(request.get_vars.config_filter)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'pattern_finder'}})

        if config_filter != 'none':
            elastic_bool.append({'term': {'configuration.raw': config_filter}})

        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip': filter}})
            elastic_should.append({'term': {'dst_ip': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Get ordered data (with maximum size aggregation)
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='src_ip', size=2147483647)\
              .bucket('by_dst', 'terms', field='dst_ip', size=2147483647)\
              .bucket('top_src_dst', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        results = search.execute()

        # Prepare ordered collection
        counter = collections.Counter()
        for src_buckets in results.aggregations.by_src.buckets:
            if type == "sources":
                counter[src_buckets.key] = len(src_buckets.by_dst.buckets)
            else:
                for dst_buckets in src_buckets.by_dst.buckets:
                    counter[dst_buckets.key] += 1

        # Select first N (number) values
        data = ""
        for ip, count in counter.most_common(number):
            data += ip + "," + str(count) + ","
        data = data[:-1]

        if data == "":
            json_response = '{"status": "Empty", "data": "No data found"}'
        else:
            json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_attacks_list():
    """
    Obtains list of all detections by Pattern Finder in given time range.

    :return: JSON with status "ok" or "error" and requested data.
    """

    # Check login
    if not session.logged:
        json_response = '{"status": "Error", "data": "You must be logged!"}'
        return json_response

    # Check mandatory inputs
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.filter):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    filter = escape(request.get_vars.filter)
    config_filter = escape(request.get_vars.config_filter)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'pattern_finder'}})

        if config_filter != 'none':
            elastic_bool.append({'term': {'configuration.raw': config_filter}})

        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip': filter}})
            elastic_should.append({'term': {'dst_ip': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})

        qx = Q({'bool': {'must': elastic_bool}})

        # Search with maximum size aggregations
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='src_ip', size=2147483647) \
                   .bucket('by_dst', 'terms', field='dst_ip', size=2147483647) \
                   .bucket('top_src_dst', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        results = search.execute()

        # Result Parsing into CSV in format: timestamp, source_ip, destination_ip, flows, duration
        data = ""
        for src_aggregations in results.aggregations.by_src.buckets:
            for result in src_aggregations.by_dst.buckets:
                record = result.top_src_dst.hits.hits[0]["_source"]
                timestamp = record['@timestamp'].replace('T', ' ').replace('Z', '')
                closest_patterns = ""
                for pattern in record['closest_patterns']:
                    closest_patterns += str(pattern) + '; '

                for data_array in record['data_array']:
                    if data_array['name'] == record['closest_patterns'][0]:
                        distribution = data_array['distribution']
                        mid_index = len(distribution)/2
                        array_ratio = sum(distribution[:mid_index]) / float(sum(distribution[mid_index:])) \
                            if sum(distribution[mid_index:]) else float('inf')

                if array_ratio < 1.1:
                    confidence = "Very Low"
                elif array_ratio < 1.25:
                    confidence = "Low"
                elif array_ratio < 2:
                    confidence = "Medium"
                elif array_ratio < 5:
                    confidence = "High"
                else:
                    confidence = "Very high"

                closest_patterns = closest_patterns[:-2]
                data += timestamp + ',' + record["src_ip"] + ',' + record["dst_ip"] + ',' \
                        + (record.get('configuration') or 'unknown') + ',' + closest_patterns + ',' + confidence + ','
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
