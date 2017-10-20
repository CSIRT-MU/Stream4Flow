# -*- coding: utf-8 -*-

# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import advanced python collections
import collections
# Import global functions
from global_functions import escape

#----------------- Main Functions -------------------#


def dns_open_resolvers():
    """
    Show the main page of the open DNS resolvers section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/dns_open_resolvers.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_top_n_statistics():
    """
    Obtains TOP N open DNS resolvers statistics.

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
    filter = escape(request.get_vars.filter)
    number = int(escape(request.get_vars.number))

    try:
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'open_dns_resolver'}})

        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'resolved_data.raw': filter}})
            elastic_should.append({'term': {'resolver_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Get ordered data (with maximum size aggregation)
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='resolver_ip.raw', size=2147483647)\
            .bucket('by_data', 'terms', field='resolved_data.raw', size=2147483647) \
            .bucket('sum_by_ip', 'sum', field='flows')
        results = search.execute()

        # Prepare ordered collection
        counter = collections.Counter()
        if type == "resolved":
            for resolver_buckets in results.aggregations.by_src.buckets:
                for dst_buckets in resolver_buckets.by_data.buckets:
                    counter[resolver_buckets.key] += int(dst_buckets.sum_by_ip.value)
        else:
            for resolver_buckets in results.aggregations.by_src.buckets:
                for dst_buckets in resolver_buckets.by_data.buckets:
                    counter[dst_buckets.key] += int(dst_buckets.sum_by_ip.value)

        # Select first N (number) values
        data = ""
        for ip, count in counter.most_common(number):
            data += ip + "," + str(count) + ","

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
    if not (request.get_vars.beginning and request.get_vars.end and request.get_vars.filter):
        json_response = '{"status": "Error", "data": "Some mandatory argument is missing!"}'
        return json_response

    # Parse inputs and set correct format
    beginning = escape(request.get_vars.beginning)
    end = escape(request.get_vars.end)
    filter = escape(request.get_vars.filter)

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch(
            [{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'open_dns_resolver'}})

        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'resolver_ip.raw': filter}})
            elastic_should.append({'term': {'resolved_data.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        qx = Q({'bool': {'must': elastic_bool}})

        # Search with maximum size aggregations
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='resolver_ip.raw', size=2147483647)\
            .bucket('by_data', 'terms', field='resolved_data.raw', size=2147483647) \
            .bucket('sum_by_ip', 'sum', field='flows')
        search.aggs['by_src'].bucket('by_start_time', 'top_hits', size=1, sort=[{'timestamp': {'order': 'asc'}}])
        results = search.execute()

        # Prepare ordered collection
        counter = collections.Counter()
        for resolver_buckets in results.aggregations.by_src.buckets:
            domain_counter = collections.Counter()

            # Calculate sums for each resolved query and resolver ip
            for dst_buckets in resolver_buckets.by_data.buckets:
                domain_counter[(resolver_buckets.key, dst_buckets.key)] += int(dst_buckets.sum_by_ip.value)

            top_resolved_query_for_ip = domain_counter.most_common(1)[0][0][1]
            top_resolved_query_flows_count = domain_counter.most_common(1)[0][1]
            first_timestamp_for_ip = resolver_buckets.by_start_time[0].timestamp.replace("T", " ").replace("Z", "")

            counter[(resolver_buckets.key, top_resolved_query_for_ip, top_resolved_query_flows_count, first_timestamp_for_ip)] \
                += sum(domain_counter.values())

        # Result Parsing into CSV in format: timestamp, resolver_ip, resolved_data, flows
        data = ""
        for record in counter.most_common():
            data += str(record[0][0]) + "," + str(record[0][1]) + "," \
                    + str(record[0][2]) + "," + str(record[0][3]) + "," + str(record[1]) + ","
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Exception: ' + escape(str(e)) + '"}'
        return json_response
