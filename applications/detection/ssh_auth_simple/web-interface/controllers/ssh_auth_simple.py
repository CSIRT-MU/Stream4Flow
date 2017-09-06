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


def ssh_auth_simple():
    """
    Show the main page of the SSH Auth Simple section.

    :return: Empty dictionary
    """
    # Use standard view
    response.view = request.controller + '/ssh_auth_simple.html'
    return dict()


#----------------- Chart Functions ------------------#


def get_histogram_statistics():
    """
    Obtains statistics about SSH attack in time range.

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
        elastic_bool.append({'term': {'@type': 'ssh_auth_simple'}})
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

        if not detections:
            # Return No data info message
            return '{"status": "Empty", "data": "No data found."}'

        # Return data as JSON
        response = {"status" : "Ok", "data" : detections}
        return json.dumps(response)

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_top_n_statistics():
    """
    Obtains TOP N statistics about SSH attacks specified by type.

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
        elastic_bool.append({'term': {'@type': 'ssh_auth_simple'}})
        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Get ordered data (with maximum size aggregation)
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647)\
              .bucket('by_dst', 'terms', field='dst_ip.raw', size=2147483647)\
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
            json_response = '{"status": "Empty", "data": "No data found."}'
        else:
            json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_attacks_list():
    """
    Obtains list of all SSH attacks in given time range.

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

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'ssh_auth_simple'}})
        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        qx = Q({'bool': {'must': elastic_bool}})

        # Search with maximum size aggregations
        search = Search(using=client, index='_all').query(qx)
        search.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
              .bucket('by_dst', 'terms', field='dst_ip.raw', size=2147483647) \
              .bucket('top_src_dst', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        results = search.execute()

        # Result Parsing into CSV in format: timestamp, source_ip, destination_ip, flows, duration
        data = ""
        for src_aggregations in results.aggregations.by_src.buckets:
            for result in src_aggregations.by_dst.buckets:
                record = result.top_src_dst.hits.hits[0]["_source"]
                m, s = divmod(record["duration_in_milliseconds"]/ 1000, 60)
                h, m = divmod(m, 60)
                duration = "%d:%02d:%02d" % (h, m, s)
                data += record["timestamp"].replace("T", " ").replace("Z","") + "," + record["src_ip"] + "," \
                        + record["dst_ip"] + "," + str(record["flows"]) + "," + str(duration) + ","
        data = data[:-1]

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
