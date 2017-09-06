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
        elastic_bool.append({'terms': {'@type': ['portscan_vertical', 'portscan_horizontal']}})
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

        # Return info message if no data is present
        if not detections:
            return '{"status": "Empty", "data": "No data found"}'

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
        if type == "horizontal-sources" or type == "horizontal-victims":
            elastic_bool.append({'term': {'@type': 'portscan_horizontal'}})
            dst_field = 'dst_port.raw'
        else:
            elastic_bool.append({'term': {'@type': 'portscan_vertical'}})
            dst_field = 'dst_ip.raw'
        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})
        # Prepare query
        qx = Q({'bool': {'must': elastic_bool}})

        # Elastic can sometimes return not all records that match the search
        search_ip = Search(using=client, index='_all').query(qx)
        search_ip.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
                      .bucket('by_dst', 'terms', field=dst_field, size=2147483647) \
                      .bucket('by_targets', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])

        results_ip = search_ip.execute()

        # Prepare ordered collection
        counter = collections.Counter()
        if type == "horizontal-sources" or type == "vertical-sources":
            for src_buckets in results_ip.aggregations.by_src.buckets:
                for result in src_buckets.by_dst.buckets:
                    hit = result.by_targets.hits.hits[0]["_source"]
                    # For each source IP add number of targets to the counter
                    counter[hit["src_ip"]] += hit["targets_total"]
        else:  # victims
            for src_buckets in results_ip.aggregations.by_src.buckets:
                for result in src_buckets.by_dst.buckets:
                    hit = result.by_targets.hits.hits[0]["_source"]
                    if type == "horizontal-victims":
                        counter[hit["dst_port"]] += hit["targets_total"]
                    else:
                        counter[hit["dst_ip"]] += hit["targets_total"]

        # Select first N (number) values
        data = ""
        for value, count in counter.most_common(number):
            data += value + "," + str(count) + ","
        data = data[:-1]

        # Return info message if no data is present
        if data == "":
            json_response = '{"status": "Empty", "data": "No data found"}'
        # Return data as JSON
        else:
            json_response = '{"status": "Ok", "data": "' + data + '"}'

        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response


def get_scans_list():
    """
    Obtains list of all ports scans in given time range.

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
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'portscan_vertical'}})

        # Set filter
        if filter != 'none':
            elastic_should = []
            elastic_should.append({'term': {'src_ip.raw': filter}})
            elastic_should.append({'term': {'dst_ip.raw': filter}})
            elastic_bool.append({'bool': {'should': elastic_should}})

        # Get data for vertical scans
        qx = Q({'bool': {'must': elastic_bool}})
        s = Search(using=client, index='_all').query(qx)
        s.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
            .bucket('by_dst_ip', 'terms', field='dst_ip.raw', size=2147483647) \
            .bucket('top_src_dst', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        vertical = s.execute()

        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': beginning, 'lte': end}}})
        elastic_bool.append({'term': {'@type': 'portscan_horizontal'}})

        # Append filter
        if filter != 'none':
            elastic_bool.append({'bool': {'should': elastic_should}})

        # Get data for horizontal scans
        rx = Q({'bool': {'must': elastic_bool}})
        r = Search(using=client, index='_all').query(rx)
        r.aggs.bucket('by_src', 'terms', field='src_ip.raw', size=2147483647) \
            .bucket('by_dst_port', 'terms', field='dst_port.raw', size=2147483647) \
            .bucket('top_src_dst', 'top_hits', size=1, sort=[{'@timestamp': {'order': 'desc'}}])
        horizontal = r.execute()

        # Result Parsing into CSV in format: type, timestamp, source_ip, destination_ip/port, targets count, duration
        data = ""
        for src_aggregations in vertical.aggregations.by_src.buckets:
            for result in src_aggregations.by_dst_ip.buckets:
                record = result.top_src_dst.hits.hits[0]["_source"]
                m, s = divmod(record["duration_in_milliseconds"] / 1000, 60)
                h, m = divmod(m, 60)
                duration = "%d:%02d:%02d" % (h, m, s)
                data += "Vertical," + record["@timestamp"].replace("T", " ").replace("Z", "") + "," + record["src_ip"] \
                        + "," + record["dst_ip"] + "," + str(record["targets_total"]) + "," + str(record["flows"]) + "," + str(duration) + ","

        for src_aggregations in horizontal.aggregations.by_src.buckets:
            for result in src_aggregations.by_dst_port.buckets:
                record = result.top_src_dst.hits.hits[0]["_source"]
                m, s = divmod(record["duration_in_milliseconds"] / 1000, 60)
                h, m = divmod(m, 60)
                duration = "%d:%02d:%02d" % (h, m, s)
                data += "Horizontal," + record["@timestamp"].replace("T", " ").replace("Z", "") + "," + record["src_ip"] \
                        + "," + record["dst_port"] + "," + str(record["targets_total"]) + "," + str(record["flows"]) + "," + str(duration) + ","
        data = data[:-1]

        # Return info message if no data is present
        if data == "":
            json_response = '{"status": "Empty", "data": "No data found"}'
        # Return data as JSON
        else:
            json_response = '{"status": "Ok", "data": "' + data + '"}'

        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
