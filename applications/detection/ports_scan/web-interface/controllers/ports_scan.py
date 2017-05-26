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
