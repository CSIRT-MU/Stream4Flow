# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2018 Milan Cermak <cermak@ics.muni.cz>, Institute of Computer Science, Masaryk University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import math  # Mathematical functions


def get_distance(vector, configuration):
    """
    Compute Quadratic form distance of given vector and patterns specified in the configuration.

    :param vector: vector that is compared with patterns specified in configuration
    :param configuration: loaded application configuration
    :return: Dictionary of pattern names and its distances from given vector
    """
    distances = {}
    for pattern in configuration['distance']['patterns']:
        request_distance = sum([((v - p) / p) ** 2 for v, p in zip(vector['vector']['request'], pattern['request'])])
        response_distance = sum([((v - p) / p) ** 2 for v, p in zip(vector['vector']['response'], pattern['response'])])
        distance = math.sqrt(request_distance + response_distance)
        distances[pattern['name']] = distance
    return {'distances': distances}
