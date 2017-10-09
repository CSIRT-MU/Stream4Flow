# -*- coding: utf-8 -*-

#
# MIT License
#
# Copyright (c) 2016 Tomas Pavuk <433592@mail.muni.cz>, Institute of Computer Science, Masaryk University
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

from netaddr import IPAddress  # IP address conversion


def convert_dns_rdata(dnsr_data, dnsr_data_type):
    """
    Checks the data type version and calls correct parsing function

    :param dnsr_data: Data to parse
    :param dnsr_data_type: Type of the data
    :return: Parsed data
    """
    if dnsr_data_type == 1:
        return convert_ipv4(dnsr_data)
    if dnsr_data_type == 28:
        return convert_ipv6(dnsr_data)
    return convert_string(dnsr_data)


def convert_ipv4(data_to_convert):
    """
    Parse IPv4 address from byte array

    :param data_to_convert: Input byte array
    :return: Parsed IPv4 address
    """
    return str(IPAddress(int(data_to_convert[:10], 16)))


def convert_ipv6(data_to_convert):
    """
    Parse IPv6 address from byte array

    :param data_to_convert: Input byte array
    :return: Parsed IPv6 address
    """
    return str(IPAddress(int(data_to_convert[:34], 16)))


def convert_string(data_to_convert):
    """
    Parse ASCII string from byte array

    :param data_to_convert: Input byte array
    :return: Parsed string
    """
    return data_to_convert[2:].decode('hex')
