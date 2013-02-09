#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
#
# Copyright (c) 2011-2013 NaN Projectes de Programari Lliure, S.L.
# http://www.NaN-tic.com
# All Rights Reserved.
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsability of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# garantees and support are strongly adviced to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
##############################################################################

import psycopg2
import os
import optparse
import sys
import subprocess
import ConfigParser

def read_kettle_properties(filename='kettle.properties'):
    config ={}
    with open(filename) as f:
        for line in f:
            tokens = line.split('=')
            config[tokens[0].strip()] = "=".join(
                    [x.strip() for x in tokens[1:]])
    return config

def get_source_connection(config):
    return  psycopg2.connect(
        dbname = config['source'],
        host = config['source_host'],
        port = config['source_port'],
        user = config['source_user'],
        password = config['source_password'])

def get_target_connection(config):
    return psycopg2.connect(
        dbname = config['target'],
        host = config['target_host'],
        port = config['target_port'],
        user = config['target_user'],
        password = config['target_password'])

#TOOLS
def readConfigFile(filename):
    if not os.path.exists(filename):
        return {}

    config = ConfigParser.ConfigParser()
    f = open(filename, 'r')
    config.readfp(f)

    result = {}
    for section in config.sections():
        result[section] = {}
        for option in config.options(section):
            result[section][option] = config.get(section, option)

    f.close()
    return result

#TOOLS
def writeConfigFile(config, filename):

    config_parser = ConfigParser.ConfigParser()
    dirname = os.path.dirname(filename)

    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)

    f = open(filename, 'w+')
    config_parser.readfp(f)

    sorted_list = sorted(config.keys())
    for key in sorted_list:
        config_parser.add_section(key)

    if 'transformation_order' in config:
        order = config.pop('transformation_order')
        config_parser.set('transformation_order', 'transformation_order',
                order)

    if 'start_script' in config:
        script = config.pop('start_script')
        config_parser.set('start_script','script',script)
    
    if 'end_script' in config:
        script = config.pop('end_script')
        config_parser.set('end_script','script',script)

    for key, value in config.iteritems():
        for k, v in value.iteritems():
            if (k == 'source' and v == 'None') or (k == 'target' and v == 'None'):
                config_parser.remove_option(key, k)
            else:
                config_parser.set(key, k, v)

    config_parser.write(f)
    f.close()

