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

##############################################################################
# Create database from SQL file
# Add in kettle.properties:
# template_sqlfile = /path/to/psql_filename.sql
##############################################################################

import os
import subprocess

config = {}


if __name__ == '__main__':

    with open('kettle.properties') as f:
        for line in f:
            tokens = line.split('=')
            config[tokens[0].strip()] = "=".join([x.strip() for x in tokens[1:]])

    dbname = config['target']
    user = config['target_user']
    password = config['target_password']
    sqlfile = config['template_sqlfile']

    if not os.path.isfile(sqlfile):
        print "SQL file %s not found" % sqlfile
        exit()

    process = subprocess.Popen("""
        export PGPASSWORD=%(password)s
        psql %(dbname)s -U %(user)s --quiet -c "select pg_terminate_backend(procpid) from pg_stat_activity where procpid <> pg_backend_pid( ) and datname='%(dbname)s'"
        dropdb -U %(user)s %(dbname)s
        createdb -U %(user)s %(dbname)s -O %(user)s
        psql -U %(user)s %(dbname)s < %(sqlfile)s
        export PGPASSWORD=""" % {
            'user': user,
            'password': password,
            'dbname': dbname,
            'sqlfile': sqlfile,
            }, shell=True)
    process.wait()
