#!/usr/bin/python 
# -*- coding: utf-8 -*-

##############################################################################
#
# Copyright (c) 2011 NaN Projectes de Programari Lliure, S.L.
#                                                        http://www.NaN-tic.com
#                                                          All Rights Reserved.
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
import json
import os
import glob
import ConfigParser
import optparse
import sys


class Settings(dict):
    def __init__(self, *args, **kw):
        super(Settings, self).__init__(*args, **kw)
        self.__dict__ = self

def parse_arguments(arguments):
    parser = optparse.OptionParser(usage='kafkadb.py [options]')
    parser.add_option('', '--get-model', dest='model', help='Returns Model Name given a table')
    parser.add_option('', '--get-model-field', dest='field', help='Returns Model given a Field and Table')
    parser.add_option('', '--gen-file', dest='migrate', help='Generate Migration File')
    parser.add_option('', '--set-deferred', dest='deferred', help='Makes deferreble target database constraints')
    parser.add_option('', '--set-undeferred', dest='undeferred', help='Makes undeferreble target database constraints')
        
    (option, arguments) = parser.parse_args(arguments)
    # Remove first argument because it's application name
    arguments.pop(0)

    settings = Settings()
    
    if option.model:
        settings.model=option.model
    else:
        settings.model=False
    
    if option.field:
        settings.field=option.field
    else:
        settings.field=False
    
    if option.migrate:
        settings.migrate=option.migrate
    else:
        settings.migrate=False
    
    if option.deferred:
        settings.deferred=True
    else:
        settings.deferred=False
    
    if option.undeferred:
        settings.undeferred=True
    else:
        settings.undeferred=False

    return settings

def getFields( cursor ):
    query = """ SELECT
            a.attname as field,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
            relname
        FROM
            pg_catalog.pg_attribute a,
            pg_catalog.pg_class c,
            pg_catalog.pg_namespace n
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
            AND a.attrelid = c.oid
            AND n.oid = c.relnamespace
            AND pg_catalog.pg_table_is_visible(c.oid)
            AND nspname='public'
            AND c.relkind != 'S'
            AND c.oid not in ( select indexrelid from pg_index )
        order BY
            relname,field;
    """
    source = {}
    cursor.execute( query )
    for field,ftype,table in cursor.fetchall():
        if not source.get( table ):
            source[table] = {'hash':[]}
        source[table][field]=ftype
        source[table]['hash'].append( (field,ftype ) )
    return source.copy()


def getModel( cursor, tableName=None, fieldName=None):
    
    model = None
    if not tableName is None :
        query = """
               SELECT 
                     module
               FROM
                     ir_model_data
               where 
                     name = 'model_%s'"""%tableName                     
        cursor.execute( query )
    
        model = cursor.fetchone()
        if model:
            model= model[0]
    
    if not fieldName is None :
        if tableName:
            where = "field_%s_%s"%(tableName,fieldName)
        else:
            where = "field%%_%s"%(fieldName)
            
        query = """
               SELECT 
                     module
               FROM
                     ir_model_data
               where 
                     name = '%s' """%where                     
        cursor.execute( query )
        model = ",".join( [x[0] for x in  cursor.fetchall() ] )
        
    
    return model

def updateConstraints( cursor, deferred ):
    cursor.execute("UPDATE pg_trigger set tderreble = %s"%deferred )
    cursor.execute("UPDATE pg_constraint set condeferrable=%s"%deferred)
    
    
def getFiles( path='model-ktr'):
    #TODO: get all transformation for a given module.
    
    fileList=[]
    for root, subFolders, files in os.walk( path ):
        for file in files:
            fileList.append(os.path.join(root,file)) 
        for subfolder in subFolders:
            folder = os.path.join( root,subfolder )
            fileList += getFiles( folder )
                
    return fileList

    
def getTransformations( path='model-ktr'):

    ktr = {}     
    fileList = list(set(getFiles( path )))
    for file in fileList:
        table =file.split('/')[-1][:-4]
        ktr[table] = ktr.get(table,[]) + [file]
    
    return ktr
    
def getConfig( source, target ):

    sFields = getFields( source )
    tFields = getFields( target )

    tables = list( set(sFields.keys() + tFields.keys() ) )
    result ={}.fromkeys( tables )
    
    ktr = getTransformations()


    for table in tables:
        print "table:",table
        #TODO: avoid this step

        result[table]={ 
                        #'source':[],
                        #'target':[],
                        'migrate':False,
                        'transformation': ktr.get( table, [] )
                       }
        shash=[]
        thash=[]
        if False:
            if tFields.get( table ) and sFields.get( table ):
                shash=sFields[table]['hash']
                thash=tFields[table]['hash']
                if shash == thash:
                    continue
                result[table]['migrate']=True
                result[table]['source'] =[x[0] for x in list(set(shash)-set(thash))]
                result[table]['target'] =[x[0] for x in list(set(thash)-set(shash))]
                result[table]['transformation']= ktr.get(table)
                result[table]['delete'] = False;
        
            elif sFields.get( table ):
                shash=sFields[table]['hash']
                result[table]['source']= [x[0] for x in shash ]
                result[table]['transformation']= ktr.get(table)
        
            else:
                thash=tFields[table]['hash']
                result[table]['target'] = [x[0] for x in thash]
                result[table]['transformation']= ktr.get(table)
                
    return result

def writeConfigFile( config, filename='basic.json'):

    with open(filename, mode='w') as f:
        json.dump( config , f, indent=4)

    f.close()


def migrate( source, target,  filename ):
    
    config = getConfig( source, target )
    writeConfigFile( config, filename )
    
    
if __name__ == '__main__':
    
    settings = parse_arguments( sys.argv )

    # Config
    source_db  = psycopg2.connect('dbname=v5', 'host=localhost','user=angel')
    target_db  = psycopg2.connect('dbname=v6' ,'host=localhost','user=angel')
    path = 'model-ktr/'
    
    settings = parse_arguments( sys.argv )

    sourceCR = source_db.cursor()
    targetCR = target_db.cursor()   

    
    if settings.model:
        model = getModel( targetCR, settings.model )
        print "Model:%s for table name:%s"%(model,settings.model)       
        
    if settings.field:
        model = getModel( targetCR, settings.model, settings.field )
        print "Model:%s for table name:%s and field name:%s"%(model,settings.model,settings.field)       
        
    if settings.deferred:
        updateConstraints( targetCR, settings.deferred )

    if settings.deferred:
        updateConstraints( targetCR, settings.undeferred )
        
    if settings.migrate:
        migrate( sourceCR, targetCR, settings.migrate )
   
    source_db.close()
    target_db.close()
    
    

