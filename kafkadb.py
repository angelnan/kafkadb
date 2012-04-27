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
import config
import subprocess



class Settings(dict):
    def __init__(self, *args, **kw):
        super(Settings, self).__init__(*args, **kw)
        self.__dict__ = self

def parse_arguments(arguments):
    parser = optparse.OptionParser(usage='kafkadb.py [options]')
    parser.add_option('', '--get-model', dest='model', help='Returns Model Name given a table')
    parser.add_option('', '--get-model-field', dest='field', help='Returns Model given a Field and Table')
    parser.add_option('', '--migrate-module', dest='module', help='Generate Migration File for Module... Default: All')
    parser.add_option('', '--set-deferred', dest='deferred', help='Makes deferreble target database constraints')
    parser.add_option('', '--set-undeferred', dest='undeferred', help='Makes undeferreble target database constraints')
    parser.add_option('', '--make-config', dest='make', help='Generats config file to migrate system')
    parser.add_option('', '--migrate', dest='migrate', help='Process execute migration')
         
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
    
    if option.module:
        settings.module=option.module
    else:
        settings.module=False
    
    if option.deferred:
        settings.deferred=True
    else:
        settings.deferred=False
    
    if option.undeferred:
        settings.undeferred=True
    else:
        settings.undeferred=False

    settings.make = False
    if option.make:
        settings.make=True
        
    settings.migrate = False
    if option.migrate:
        settings.migrate = True
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
        model = getModel( cursor, table, field )
        if '_rel' in table:
            print "relation table:",table
        if not source.get(model):
            source[model] = {}
        if not source[model].get(table):
            source[model][table] = {'hash':[]}
        source[model][table][field]=ftype
        source[model][table]['hash'].append( (field,ftype ) )
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
    cursor.execute("UPDATE pg_trigger set tgdeferrable = %s"%deferred )
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
   
def getModuleDiff(source, target):
    
    tables = list(set(source.keys() + target.keys()))
    result = {}.fromkeys(tables)
    
    for table in tables:
        result[table] = {
            'migrate':False,
            'transformation': ktr.get(table, None),
            'depends':False,
            'delete':True,
            }

        if table in source and table in target:
            result[table]['on'] = 'both'
            if config.json_verbose:
                shash=source[table]['hash']
                thash=target[table]['hash']
                if shash == thash:
                    continue
                result[table]['migrate']=False
                result[table]['source'] =[x[0] for x in list(set(shash)-set(thash))]
                result[table]['target'] =[x[0] for x in list(set(thash)-set(shash))]
                result[table]['delete'] = True

        elif table in source and not table in target:
            result[table]['on'] = 'source'
        else:
            result[table]['on'] = 'target'
    return result.copy()
        
def getConfig( source, target, migrate_module ):

    source_modules = getFields( source )        
    target_modules = getFields( target )
        
    modules = list(set(source_modules.keys() + \
                target_modules.keys()))
    
    if migrate_module:
        if not migrate_module in modules:
            raise "No module in source or target"
        modules = [ migrate_module ]
        
    result = {}.fromkeys( modules )
    for module in modules:
        result[module] = {}
#        if module != 'base':
#            continue
#        
#        print source_modules[module].keys()
        if module in source_modules and \
            module in target_modules:    
            result[module] = getModuleDiff(source_modules[module],
                        target_modules[module])
        elif module in source_modules:
            result[module] = getModuleDiff(source_modules[module],{})
        else:
            result[module] = getModuleDiff({},target_modules[module])
 
        path = os.path.join(config.transformation_path, module)
        file_name = '%s.json' % module        
        writeConfigFile( result[module], file_name )          
    return result

def writeConfigFile( config, filename='basic.json'):

    with open(filename, mode='w+') as f:
        json.dump( config , f, indent=8)

    f.close()


def migrate_module( source, target,  module ):
    config = getConfig( source, target, module )

def make_dependencies( data ):
    dependencies = []
    trans = data.copy()
    while trans:
        table,table_data = trans.popitem()
        for depend in table_data['depends'] or []:
            if depend in dependencies:
                continue     
            if table in dependencies:
                index = dependencies.index(table)            
                dependencies.insert(index, depend)
            else:
                dependencies.append(depend)
            
        
        if not table in dependencies:
            dependencies.append(table)        
    return dependencies
    
        
def make_config_file( filename ):
    
    file_list = getFiles()
    result = {}
    config_file_list = set([ x for x in file_list if '.json' in x])
    delete_string = []
    disable_string = []
    enable_string = []
    for config_file in config_file_list:
        f = open(config_file,'r')
        json_data = json.loads( f.read())
        f.close()
        for table,table_data in json_data.iteritems():            
            if table in result:
                result[table]['transformation'] += table_data['transformation']
                result[table]['depends'] += table_data['depends'] 
                result[table]['execute'] += table_data['execute']
                continue
            if not table_data.get('migrate'):
                continue
            result[table] = table_data.copy()
            
            if table_data.get('delete'):
                delete_string.append("DELETE FROM %s; \n" % table)
                
            disable_string.append("ALTER TABLE %s DISABLE TRIGGER ALL;\n" % table)
            enable_string.append("ALTER TABLE %s ENABLE TRIGGER ALL;\n" % table)
            
    # DELETE TABLE DATA BEFORE INSERT
    # DISABLE TRIGGERS
    f = open( config.output_prepare_file, 'w')
    f.write( "\n".join(disable_string))
    f.write( "\n".join(delete_string))
    f.close()
    
    
    # ENABLE TRIGGERS AGAIN
    f = open( config.output_finish_file, 'w')
    f.write( "\n".join(enable_string))
    f.close()
    
    dependencies = make_dependencies(result)
    result['transformation_order'] = dependencies
    writeConfigFile( result, 'migration.json')
    
    
            
def migrate(targetCR):
    
    #Execute java process
    
    print "START...."
    subprocess.call(["java","-jar", "kafkadb.jar", "migration.json"])        
    print "Kettle transformation process finish"
    
    
    #Read prepare strings
    print "Reading PREPARE file..."
    f = open( config.output_prepare_file)
    prepare_sql = f.read()
    f.close()
    
    #Read copy generated file
    print "Reading COPY file"
    f = open(config.output_copy_file)
    copy_sql = f.read()
    f.close()
    
    #Read copy generated file
    print "Reading Finish file"
    f = open(config.output_finish_file)
    finish_sql = f.read()
    f.close()
    
    #Set constraints as deferred
    print "Updating constraints...."
    updateConstraints( targetCR, True )
    
    #targetCR.execute("BEGIN TRANSACTION;")
    print "Upload Data start..."
    targetCR.execute("SET CONSTRAINTS ALL DEFERRED;")
    if prepare_sql:
        print "Prepare Statements.."
        print prepare_sql
        targetCR.execute(prepare_sql)
        
    print "upload finish, comitting..."
    print copy_sql
    targetCR.execute( copy_sql )
    
    print "enable triggers again"
    print finish_sql
    targetCR.execute( finish_sql )
    target_db.commit()
    print "upload data FINISH"
    
    
    #Set constraints as undeferred
    print "Restoring Constraints"
    updateConstraints( targetCR, False )
    
    
    
            
if __name__ == '__main__':
    
    settings = parse_arguments( sys.argv )

    # Config
    source_db  = psycopg2.connect(
        'dbname=%s' % config.source_db,
        'host=%s' % config.source_host_db,
        'user=%s' % config.source_user_db)
    target_db  = psycopg2.connect(
        'dbname=%s' % config.target_db ,
        'host=%s' % config.target_host_db,
        'user=%s' % config.target_user_db)
    
    target_db.set_session(deferrable=True)
        
    path = config.transformation_path
    
    settings = parse_arguments( sys.argv )

    sourceCR = source_db.cursor()
    targetCR = target_db.cursor()   

    ktr = getTransformations()
    
    if settings.migrate:
        print "start migration"
        model = migrate( targetCR )
        print "end migration"
        
    if settings.model:
        model = getModel( targetCR, settings.model )
        print "Model:%s for table name:%s"%(model,settings.model)       
        
    if settings.field:
        model = getModel( targetCR, settings.model, settings.field )
        print "Model:%s for table name:%s and field name:%s"%(model,settings.model,settings.field)       
        
    if settings.deferred:
        updateConstraints( targetCR, settings.deferred )

    if settings.undeferred:
        updateConstraints( targetCR, settings.undeferred )
        
    if settings.module:
        migrate_module( sourceCR, targetCR, settings.module )
   
    if settings.make:
        make_config_file('migrate.json')
    source_db.close()
    target_db.close()
    
    

