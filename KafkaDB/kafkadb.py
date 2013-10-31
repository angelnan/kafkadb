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

from tools import *

config = {}


class Settings(dict):
    def __init__(self, *args, **kw):
        super(Settings, self).__init__(*args, **kw)
        self.__dict__ = self


class KafkaModel(object):

    def __init__(self, model, cursor):
        self.fields = {}
        self.model = model
        self.cursor = cursor

        self.options = {
            'migrate': False,
            'transformation': '',
            'depends': '',
            'delete': False,
            'mapping': '',
            'source': None,
            'target': None,
            'insert': False,
            'script': False,
            'parent': False,
        }

        self.getFields()

    def getFields(self):
        self.cursor.execute(
            'SELECT '
            '   a.attname as field,'
            '   pg_catalog.format_type(a.atttypid, a.atttypmod) as type '
            'FROM '
            '   pg_catalog.pg_attribute a, '
            '   pg_catalog.pg_class c, '
            '   pg_catalog.pg_namespace n  '
            'WHERE  '
            '   a.attnum > 0 '
            '   AND NOT a.attisdropped '
            '   AND a.attrelid = c.oid '
            '   AND n.oid = c.relnamespace '
            '   AND pg_catalog.pg_table_is_visible(c.oid) '
            '   AND nspname = %s'
            '   AND c.relkind != %s '
            '   AND c.oid not in ( select indexrelid from pg_index )'
            '   AND relname = %s '
            'ORDER BY relname,field ', ('public', 'S', self.model))

        for field, ftype in self.cursor.fetchall():
            self.fields[field] = ftype


class TrytonModel(KafkaModel):

    def __init__(self, model, cursor):
        return super(TrytonModel, self).__init__(model, cursor)


class OpenerpModel(KafkaModel):

    def __init__(self, model, cursor):
        super(OpenerpModel, self).__init__(model, cursor)


def moduleFactory(cursor, name, program, version=None):
    if program == 'tryton':
        return TrytonModule(cursor, name, program, version)
    elif program == 'openerp':
        return  OpenerpModule(cursor, name, program, version)
    else:
        print "not suported yet"
        return None



class Module(object):

    def __init__(self, cursor, name, program, version=None):
        self.name = name
        self.version = version
        self.program = None
        self.model = {}
        self.cursor = cursor
        self.factory = None
        self.getModels()
        self.dependencies = []
        self.filename = os.path.join(config.get('transformation_path', ''),
                    self.name, self.name + ".cfg")

        self.loadConfigFile()

    def getModels(self):
        pass

    def getFiles(self):
        path = os.path.join(config.get('transformation_path', ''),
                self.name)

        files = getFiles(path)
        for filename in files:
            if not '.ktr' in filename:
                continue
            file_model = filename.split('/')[-1]
            model = file_model[:-4]
            if not self.model.get(model):
                continue

            self.model[model].options['transformation'] = file_model
            self.model[model].options['migrate'] = True

    def isInstalled(self):
        self.cursor.execute(
            'select count(*) from ir_module_module where name=%s and state=%s',
            (self.name, 'installed'))
        res = self.cursor.fetchone()[0]
        if res:
            return True

        return False

    def loadConfigFile(self):

        if not os.path.exists(self.filename):
            return

        config = ConfigParser.ConfigParser()
        f = open(self.filename, 'r')
        config.readfp(f)

        for section in config.sections():
            model = self.model.get(section)
            if not model:
                self.model[section] = TrytonModel(section, self.cursor)
                model = self.model[section]
            for option in model.options:
                if config.has_option(section, option):
                    model.options[option] = config.get(section, option)

        self.getFiles()

    def writeConfigFile(self):

        data = {}
        for model_name, model in self.model.iteritems():
            data[model_name] = model.options.copy()

        writeConfigFile(data, self.filename)



class OpenerpModule(Module):

    program = 'openerp'

    def getModels(self):

        self.cursor.execute(
            "SELECT "
            "   name "
            "FROM "
            "   ir_model_data "
            "WHERE "
            "   module = %s and "
            "   name like %s", (self.name, 'module_%'))

        for model_name, in self.cursor.fetchall():
            model_name = model_name.replace('module_', '')
            model = OpenerpModel(model_name, self.cursor)
            self.model[model_name] = model


class TrytonModule(Module):

    program = 'tryton'

    def getModels(self):
        self.cursor.execute(
            'SELECT DISTINCT model FROM ('
            'SELECT '
            '    distinct m.model '
            'FROM'
            '    ir_model_field f,'
            '    ir_model m '
            'WHERE'
            '    f.model = m.id and '
            '    f.module=%s '
            'UNION '
            'SELECT '
            ' distinct model '
            'FROM'
            '   ir_model_data '
            'WHERE'
            '   module = %s ) as aux', (self.name,self.name))

        for model_name, in self.cursor.fetchall():
            model_name = model_name.replace('.', '_')
            model = TrytonModel(model_name, self.cursor)
            self.model[model_name] = model


def migrate_module(source, target, module):

    #sourceModule = moduleFactory( source, module, 'openerp')
    targetModule = moduleFactory(target, module, 'tryton')

    targetModule.writeConfigFile()


def parse_arguments(arguments):
    parser = optparse.OptionParser(usage='kafkadb.py [options]')
    parser.add_option('', '--get-model', dest='model',
            help='Returns Model Name given a table')
    parser.add_option('', '--get-model-field', dest='field',
            help='Returns Model given a Field and Table')
    parser.add_option('', '--migrate-module', dest='module',
            help='Generate Migration File for Module... Default: All')
    parser.add_option('', '--set-deferred', dest='deferred',
            action='store_true', help='Makes deferreble target database \
            constraints')
    parser.add_option('', '--set-undeferred', dest='undeferred',
            action='store_true', help='Makes undeferreble target database \
            constraints')
    parser.add_option('', '--make-config', action='store_true', dest='make',
            help='Generats config file to migrate system')
    parser.add_option('', '--migrate', action='store_true', dest='migrate',
            help='Process execute migration')

    (option, arguments) = parser.parse_args(arguments)
    # Remove first argument because it's application name
    arguments.pop(0)

    settings = Settings()

    if option.model:
        settings.model = option.model
    else:
        settings.model = False

    if option.field:
        settings.field = option.field
    else:
        settings.field = False

    if option.module:
        settings.module = option.module
    else:
        settings.module = False

    if option.deferred:
        settings.deferred = True
    else:
        settings.deferred = False

    if option.undeferred:
        settings.undeferred = True
    else:
        settings.undeferred = False

    settings.make = False
    if option.make:
        settings.make = True

    settings.migrate = False
    if option.migrate:
        settings.migrate = True
    return settings


#TOOLS
#TOOD: unused
def getFields(cursor):
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
    cursor.execute(query)
    for field, ftype, table in cursor.fetchall():
        model = getModel(cursor, table, field)
        if '_rel' in table:
            pass
        if not source.get(model):
            source[model] = {}
        if not source[model].get(table):
            source[model][table] = {'hash': []}
        source[model][table][field] = ftype
        source[model][table]['hash'].append((field, ftype))
    return source.copy()


#TOOLS
def getModel(cursor, tableName=None, fieldName=None):

    model = None
    if not tableName is None:
        query = """
               SELECT
                     module
               FROM
                     ir_model_data
               where
                     name = 'model_%s'""" % tableName
        cursor.execute(query)

        model = cursor.fetchone()
        if model:
            model = model[0]

    if not fieldName is None:
        if tableName:
            where = "field_%s_%s" % (tableName, fieldName)
        else:
            where = "field%%_%s" % (fieldName)

        query = """
               SELECT
                     module
               FROM
                     ir_model_data
               where
                     name = '%s' """ % where
        cursor.execute(query)
        model = ",".join([x[0] for x in cursor.fetchall()])

    return model


#TOOLS
def updateConstraints(cursor, deferred):
    cursor.execute("UPDATE pg_trigger set tgdeferrable = %s" % deferred)
    cursor.execute("UPDATE pg_constraint set condeferrable=%s" % deferred)


#TOOLS
def getFiles(path='model-ktr'):
    #TODO: get all transformation for a given module.

    fileList = []
    for root, subFolders, files in os.walk(path):
        files = [f for f in files if not f[0] == '.']
        subFolders[:] = [d for d in subFolders if not d[0] == '.']
        for file in files:
            fileList.append(os.path.join(root, file))
        for subfolder in subFolders:
            folder = os.path.join(root, subfolder)
            fileList += getFiles(folder)

    return fileList


#TOOLS
def getTransformations(path='model-ktr'):

    ktr = {}
    fileList = list(set(getFiles(path)))
    for file in fileList:
        table = file.split('/')[-1][:-4]
        ktr[table] = ktr.get(table, []) + [file.split('/')[-1]]
    return ktr


#TOOLS
def getModuleDiff(source, target):

    tables = list(set(source.keys() + target.keys()))
    result = {}.fromkeys(tables)

    for table in tables:
        result[table] = {
            'migrate': False,
            'transformation': ktr.get(table, None),
            'depends': False,
            'delete': True,
            'source': None,
            'target': None,
            'insert': False,
            'start_script': False,
            'end_script': False,
            'parent': False,
            }

        if table in source and table in target:
            result[table]['on'] = 'both'
            if config.json_verbose:
                shash = source[table]['hash']
                thash = target[table]['hash']
                if shash == thash:
                    continue
                resutl[table]['parent'] = False
                result[table]['migrate'] = False
                result[table]['source'] = [x[0] for x in list(set(shash) - set(thash))]
                result[table]['target'] = [x[0] for x in list(set(thash) - set(shash))]
                result[table]['delete'] = True

        elif table in source and not table in target:
            result[table]['on'] = 'source'
        else:
            result[table]['on'] = 'target'
    return result.copy()



def make_dependencies(data):
    order = []
    trans = data.copy()
    while trans:
        table, table_data = trans.popitem()
        depends = table_data['depends'] and \
                table_data['depends'].split(',') or []
        depends = list(set([x.strip(' ') for x in depends]))

        a = order[:]
        b = []
        if table in order:
            a = order[:order.index(table)]
            b = order[order.index(table):]
        else:
            b = [table]

        for depend in depends:
            if depend in b:
                b.remove(depend)
            if not depend in a:
                a.append(depend)

        order = a + b

    return order



def make_config_file(targetCR, filename):

    result = make_config(targetCR)
    writeConfigFile(result, config['migration_config'])


def get_value( val, val2 ):

    if val is None:
        val = ''
    if val2 is None:
        val2 = ''

    if val and val.strip() and val2 and val2.strip():
        return val.strip() + "," + val2.strip()
    elif val.strip() and not val2.strip():
        return val.strip()
    elif not val.strip() and val2.strip():
        return val2.strip()

def make_config(targetCr):

    file_list = getFiles(config['transformation_path'])
    result = {}
    config_file_list = set([x for x in file_list if '.cfg' in x])

    dirname = os.path.dirname(config['sql_prepare'])
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)

    start_script = []
    end_script = []

    for config_file in config_file_list:
        module = os.path.dirname(config_file)
        module_name = module.split('/')[-1]
        targetModule = moduleFactory(targetCr, module_name, 'tryton')
        if not targetModule.isInstalled():
            continue
        data = readConfigFile(config_file)
        for key, value in data.iteritems():

            if value.get('migrate') == 'False':
                continue

            if value.get('start_script') and \
                    value.get('start_script') != 'False':

                script_path=config['transformation_path']
                for script_file in value['start_script'].split(","):
                    start_script += [os.path.join(script_path,
                        module, script_file)]
                continue

            if value.get('end_script') and \
                    value.get('end_script') != 'False':

                script_path=config['transformation_path']
                for script_file in value['end_script'].split(","):
                    end_script += [os.path.join(script_path,
                        module, script_file)]
                continue


            if key in result:
                if not eval(value.get('insert','False')):
                    result[key]['transformation'] = "%s,%s/%s" % (
                        result[key]['transformation'],
                        module,
                        value['transformation'])
                else:
                    result[key]['transformation'] = "%s/%s,%s" % (
                        module,
                        value['transformation'],
                        result[key]['transformation'])
                result[key]['depends'] = get_value(result[key]['depends'],
                        value['depends'])

                result[key]['delete'] = str(eval(result[key]['delete']) or
                        eval(value['delete']))

                parent = 'parent'
                if not parent in result[key]:
                    result[key][parent] = 'False'

                result[key][parent] = str(eval(result[key].get(parent,'False')) or
                        eval(value.get(parent,'False')))
                result[key]['mapping'] = get_value(result[key]['mapping'],
                        value['mapping'])
                continue

            if not value.get('transformation'):
                continue
            result[key] = value.copy()
            result[key]['transformation'] = "%s/%s" % (
                    module,
                    value['transformation'])

    dependencies = make_dependencies(result)
    if None in dependencies:
        dependencies.remove(None)
    result['transformation_order'] = ",".join([x.strip() for x in dependencies])
    result['start_script'] = ",".join(start_script)
    result['end_script'] = ",".join(end_script)

    return result


def migrate_sql():

    data = readConfigFile('migration.cfg')

    delete = []
    disable = []
    enable = []
    mapping = [
            'DROP SCHEMA IF EXISTS  migration CASCADE;',
            'CREATE schema migration;']
    sequence = []

    for key, value in data.iteritems():
        target_table = value.get('target', key)

        if key in ['transformation_order','start_script','end_script']:
            continue

        if value.get('migrate') == 'False':
            continue

        if eval(value.get('delete', 'False')):
            delete.append('DELETE FROM "%s"; \n' % target_table)

        if value.get('mapping') and value.get('mapping') != 'None':
            mappings = value['mapping'].split(',')
            for mapp in mappings:
                mapping.append(
                'CREATE TABLE migration."%s" (source int, target int);\n' % (
                mapp))

        disable.append('ALTER TABLE "%s" DISABLE TRIGGER ALL;\n' % target_table)
        enable.append('ALTER TABLE "%s" ENABLE TRIGGER ALL;\n' % target_table)
        target_sequence = '%s_id_seq' % target_table
        target_field = 'id'
        if '__history' in target_table:
            target_sequence = '%s___id_seq' % target_table
            target_field = '__id'
        sequence.append(
            "select setval('%s', (select max(%s) from \"%s\"));"
            "\n" % (target_sequence, target_field, target_table))

    # DELETE TABLE DATA BEFORE INSERT
    # DISABLE TRIGGERS
    disable.insert(0, "-- preapre statements")
    f = open(config['sql_prepare'], 'w+')
    f.write("\n".join(disable))
    f.write("\n".join(delete))
    f.close()

    # ENABLE TRIGGERS AGAIN
    f = open(config['sql_finish'], 'w+')
    f.write("\n".join(enable))
    f.write("\n".join(sequence))
    f.close()

    f = open(config['sql_files'] + "/map.sql", 'w+')
    f.write("\n".join(mapping))
    f.close()


def executeScripts(target='start_script'):

    migration = readConfigFile('migration.cfg')
    if migration.get(target):
        scripts = migration.get(target)['script']
        #print scripts
        for script in scripts.split(","):
            print "Python Script(%s): " % (target),script
            if not script:
                continue
            subprocess.call(["python " + script], shell=True)


def _parent_store_compute(cr, table, field):
        def browse_rec(root, pos=0):
            where = field + '=' + str(root)

            if not root:
                where = parent_field + 'IS NULL'

            cr.execute('SELECT id FROM "%s" WHERE %s \
                ORDER BY %s' % (table, where, field))
            pos2 = pos + 1
            childs = cr.fetchall()
            for id in childs:
                pos2 = browse_rec(id[0], pos2)
            cr.execute('update %s set "left"=%s, "right"=%s\
                where id=%s' % (table, pos, pos2, root))
            return pos2 + 1

        query = 'SELECT id FROM "%s" WHERE %s IS NULL order by %s' % (
            table, field, field)
        pos = 0
        cr.execute(query)
        for (root,) in cr.fetchall():
            pos = browse_rec(root, pos)
        return True


def calc_parent_leftright(targetCR):
    migration = readConfigFile('migration.cfg')
    tables = []
    for table ,values in migration.iteritems():
        parent = values.get('parent')
        if parent is None or parent == 'False':
            continue
        tables.append((table, parent))

    for table, field in tables:
        print "calculating parent_left of table", table, "and field:", field
        _parent_store_compute(targetCR, table, field)



def migrate(targetCR):

    #Execute java process
    if not os.path.exists(config['sql_files']):
        os.makedirs(config['sql_files'])

    print "Execute start scripts"
    executeScripts()

    print "START...."
    migrate_sql()

    print "Reading Mappingfile..."
    f = open(config['sql_files'] + "/map.sql")
    map_sql = f.read()
    f.close()

    if map_sql:
        print "Prepare Statements.."
        #print map_sql
        targetCR.execute(map_sql)
        target_db.commit()

    subprocess.call(["java", "-jar", "kafkadb.jar", "migration.cfg"])
    print "Kettle transformation process finish"

    #Read prepare strings
    print "Reading PREPARE file..."
    f = open(config['sql_prepare'])
    prepare_sql = f.read()
    f.close()

    if prepare_sql:
        print "Prepare Statements.."
    #    print prepare_sql
        targetCR.execute(prepare_sql)

    #Read copy generated file
    print "Reading COPY file"
    f = open(config['sql_copy'])
    copy_sql = f.read()
    f.close()

    #Read copy generated file
    print "Reading Finish file"
    f = open(config['sql_finish'])
    finish_sql = f.read()
    f.close()

    #Set constraints as deferred
    print "Updating constraints...."
    updateConstraints(targetCR, True)

    #targetCR.execute("BEGIN TRANSACTION;")
    print "Upload Data start..."
    targetCR.execute("SET CONSTRAINTS ALL DEFERRED;")

    if copy_sql:
        print "upload finish, comitting..."
    #    print copy_sql
        targetCR.execute(copy_sql)

    print "enable triggers again"
    #print finish_sql
    targetCR.execute(finish_sql)
    target_db.commit()
    print "upload data FINISH"

    print "Execute start scripts"
    executeScripts('end_script')

    #Set constraints as undeferred
    print "Restoring Constraints"
    updateConstraints(targetCR, False)
    print "Calc Parent Left-Right on Tables"
    calc_parent_leftright(targetCR)



if __name__ == '__main__':

    settings = parse_arguments(sys.argv)

    config = read_kettle_properties()

    if config.get('start_scripts') and settings.get('migrate'):
        scripts = config.get('start_scripts')
        for script in scripts.split(","):
            print "Start Python Script:",script
            subprocess.call(["python", script])

    if not os.path.exists(config['sql_files']):
        os.makedirs(config['sql_files'])

    open(config['sql_copy'], 'w').close()

    # Config
    source_db = get_source_connection(config)
    target_db = get_target_connection(config)
    target_db.set_session(deferrable=True)

    transformation_path = config['transformation_path']


    sourceCR = source_db.cursor()
    targetCR = target_db.cursor()

    ktr = getTransformations(transformation_path)

    if settings.migrate:
        print "start migration"
        model = migrate(targetCR)
        print "end migration"

    if settings.model:
        model = getModel(targetCR, settings.model)
        print "Model:%s for table name:%s" % (model, settings.model)

    if settings.field:
        model = getModel(targetCR, settings.model, settings.field)
        print "Model:%s for table name:%s and field name:%s" % (
            model, settings.model, settings.field)

    if settings.deferred:
        updateConstraints(targetCR, settings.deferred)

    if settings.undeferred:
        updateConstraints(targetCR, settings.undeferred)

    if settings.module:
        migrate_module(sourceCR, targetCR, settings.module)

    if settings.make:
        make_config_file(targetCR, config['migration_config'])

    source_db.close()
    target_db.commit()
    target_db.close()

    if config.get('end_scripts') and settings.get('migrate'):
        scripts = config.get('end_scripts')
        for script in scripts.split(","):
            print "End Python Script:",script
            subprocess.call(["python", script])
