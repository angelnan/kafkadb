# -*- coding: utf-8 -*-
import psycopg2
import optparse
import sys
import os,glob

config = {}


def _parent_store_compute(cr,table_name, parent_field):
        def browse_rec(root, pos=0):
            where = parent_field +'='+str(root)

            if not root:
                where = parent_field+'IS NULL'

            cr.execute('SELECT id FROM %s WHERE %s ORDER BY %s'%(table_name,where,parent_field) )
            pos2 = pos + 1
            childs = cr.fetchall()
            for id in childs:
                pos2 = browse_rec(id[0], pos2)
            cr.execute('update %s set parent_left=%s, parent_right=%s where id=%s'%(table_name, pos,pos2,root))
            return pos2+1

        query = 'SELECT id FROM %s WHERE %s IS NULL order by %s'%(table_name,parent_field,parent_field)
        print query
        pos = 0
        cr.execute(query)
        for (root,) in cr.fetchall():
            pos = browse_rec(root, pos)
            print root,pos

        return True




if __name__ == "__main__":


    with open('kettle.properties') as f:
        for line in f:
            tokens = line.split('=')
            config[tokens[0].strip()] = "=".join([x.strip() for x in tokens[1:]])

  opt =  parseArguments(sys.argv[1:])

  if not (opt.table_name and opt.parent_field):
    print "Table and Field names are required"
    exit()

  #db = psycopg2.connect('dbname=guinama', 'host=localhost','user=angel')
  db = psycopg2.connect("dbname=%s port=%s user=%s"%( config['DATABASE'],config['DATABASE_PORT'],config['DATABASE_USER'] ) )
  cr = db.cursor()
  _parent_store_compute(cr, str(opt.table_name), str(opt.parent_field ))
  db.commit()
  db.close()










