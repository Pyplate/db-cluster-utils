#!/usr/bin/env python

#
#    Delete databases and users on a MySQL server cluster
#
#    Copyright (C) 2014  Steve Breuning
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import MySQLdb
import sys

def print_exception(e):
    line_number = sys.exc_info()[2].tb_lineno
    print "Line: " + str(line_number)
    print e



def drop_table(cur, table_name):

    drop_query = "DROP TABLE IF EXISTS %s" % table_name

    try:
        cur.execute(drop_query)
        print "Dropped table " + table_name
    except Exception, e:
        print "Failed to drop table " + table_name
        print_exception(e)


# remove tables
def remove_tables(cur):
    drop_table (cur, 'users')
    drop_table (cur, 'pages')
    drop_table (cur, 'categories')




def drop_user (cur, username):

    revoke_query = "REVOKE ALL PRIVILEGES FROM '%s'@'localhost'" % username
    drop_query = "DROP USER '%s'@'localhost'" % username

    try:
#        cur.execute(revoke_query)
        cur.execute(drop_query)
        print "Dropped user " + username
    except Exception, e:
        print "Failed to drop user " + username
        print_exception(e)


# remove users
def remove_users (cur):

    drop_user (cur, 'slave_user')
    drop_user (cur, 'db_user')



def stop_slave(cur):
    cur.execute("STOP SLAVE")


def drop_database (cur, db_name):

    try:
        cur.execute("DROP DATABASE my_db")
        print "Dropped database " + db_name
    except Exception, e:
        print "Failed to delete database " + db_name
        print_exception(e)

# remove db
def remove_db(cur):

    drop_database (cur, 'my_db')




def clean_server(host, username, password):

    print "Cleaning " + host
    conn = MySQLdb.connect(host, username, password, '')
    curs = conn.cursor()

    try:
        curs.execute("USE my_db")
    except Exception, e:
        print "Couldn't change database"
        print_exception(e)

    stop_slave(curs)

    print "Connected"
    remove_tables(curs)
    print "Removed tables"
    remove_users (curs)
    print "Removed users"
    remove_db(curs)
    print "Removed database"


# the program starts here
def main():

    clean_server('db1', 'root', 'T4nk3r!12')
    clean_server('db2', 'root', 'T4nk3r!12')
    clean_server('db3', 'root', 'T4nk3r!12')
    clean_server('db4', 'root', 'T4nk3r!12')

if __name__ == "__main__":
    main()



