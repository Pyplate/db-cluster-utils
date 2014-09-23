#!/usr/bin/env python

#
#    Create a database, set up tables and set up replication on a cluster 
#    of MySQL servers.
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

# the name of the database to create
database_name = "my_db"

# the IP address of the control node
control_host = "192.168.0.8"

# the IP address of the master server
master_ip = "192.168.0.35"

# a list of IP addresses of the slave servers
slave_ip_list = ["192.168.0.36", "192.168.0.37", "192.168.0.38"]

# root user's pass word 
root_pw = "somepassword"

# database user's password
db_user_pw = "mypassword"

# slave user's password
slave_user_pw = "mypassword"

# These string will store information about the master server's
# log position.  This information is necessary for replication
mysqlbin = ""
binlog = ""


#mysqlbin = "mysql-bin.000001"
#binlog = "3771"





# create database tables
def create_tables (curs):
    """
    Create database tables
    """

    # create tables
    curs.execute("""CREATE TABLE pages (
                                path TEXT, 
                                title TEXT, 
                                category TEXT,
                                description TEXT,
                                tags TEXT,
                                status TEXT,
                                pubdate DATETIME, 
                                updated DATETIME
                              )""")


    curs.execute("""CREATE TABLE categories (
                                name TEXT,
                                path TEXT,
                                description TEXT,
                                tags TEXT,
                                status TEXT,
                                pubdate DATETIME, 
                                updated DATETIME
                              )""")


    curs.execute("""CREATE TABLE users (
                                username TEXT, 
                                password TEXT,
                                email_addr TEXT,
                                joined DATETIME
                              )""")

    print "Created tables"




def create_users (cur):
    """
    Create users for the database
    """

    create_user (cur, 'db_user', db_user_pw)
    create_user (cur, 'slave_user', slave_user_pw)




def create_user(curs, user_name, pass_word):
    """
    Create a database user
    """

    # I've had some problems creating tables in scripts, 
    # so I'm explicitly granting create privileges

    grant_create_query = "GRANT CREATE, RELOAD ON *.* TO 'root'@'%s' IDENTIFIED BY '%s'" % (control_host, root_pw)
    grant_all_query = "GRANT ALL ON *.* TO 'root'@'%s' IDENTIFIED BY '%s'" % (control_host, root_pw)

    curs.execute(grant_create_query)
    curs.execute("FLUSH PRIVILEGES")

    create_query = "CREATE USER '%s'@'localhost' IDENTIFIED BY '%s'" % (user_name, pass_word)
    grant_query = "GRANT ALL ON %s.* TO '%s'@'localhost'" % (database_name, user_name)

    try:
        curs.execute(create_query)
        print "Created user " + user_name
        curs.execute(grant_query)
        curs.execute("FLUSH PRIVILEGES")
        print "Granted privileges"

    except Exception, e:
        print "Failed to create user " + user_name
        print_exception(e)

    curs.execute(grant_all_query)
    curs.execute("FLUSH PRIVILEGES")



def create_db(curs, db_name):
    """
    Create a database
    """

    create_query = "CREATE DATABASE %s" % db_name
    use_query = "USE %s" % db_name

    try:
        curs.execute(create_query)
        curs.execute(use_query)
        print "Created database " + db_name
    except Exception, e:
        print "Failed to create database " + db_name
        print_exception(e)




def grant_replication (curs, user_name, ip, password):
    """
    Grant replication privileges on the master to a slave
    """

    query = "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%s' IDENTIFIED BY '%s'" % (user_name, ip, password)

    try:
        curs.execute(query)
        curs.execute("FLUSH PRIVILEGES")
        print "Granted replication privilege to " + user_name + "@" + ip + " id'd by " + password
    except Exception, e:
        print "Couldn't grant replication privilege to " + user_name
        print_exception(e)





def use_master(cur, master_ip_addr, slave_user_pword, mysqlbin_str, binlog_str):
    """
    tell a slave which server to use as a master
    """

    set_master_query = """CHANGE MASTER TO MASTER_HOST='%s', 
                    MASTER_USER='slave_user', 
                    MASTER_PASSWORD='%s', 
                    MASTER_LOG_FILE='%s', 
                    MASTER_LOG_POS=%s;
                 """ % (master_ip_addr, slave_user_pword, mysqlbin_str, binlog_str)

    try:
        cur.execute(set_master_query)

        cur.execute("START SLAVE")

    except Exception, e:
        print "Failed to set master\n"
        print_exception(e)



def start_slave (cur):
    """
    start the slave thread
    """

    try:
        cur.execute("START SLAVE")
        print "Started slave thread"
    except Exception, e:
        print "Failed to start slave\n"
        print_exception(e)



def set_read_only(curs):
    """
    make a database read only
    """

    try:
        curs.execute("FLUSH TABLES WITH READ LOCK;")
        curs.execute("SET GLOBAL read_only = 1;")
        print "Set read only"
    except Exception, e:
        print "Failed to set database read only" 
        print_exception(e)

    



def set_as_master (curs, host):
    """
    Make a host a master
    """

    global mysqlbin
    global binlog


    # Grant replication rights to slaves
    for host in slave_ip_list:
        grant_replication (curs, 'slave_user', host, slave_user_pw)

    # get master status
    curs.execute("FLUSH TABLES WITH READ LOCK")
    curs.execute("SHOW MASTER STATUS")
    master_status = curs.fetchall()
    curs.execute("UNLOCK TABLES")

    mysqlbin = str(master_status[0][0])
    binlog = str(master_status[0][1])






def print_exception(e):
    """
    print exception messages
    """

    line_number = sys.exc_info()[2].tb_lineno
    print "Line: " + str(line_number)
    print e







def init_server (host, root_pw, master):
    """
    Initialize a MySQL server as either a master or slave
    """

    conn = MySQLdb.connect(host, 'root', root_pw, '')
    curs = conn.cursor()

    create_db (curs, database_name)
    print "Created database"

    create_users (curs)
    print "Created users"

    # create the database tables
    try:
        create_tables(curs)
        conn.commit()
    except Exception, e:
        conn.rollback()
        print "Failed to create tables"
        print_exception(e)

    if master == True:
        # set up this server as the master
        set_as_master (curs, host)

    else:
        # this server is a slave
        set_read_only(curs)
        # tell the slave which master to use
        use_master (curs, master_ip, slave_user_pw, mysqlbin, binlog)
        start_slave (curs)

    conn.commit()

    # close the connection to the database
    conn.close()
    print "DB Connection closed\n"






def main():
    """
    the program starts here
    """

    print "Init Master"
    init_server (master_ip, root_pw, True)

    print "MySQL bin: " + str(mysqlbin)
    print "Bin log: " + str(binlog)

    for host in slave_ip_list:
        print "Init server " + host
        init_server (host, root_pw, False)


if __name__ == "__main__":
    main()






#def ():
    """
    a simple template for functions that use queries
    """
#
#    query = "" % 
#
#    try:
#        curs.execute("")
#        print ""
#    except Exception, e:
#        print "" + 
#        print_exception(e)


