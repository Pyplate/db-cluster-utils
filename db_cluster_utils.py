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
import time
import os
import subprocess
import ConfigParser
import getopt




def config_file_path():
#    return '/etc/db_cluster_utils/cluster_utils.conf'
    return './cluster_utils.conf'

def log_file_path():
    return './status_log_records.dat'




def usage ():

    print """Usage:
db_cluster_utils.py -option [argument]
 -a	--add <slave ip>	add a new slave to the cluster
 -c	--config		view cluster configuration information
 -d	--demote		demote a master to a slave
 -h	--help			display this help message
 -i	--init			initialize the cluster based on settings 
				in /etc/db_cluster_utils/cluster_utils.conf
 -m	--move <destination ip>	Move the database to another server			
 -p	--promote <slave ip>	promote a slave to master
 -r	--remove <slave ip>	remove this node from the cluster
 -s	--start			start replication
 -t	--stop			stop replication
 -w	--wipe [IP address]	wipe a database from the entire cluster, or
				from a single server if an IP address 
				is specified
"""



class db_server:


    def __init__ (self, host, rt_password):

        try:
            self.conn = MySQLdb.connect(host, 'root', rt_password, '')
            self.curs = self.conn.cursor()
            self.ip_addr = host
        except Exception, e:
            self.ip_addr = host
            print self.ip_addr + " Failed to connect"
            print_exception(e)

#        print self.ip_addr + " Connected successfully"


    def __del__ (self):
        # close the connection to the database
        self.conn.close()
#        print self.ip_addr + " DB Connection closed\n"



    def init_server (self, db_name, usr_pass, slv_pass):
        """
        Initialize a MySQL server as either a master or slave
        """

        self.create_db (db_name)
        self.create_users (usr_pass, slv_pass)



    def create_db(self, db_name):
        """
        Create a database
        """

        create_query = "CREATE DATABASE %s" % db_name
        use_query = "USE %s" % db_name

        try:
            self.curs.execute(create_query)
            self.curs.execute(use_query)
            print self.ip_addr + " Created database " + db_name
        except Exception, e:
            print self.ip_addr + " *** Failed to create database " + db_name
            print_exception(e)




    def create_users (self, user_passwd, slave_passwd):
        """
        Create users for the database
        """

        self.create_user ('db_user', user_passwd)
        self.create_user ('slave_user', slave_passwd)



    def create_user(self, user_name, pass_word):
        """
        Create a database user
        """

        # get settings from config file
        config = ConfigParser.SafeConfigParser ()
        config.read (config_file_path())

        database_name = config.get('cluster_utils','database name')
        control_host = config.get('cluster_utils','control host')
        root_pw = config.get('cluster_utils','root password')

        # I've had some problems creating tables in scripts, 
        # so I'm explicitly granting create privileges

        grant_create_query = "GRANT CREATE, RELOAD ON *.* TO 'root'@'%s' IDENTIFIED BY '%s'" % (control_host, root_pw)
        grant_all_query = "GRANT ALL ON *.* TO 'root'@'%s' IDENTIFIED BY '%s'" % (control_host, root_pw)

        self.curs.execute(grant_create_query)
        self.curs.execute("FLUSH PRIVILEGES")

        create_query = "CREATE USER '%s'@'localhost' IDENTIFIED BY '%s'" % (user_name, pass_word)
        grant_query = "GRANT ALL ON %s.* TO '%s'@'localhost'" % (database_name, user_name)

        try:
            self.curs.execute(create_query)
            print self.ip_addr + " Created user " + user_name
            self.curs.execute(grant_query)
            self.curs.execute("FLUSH PRIVILEGES")
            print self.ip_addr + " Granted privileges"

        except Exception, e:
            print self.ip_addr + " *** Failed to create user " + user_name
            print_exception(e)

        self.curs.execute(grant_all_query)
        self.curs.execute("FLUSH PRIVILEGES")




    def wipe (self, db_name):

        self.drop_user ('slave_user')
        self.drop_user ('db_user')

        self.drop_database (db_name)




    def drop_user (self, username):

        revoke_query = "REVOKE ALL PRIVILEGES FROM '%s'@'localhost'" % username
        drop_query = "DROP USER '%s'@'localhost'" % username

        try:
#            cur.execute(revoke_query)
            self.curs.execute(drop_query)
            print self.ip_addr + " Dropped user " + username
        except Exception, e:
            print self.ip_addr + " *** Failed to drop user " + username
            print_exception(e)



    def drop_database (self, db_name):

        try:
            self.curs.execute("DROP DATABASE my_db")
            print self.ip_addr + " Dropped database " + db_name
        except Exception, e:
            print self.ip_addr + " *** Failed to delete database " + db_name
            print_exception(e)




    def get_master_status (self):
        """
        get the position of the bin log file
        """

        try:
            # get master status
            self.curs.execute("FLUSH TABLES WITH READ LOCK")
            self.curs.execute("SHOW MASTER STATUS")
            master_status = self.curs.fetchall()
            self.curs.execute("UNLOCK TABLES")

            mysql_bin = str(master_status[0][0])
            b_log = str(master_status[0][1])

            print self.ip_addr + " Got master status: " + mysql_bin + ", " + b_log

        except Exception, e:
            print self.ip_addr + " *** Couldn't get master status"
            print_exception(e)
            mysql_bin = ""
            b_log = ""

        return mysql_bin, b_log




    def set_as_master (self, slave_list, slave_password):
        """
        Make a host a master
        """

        # Grant replication rights to slaves
        for host in slave_list:
            self.grant_replication ('slave_user', host, slave_password)




    def grant_replication (self, user_name, ip, password):
        """
        Grant replication privileges on the master to a slave
        """

        query = "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%s' IDENTIFIED BY '%s'" % (user_name, ip, password)

        try:
            self.curs.execute(query)
            self.curs.execute("FLUSH PRIVILEGES")
            print self.ip_addr + " Granted replication privilege to " + user_name + "@" + ip + " id'd by " + password
        except Exception, e:
            print self.ip_addr + " *** Couldn't grant replication privilege to " + user_name
            print_exception(e)




    def demote_master (self, slave_list, slave_password):
        """
        Make a host a slave
        """

        # Grant replication rights to slaves
        for host in slave_list:
            self.revoke_replication ('slave_user', host)



    def revoke_replication (self, user_name, ip):
        """
        Grant replication privileges on the master to a slave
        """

        query = "REVOKE REPLICATION SLAVE ON *.* FROM '%s'@'%s'" % (user_name, ip)

        try:
            self.curs.execute(query)
            self.curs.execute("FLUSH PRIVILEGES")
            print self.ip_addr + " Revoked replication privilege from " + user_name + "@" + ip
        except Exception, e:
            print self.ip_addr + " *** Couldn't revoke replication privilege from " + user_name
            print_exception(e)




    def set_read_only (self):
        """
        make a database server read only
        """

        try:
            self.curs.execute("FLUSH TABLES WITH READ LOCK;")
            self.curs.execute("SET GLOBAL read_only = 1;")
            print self.ip_addr + " Set read only"
        except Exception, e:
            print self.ip_addr + " *** Failed to set database read only" 
            print_exception(e)



    def clear_read_only (self):
        """
        clear read only status from database 
        """

        try:
            self.curs.execute("SET GLOBAL read_only = 0;")
            self.curs.execute("UNLOCK TABLES;")
            print self.ip_addr + " Cleared read only"
        except Exception, e:
            print self.ip_addr + " *** Failed to clear read only status" 
            print_exception(e)



    def use_master(self, master_ip_addr, slave_user_pword, mysqlbin_str, binlog_str):
        """
        tell a slave which server to use as a master
        """

        set_master_query = """CHANGE MASTER TO MASTER_HOST='%s', 
                    MASTER_USER='slave_user', 
                    MASTER_PASSWORD='%s', 
                    MASTER_LOG_FILE='%s', 
                    MASTER_LOG_POS=%s;""" % (master_ip_addr, slave_user_pword, mysqlbin_str, binlog_str)


        print set_master_query

        try:
            # print set_master_query
            self.curs.execute(set_master_query)
            print self.ip_addr + " Set master"
        except Exception, e:
            print self.ip_addr + " Failed to set master"
            print_exception(e)



    def start_slave (self):
        """
        start the slave thread
        """

        try:
            self.curs.execute("START SLAVE")
            print self.ip_addr + " Started slave thread"
        except Exception, e:
            print self.ip_addr + " Failed to start slave"
            print_exception(e)



    def stop_slave (self):
        """
        Stop the slave thread
        """

        try:
            self.curs.execute("STOP SLAVE")
            print self.ip_addr + " Stopped slave thread"
        except Exception, e:
            print self.ip_addr + " Failed to stop slave"
            print_exception(e)



    def reset_slave (self):
        """
        Reset the slave
        """

        try:
            self.curs.execute("RESET SLAVE")
            print self.ip_addr + " Reset slave"
        except Exception, e:
            print self.ip_addr + " Failed to reset slave"
            print_exception(e)



    def import_db (self, file_name, db_name):
        """
        Import data from a file
        """

        import_command = "mysql -h {ip} -u root -p {database} < {file}".format(ip= self.ip_addr, database=db_name, file=file_name)

        os.system (import_command)





class db_cluster:

    def __init__ (self):
        """
        Load settings from config file and from the log
        """

        # get settings from config file
        config = ConfigParser.SafeConfigParser ()
        config.read (config_file_path())

        database_name = config.get('cluster_utils','database name')

        # try to get the master ip from the log
        master_ip = self.get_master_ip ()
#        if master_ip != "":
#            # the master isn't in the log so load it from the config file
#            master_ip = config.get('cluster_utils','master ip')

        control_host = config.get('cluster_utils','control host')

        # try to get the slave ip list from the log
        slave_ip_list = self.update_slave_ip_list ()
        if len(slave_ip_list) == 0:
            # the master isn't in the log so load it from the config file
            temp_slave_ip_list = config.get('cluster_utils','slave ip list')
            temp_slave_ip_list = temp_slave_ip_list.replace (" ", "")
            slave_ip_list = temp_slave_ip_list.split(',')

        root_password = config.get('cluster_utils','root password')
        db_user_pw = config.get('cluster_utils','db user pw')
        slave_user_pw = config.get('cluster_utils','slave user pw')

        self.database_name = database_name
        self.master_ip = master_ip
        self.control_host = control_host
        self.slave_ip_list = slave_ip_list
        self.root_password = root_password
        self.db_user_pw = db_user_pw
        self.slave_user_pw = slave_user_pw




    def init_cluster(self):
        """
        Load settings from config file - ignore the log  - deprecate?
        """

        # get settings from config file
        config = ConfigParser.SafeConfigParser ()
        config.read (config_file_path())

        database_name = config.get('cluster_utils','database name')

        master_ip = config.get('cluster_utils','master ip')
        control_host = config.get('cluster_utils','control host')
        temp_slave_ip_list = config.get('cluster_utils','slave ip list')
        temp_slave_ip_list = temp_slave_ip_list.replace (" ", "")
        slave_ip_list = temp_slave_ip_list.split(',')

        root_password = config.get('cluster_utils','root password')
        db_user_pw = config.get('cluster_utils','db user pw')
        slave_user_pw = config.get('cluster_utils','slave user pw')

        self.database_name = database_name
        self.master_ip = master_ip
        self.control_host = control_host
        self.slave_ip_list = slave_ip_list
        self.root_password = root_password
        self.db_user_pw = db_user_pw
        self.slave_user_pw = slave_user_pw


    def init_master(self):
        server = db_server (self.master_ip, self.root_password)
        server.init_server (self.database_name, self.db_user_pw, self.slave_user_pw)

        self.master_mysqlbin, self.master_binlog = server.get_master_status ()
        self.save_master_bin_log (self.master_ip, self.master_mysqlbin, self.master_binlog, "master")

        server.set_as_master (self.slave_ip_list, self.slave_user_pw)


    def init_slave(self, host):

        print "Init server " + host
        server = db_server (host, self.root_password)
        server.init_server (self.database_name, self.db_user_pw, self.slave_user_pw)

        mysql_bin, bin_log = server.get_master_status ()
        self.save_master_bin_log (host, mysql_bin, bin_log, "slave")

        server.set_read_only()
        # tell the slave which master to use
        server.use_master (self.master_ip, self.slave_user_pw, self.master_mysqlbin, self.master_binlog)



    def wipe (self, db_name):
        """
        Delete the database from each server
        """

        # get settings from config file
        config = ConfigParser.SafeConfigParser ()
        config.read (config_file_path())

        temp_server_list = config.get('cluster_utils','server pool')
        temp_server_list = temp_server_list.replace (" ", "")
        server_list = temp_server_list.split(',')

        for slave in server_list:
            server = db_server (slave, self.root_password)
            server.stop_slave ()
            server.wipe (db_name)

        records_file = open (log_file_path(), 'w')
        records_file.write("")
        records_file.close()




    def add_slave (self, host):

        master_server = db_server (self.master_ip, self.root_password)
        self.master_mysqlbin, self.master_binlog = master_server.get_master_status ()

        master_server.grant_replication ('slave_user', host, self.slave_user_pw)
        print "Initializing new slave"
        self.init_slave(host)


    def remove_slave (self, host):

        self.remove_bin_log(host)

        master_server = db_server (self.master_ip, self.root_password)
        master_server.revoke_replication ('slave_user', host)

        print "Removing slave"
        slave_server =  db_server (host, self.root_password)
        slave_server.stop_slave()
#        slave_server.wipe(self.database_name)




    def print_config (self):

        print "Database name: " + self.database_name
        print "Master server IP: " + self.master_ip
        print "Control Host: " + self.control_host
        print "Slave IP address list:"
        print self.slave_ip_list
        print "Root password: " + self.root_password
        print "User password: " + self.db_user_pw
        print "Slave user password: " + self.slave_user_pw



    def demote_cluster_master (self):

        # Revoke privileges
        master_server = db_server (self.master_ip, self.root_password)

        # should let slaves catch up ??

        # reset all slaves
        for host in self.slave_ip_list:
            server = db_server (host, self.root_password)
            server.reset_slave ()

        master_server.demote_master (self.slave_ip_list, self.slave_user_pw)
        self.update_bin_log_role (self.master_ip, "slave")
        master_server.set_read_only ()
        self.master_ip = ""



    def promote_cluster_slave (self, host):

        self.stop_replication()

        self.master_ip = host
        new_master = db_server (host, self.root_password)
        new_master.clear_read_only()

        self.update_bin_log_role (host, "master")
        self.slave_ip_list = self.update_slave_ip_list ()

        print "Slave IP list:"
        print self.slave_ip_list

        new_master.set_as_master (self.slave_ip_list, self.slave_user_pw)

        self.master_mysqlbin, self.master_binlog = new_master.get_master_status ()

        for host in self.slave_ip_list:
            print host
            slave_server = db_server (host, self.root_password)
            slave_server.use_master (self.master_ip, self.slave_user_pw, self.master_mysqlbin, self.master_binlog)


    def start_replication(self):
        for host in self.slave_ip_list:
            server = db_server (host, self.root_password)
            server.start_slave()



    def stop_replication(self):
        for host in self.slave_ip_list:
            server = db_server (host, self.root_password)
            server.stop_slave()




    def move_db (self, host):

        # make date string
        formatted_date = time.strftime("%Y.%m.%d.%H%M%S")

        dump_file = self.database_name + "_" + formatted_date + ".sql"
        dump_command = "mysqldump -h {host} -u root -p --opt {db_name} --result-file={file_name}".format (host=self.master_ip,db_name=self.database_name, file_name=dump_file)

        print "Dump the database on the master server"
        print dump_command
        subprocess.call (dump_command.split(' '))

        config = ConfigParser.SafeConfigParser ()
        config.read (config_file_path())
        host_username = config.get('cluster_utils','host username')

        print "Move the database file"
        transfer_command = "scp " + dump_file + " " + host_username + "@" + host +":./" + dump_file
        print transfer_command
        subprocess.call (transfer_command.split(' '))

        server = db_server (host, self.root_password)
        server.import_db(dump_file, self.database_name)




    def get_master_ip (self):
        """
        Get the master server's IP address from the log
        """

        records = []

        records_file  = open (log_file_path(), 'r')

        data = records_file.read ()
        records_file.close()

        records = data.split('\n')

        if records[-1] == "":
            del records[-1]

        for record in records:
            field = record.split(':')
            if field[3] == "master":
                return field[0]

        return ""




    def update_bin_log (self, host, mysqlbin, binlog, role):
        """
        Update server record
        """

        print self.ip_addr + " ****** updating bin log"


        records = []

        records_file  = open (log_file_path(), 'r')

        data = records_file.read ()
        records_file.close()

        records = data.split('\n')

        if records[-1] == "":
            del records[-1]

        records_file  = open (log_file_path(), 'w')

        for record in records:
            field = record.split(':')
            if field[0] == host:
                field[1] = mysqlbin
                field[2] = binlog
                field[3] = role

            records_file.write(field[0] + ":" + field[1] + ":" + field[2] + ":" + field[3] + "\n")

        records_file.close()



    def update_slave_ip_list (self):
        """
        Get updated slave list
        """

        records = []
        new_slave_ip_list = []


        records_file  = open (log_file_path(), 'r')
        data = records_file.read ()
        records_file.close()

        records = data.split('\n')

        if records[-1] == "":
            del records[-1]

        for record in records:
            field = record.split(':')
            if field[3] == "slave":
                new_slave_ip_list.append(field[0])

        return new_slave_ip_list





    def update_bin_log_role (self, host, role):
        """
        Update the role field in a host record
        """

        records = []

        records_file  = open (log_file_path(), 'r')

        data = records_file.read ()
        records_file.close()

        records = data.split('\n')

        if records[-1] == "":
            del records[-1]

        records_file  = open (log_file_path(), 'w')

        for record in records:
            field = record.split(':')
            if field[0] == host:
                field[3] = role

            records_file.write(field[0] + ":" + field[1] + ":" + field[2] + ":" + field[3] + "\n")

        records_file.close()





    def get_master_bin_log (self, host):
        """
        Get the bin log info for a host from the log file
        Return the bin log file and its postition
        """

        records = []

        records_file  = open (log_file_path(), 'r')
        records = records_file.readlines ()
        records_file.close()

        for record in records:
            field = record.split(':')
            if field[0] == host:
                return field[1], field[2]

        return "",""




    def save_master_bin_log (self, host, mysqlbin, binlog, role):
        """
        check to see if host has already been saved, if so, update
        """

        mb, bl = self.get_master_bin_log (host)
        if mb == "" and bl == "":
            # this host hasn't been saved yet
            file_obj = open (log_file_path(), 'a')
            file_obj.write (host + ":" + mysqlbin + ":" + binlog + ":" + role + "\n")
            file_obj.close()

        elif mb != mysqlbin or bl != binlog:
#            print "Updating bin log record"
            print "*** bin log record already written for this host"
#            self.update_bin_log (host, mysqlbin, binlog, role)




    def remove_bin_log (self, host):

        records = []

        records_file  = open (log_file_path(), 'r')

        data = records_file.read ()
        records_file.close()

        records = data.split('\n')

        if records[-1] == "":
            del records[-1]

        records_file  = open (log_file_path(), 'w')

        for record in records:
            field = record.split(':')
            if field[0] != host:
                records_file.write(field[0] + ":" + field[1] + ":" + field[2] + ":" + field[3] + "\n")

        records_file.close()





def print_exception(e):
    """
    print exception messages
    """

    line_number = sys.exc_info()[2].tb_lineno
    print "Line: " + str(line_number)
    print e







def main(argv):
    """
    the program starts here
    """

    slave_ip = None

    # get command line options
    try:

        opts, args = getopt.getopt(argv, "a:chidm:p:r:stww:", ["add=", "config", "help", "init", "demote", "move=", "promote=", "remove=" "start", "stop", "wipe", "wipe="])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    # check option
    for opt, arg in opts:

        if opt in ("-h", "--help"):
            usage ()


        if opt in ("-i", "--init"):

            # initialize the cluster
            cluster = db_cluster()
            cluster.init_cluster()
            cluster.print_config ()

            cluster.init_master()
            for host in cluster.slave_ip_list:
                cluster.init_slave(host)


        if opt in ("-d", "--demote"):

            # demote the master to a slave
            cluster = db_cluster()
            cluster.stop_replication ()
            cluster.demote_cluster_master()


        if opt in ("-m", "--move"):
            cluster = db_cluster ()
            cluster.move_db (arg)


        if opt in ("-p", "--promote"):

            # make a slave a master
            cluster = db_cluster()

            cluster.promote_cluster_slave (arg)
            cluster.start_replication ()

            cluster.print_config ()
            print "New master IP: " + arg

        if opt in ("-a", "--add"):

            # add a node to the cluster
            cluster = db_cluster()
            cluster.add_slave(arg)

            print "New slave IP: " + arg

        if opt in ("-s", "--start"):

            # start replication
            cluster = db_cluster()
            cluster.slave_ip_list = cluster.update_slave_ip_list ()

            for slave in cluster.slave_ip_list:
                server = db_server (slave, cluster.root_password)
                server.start_slave()


        if opt in ("-t", "--stop"):

            # stop replication
            cluster = db_cluster()
            cluster.slave_ip_list = cluster.update_slave_ip_list ()

            for slave in cluster.slave_ip_list:
                server = db_server (slave, cluster.root_password)
                server.stop_slave()


        if opt in ("-c", "--config"):

            # print cluster configuration information
            cluster = db_cluster()
            cluster.print_config ()
            print ""

            records_file = open (log_file_path(), 'r')
            log_file = records_file.read()
            records_file.close()
            print log_file


        if opt in ("-r", "--remove"):

            # remove a slave from the cluster

            print "Remove node: " + arg
            cluster = db_cluster()
            cluster.remove_slave (arg)


        if opt in ("-w", "--wipe"):

            # get settings from config file
            config = ConfigParser.SafeConfigParser ()
            config.read (config_file_path())

            database_name = config.get('cluster_utils','database name')

            cluster = db_cluster()
            cluster.init_cluster()

            if arg == "":
                print "Wipe the cluster"

                answer = raw_input ("Are you sure you want to wipe database " + database_name + "? (y/n): ")
                if answer == 'y':
                    cluster.stop_replication ()
                    cluster.wipe (cluster.database_name)
            else:
                answer = raw_input ("Are you sure you want to wipe database " + database_name + "from " + arg + "? (y/n): ")
                if answer == 'y':
                    server = db_server (arg, cluster.root_password)
                    server.stop_slave ()
                    server.wipe(cluster.database_name)



if __name__ == "__main__":
    main(sys.argv[1:])





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



