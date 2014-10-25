DB Cluster Utils is a tool for managing a cluster of MySQL database servers.  It works with MySQL 5.5. Support for MySQL 5.6 is planned eventually.

Read more at http://banoffeepiserver.com/mysql/python-database-cluster-management-utility/. 
<pre>
<code>
Usage:
db_cluster_utils.py -option [argument]
 -a	--add slave ip			add a new slave to the cluster
 -c	--config				view cluster configuration information
 -d	--demote				demote a master to a slave
 -h	--help					display this help message
 -i	--init					initialize the cluster based on settings 
						    in /etc/db_cluster_utils/cluster_utils.conf
 -m	--move destination ip	Move the database to another server	
 -p	--promote slave ip		promote a slave to master
 -r	--remove slave ip		remove this node from the cluster
 -s	--start				    start replication
 -t	--stop				    stop replication
 -w	--wipe [IP address]	    wipe a database from the entire cluster, or
						    from a single server if an IP address 
						    is specified
</code>
</pre>
This program is released under the GNU public license.
