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



def print_exception(e):
    """
    print exception messages
    """

    line_number = sys.exc_info()[2].tb_lineno
    print "Line: " + str(line_number)
    print e





def query (conn, curs, query):

    try:

        curs.execute (query)

        conn.commit()

        print "Done"

    except Exception, e:
        conn.rollback()
        print "Query Failed: " + query
        print_exception(e)





def main(argv):
    """
    the program starts here
    """

    conn = MySQLdb.connect('192.168.0.35', 'root', 'T4nk3r!12', 'my_db')
    curs = conn.cursor()

    # create tables
    query(conn, curs, """CREATE TABLE pages (
                            path TEXT, 
                            title TEXT, 
                            category TEXT,
                            description TEXT,
                            tags TEXT,
                            status TEXT,
                            pubdate DATETIME, 
                            updated DATETIME
                          )""")


    query(conn, curs, """CREATE TABLE categories (
                            name TEXT,
                            path TEXT,
                            description TEXT,
                            tags TEXT,
                            status TEXT,
                            pubdate DATETIME, 
                            updated DATETIME
                          )""")


    query(conn, curs, """CREATE TABLE users (
                            username TEXT, 
                            password TEXT,
                            email_addr TEXT,
                            joined DATETIME
                          )""")


    query(conn, curs, "INSERT INTO users VALUES ('Anna', '123456', 'anna@email.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Bob', '333456', 'bob@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Claire', '444456', 'claire@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Dave', '555456', 'dave@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Emma', '666456', 'emma@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Fred', '111456', 'fred@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Gwen', '222456', 'gwen@addr.com', CURRENT_DATE());")
    query(conn, curs, "INSERT INTO users VALUES ('Harry', '123123', 'harry@mail.com', CURRENT_DATE());")


    # close the connection to the database
    conn.close()
    print "DB Connection closed\n"



if __name__ == "__main__":
    main(sys.argv[1:])

