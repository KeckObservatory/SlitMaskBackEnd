# configure passwords for limited access roles
# used by slitmask admins and the slitmask web interface

# therefore this file must be protected such that it is only
# accessible to the developers who will have to use it.

# We know that the way the Keck SVN works this all moot
# because there is no way to restrict who can read this.
# Anyone who can check out from SVN will see this, and
# that file when checked out will likely be world readable.

# we suppose that those devs will be part of a group
# we suppose that the web server will be part of that group

# during development we name that group "spgcgi"

# after checkout this file should have its permissions
# rearranged by executing
#       chgrp spgcgi wspgcfg.py
#       chmod g+r wspgcfg.py
# where it is expected that the web server runs with group spgcgi

########################################################

# easy way to get host name information
import socket

hostname = socket.gethostname()

########################################################

if hostname == "geneva.ucolick.org":
    host        = "localhost"
    port        = 5432
    dbname      = "metabase"
    dbuser      = 'masklogin'
    pwdict      = {
        'masklogin'         : 'notreallythelpwd',
        'maskreader'        : 'notreallytherpwd',
        'maskuser'          : 'notreallytheupwd',
        'maskchecker'       : 'notreallythecpwd',
        'maskmiller'        : 'notreallythempwd',
        'maskadmin'         : 'notreallytheapwd',
    }
    dbpasw      = pwdict[dbuser]
else:
    # host        = "vm-hqslitmaskdb01.keck.hawaii.edu"
    host = "localhost"
    port = 5432
    dbname = "metabase"
    dbuser = 'masklogin'
    pwdict = {
        'masklogin': 'plate2sda',
        'maskreader': 'plate2sda',
        'maskuser': 'plate2sda',
        'maskchecker': 'plate2sda',
        'maskmiller': 'plate2sda',
        'maskadmin': 'plate2sda',
        'dbadmin': 'plate2sda',
    }
    dbpasw = pwdict[dbuser]
# end if
