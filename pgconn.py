# we need access to PostgreSQL
import psycopg2

# we want the dictionary cursor
import psycopg2.extras

from psycopg2.extras import DictCursor

# get the logger object
from logger_utils import get_log


class PgConn:

    def __init__(self):
        self.cursor = None
        self.conn = None
        self.msg = ""
        self.log = get_log()

    def get_conn(self):
        return self.conn

    def get_dict_curse(self):
        return self.conn.cursor(cursor_factory=DictCursor)

    def disconnect(self):

        self.msg = ""

        if self.cursor != None:
            try:
                self.cursor.close()
            except Exception as e:
                self.log.error(f"failed cursor close: exception class "
                          f"{e.__class__.__name__}: {e}")
                self.msg += "db close failed\n"
            else:
                self.cursor = None

        if self.conn != None:
            try:
                self.conn.close()
            except Exception as e:
                self.log.error(f"failed connection close: exception class "
                          f"{e.__class__.__name__}: {e}")
                self.msg += "db disconnect failed\n"
            else:
                self.conn = None

    # end def disconnect()

    def connect(self, host, port, dbname, user, password):
        print('connecting')

        # this becomes a postgresql libpq connection string
        conn_string = "host='%s' port=%s dbname='%s' user='%s' password='%s'"

        ################################################
        user = 'dbadmin'
        if (self.conn) != None:
            self.log.warning("already connected")
        else:
            # get a connection
            try:
                self.conn = psycopg2.connect(conn_string % (host, port, dbname, user, password))
            except Exception as e:
                self.log.info(f'connection params: {host}, {port}, {dbname}, {user}, {password}')
                self.log.error(f"failed connect: exception class"
                          f"{e.__class__.__name__}: {e}" )
                self.msg += "db connect failed\n"
                return False
            # end try connect
        # end if self.conn

        #self.log.error("DEBUG self.conn.encoding %s" % (self.conn.encoding))

        if (self.cursor) != None:
            self.log.warning( "already have cursor" )
        else:
            # get a cursor
            try:
                # get a cursor that returns columns by name
                self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            except Exception as e:
                self.log.error(
                "failed cursor create: exception class %s: %s"
                % (e.__class__.__name__, e) )
                self.msg += "db open failed\n"
                return False
            # end try cursor
        # end if self.cursor

        return True

    # end def connect()

# end class PgConn

