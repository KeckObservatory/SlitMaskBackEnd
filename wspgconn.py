# PostgreSQL connection using information stored in session
# it attempts to create an authenticated connection, but
# if that fails it tries to create a readonly connection

from mask_constants import MASK_ADMIN, MASK_USER, MASK_LOGIN, USER_TYPE_STR

# protected file with database names and passwords
import wspgcfg_live as wspgcfg

# parent class
from pgconn import PgConn


class WsPgConn(PgConn):

    def __init__(self, keck_id):
        super(WsPgConn, self).__init__()

        if keck_id == MASK_ADMIN:
            self.user_type = MASK_ADMIN
        else:
            self.user_type = self.set_user_type(keck_id)

    def db_connect(self):
        db_pw = wspgcfg.pwdict[USER_TYPE_STR[self.user_type]]
        host = wspgcfg.host
        port = wspgcfg.port
        dbname = wspgcfg.dbname
        self.connect(host, port, dbname, self.user_type, db_pw)

        return True

    def get_user_type(self):
        return self.user_type

    def set_user_type(self, keck_id):
        user_type = MASK_USER
        db_pw = wspgcfg.pwdict[USER_TYPE_STR[user_type]]
        host = wspgcfg.host
        port = wspgcfg.port
        dbname = wspgcfg.dbname

        self.log.debug(f"connect host {host} port {port} dbname {dbname} "
                       f"dbuser {user_type} dbpass {db_pw}")

        if not self.connect(host, port, dbname, user_type, db_pw):
            self.log.error('code error,  could not connect to db.')

        query = "select obid, pass, privbits from observers where keckid=%s;"

        try:
            self.cursor.execute(query, (keck_id,))
        except Exception as e:
            self.log.error(f"query keck_id {keck_id} failed: "
                           f"{self.cursor.query}: {e.__class__.__name__}: {e}")
            return None

        count = self.cursor.rowcount

        # user not in the mask observer table,  but in the Keck Observer table
        if count < 1:
            return user_type
        elif count > 1:
            # we require that email be unique in database table observers
            self.log.error(f"keck_id {keck_id} returned more than one record: "
                           f"{self.cursor.query}: {e.__class__.__name__}, {e}")
            return None

        result = self.cursor.fetchone()
        self.disconnect()

        user_type = self.determine_user_type(result)

        return user_type

    def determine_user_type(self, result):
        MASKADMIN = 1 << 0
        if result['privbits'] & MASKADMIN:
            return MASK_ADMIN
        else:
            return MASK_USER

