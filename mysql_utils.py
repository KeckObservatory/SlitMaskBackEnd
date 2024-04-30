import pymysql
from pymysql.cursors import DictCursor

def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host="mysqlserver.keck.hawaii.edu",
            port=0,
            user="keckOps",
            password="spOkcek",
            database="keckOperations"
        )
        return connection
    except pymysql.Error as e:
        print("Error connecting to MySQL:", e)
        return None


def query_observers():
    connection = connect_to_mysql()
    observers_data = None
    try:
        with connection.cursor(cursor=DictCursor) as mysql_cursor:
            # select Id, Firstname, Lastname from observers order by Id asc;
            query = "SELECT Id as keckid, Firstname, Lastname, Email, Affiliation, AllocInst FROM observers ORDER BY Id ASC"
            mysql_cursor.execute(query, ())
            observers_data = mysql_cursor.fetchall()
    except pymysql.Error as e:
        print("Error connecting to observer database:", e)

    return observers_data
