import pymysql
from pymysql.cursors import DictCursor


def connect_to_mysql(sql_params):
    db = sql_params['db']
    dbhost = sql_params['server']
    user = sql_params['user']
    password = sql_params['pwd']

    try:
        connection = pymysql.connect(
            host=dbhost,
            port=0,
            user=user,
            password=password,
            database=db
        )
        return connection
    except pymysql.Error as e:
        print("Error connecting to MySQL:", e)
        return None


def query_observers(sql_params):
    connection = connect_to_mysql(sql_params)
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
