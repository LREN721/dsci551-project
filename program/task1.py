import mysql.connector
from mysql.connector import Error


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def connect_dataset(user, password, host, database):
    cnx = mysql.connector.connect(user=user, password=password,
                                  host=host,
                                  database=database)
    return cnx


def ls(db, cmd):
    meta_data = 'SELECT *FROM METADATA WHERE '
    parent_data = 'SELECT *FROM PARENT_CHILD WHERE '
    cmd = cmd.strip().split('/')
    type = cmd[-1]
    sql_search = meta_data + 'file_name' + ' LIKE "%' + type + '%"'
    metad = read_query(db, sql_search)
    # print(metad)
    if metad[0][-1] == 1:
        get_id = metad[0][0]
        sql_search_two = parent_data + 'child_id' + ' LIKE "%' + str(get_id) + '%"'
        # print(sql_search_two)
        get_data = read_query(db, sql_search_two)
        # print(get_data)
        display = ""
        for index, value in enumerate(get_data):
            num = value[1].strip().split(',')
            num = num[1].strip()
            sql_search_display = meta_data + 'id' + ' LIKE "%' + num + '%"'
            # print(sql_search_display)
            dis_data = read_query(db, sql_search_display)
            display += dis_data[0][2] + " "
        return display

    else:
        return ""


def mkdir(db, cmd):
    meta_data = 'SELECT *FROM METADATA WHERE '
    cmd = cmd.strip().split('/')
    getid = 0
    mycursor = db.cursor()
    last_id = mycursor.lastrowid
    for value in cmd:
        sql_search = meta_data + 'file_name' + ' = "' + value + '"'
        get_data = read_query(db, sql_search)
        if not get_data:
            sql = 'INSERT INTO METADATA(id, file_type, file_name, has_child) VALUES (%s, %s, %s, %s)'
            if "." in value:
                val = (last_id, "FILE", value, 1)
            else:
                val = (last_id,"DICTECTORY", value, 0)
            mycursor.execute(sql, val)
            db.commit()
            print("1 record inserted, ID:", mycursor.lastrowid)
        else:
            getid = get_data[0][0]

    return


def rm(db, cmd):
    return


user = 'root'
password = ''
host = 'localhost'
database = 'dsci551'
db = connect_dataset(user, password, host, database)
meta_data = """SELECT *FROM METADATA;"""


# parent_data = """SELECT *FROM PARENT_CHILD;"""
# source_data = """SELECT *FROM SOURCE;"""
# metad = read_query(cnx, meta_data)

# result = ls(db, "ls /")
# print(result)
result = mkdir(db, "/user/test.txt")
data = read_query(db, meta_data)



