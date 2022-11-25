import mysql.connector
from mysql.connector import Error
import pandas as pd


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
    mycursor = db.cursor()

    pre_id = 10000
    pre_name = ''
    # last_id = mycursor.lastrowid

    # DELETE FROM METADATA WHERE file_name='test.txt'

    # mycursor.execute('''
    #                 DELETE FROM METADATA
    #                 WHERE file_name='test.txt'
    #                ''')
    # db.commit()

    # metad = read_query(db, "SELECT *FROM METADATA;")
    for index, value in enumerate(cmd):
        sql_search = meta_data + 'file_name' + ' = "' + value + '"'
        get_data = read_query(db, sql_search)
        if not get_data:
            sql = 'INSERT INTO METADATA(file_type, file_name, has_child) VALUES (%s, %s, %s)'
            sql3 = 'UPDATE PARENT_CHILD SET child_id = %s WHERE parent_id = %s'

            if "." in value:
                if index == len(cmd):
                    val = ("FILE", value, 0)
                else:
                    val = ("FILE", value, 1)
            if "." not in value:
                if index == len(cmd):
                    val = ("DICTECTORY", value, 0)
                else:
                    val = ("DICTECTORY", value, 1)

            mycursor.execute(sql, val)
            db.commit()
            cur_id = mycursor.lastrowid
            print("1 record inserted, ID:", cur_id)

            sql2 = 'INSERT INTO PARENT_CHILD VALUES(%s, %s);'
            val2 = (cur_id, '')
            mycursor.execute(sql2, val2)
            db.commit()
            print("1 record inserted, ID:", mycursor.lastrowid)

            if pre_id != 10000:
                child_sql = 'SELECT child_id FROM PARENT_CHILD WHERE parent_id='
                child_sql += str(pre_id)
                # print(child_sql)
                child_data = read_query(db, child_sql)
                if child_data[0][0] != '':
                    c_d = child_data[0][0] + "," + str(cur_id)

                if child_data[0][0] == '':
                    c_d += str(cur_id)
                # print(c_d)
                val3 = (c_d, pre_id)
                mycursor.execute(sql3, val3)
                db.commit()
                pre_id = cur_id
                print("1 record inserted, ID:", mycursor.lastrowid)
                continue
                # val3 = (pre_id, )
        else:
            pre_id = get_data[0][0]
            pre_name = get_data[0][2]


def rm(db, cmd):
    cmd = cmd.strip().split('/')
    meta_data = 'SELECT id FROM METADATA WHERE file_name='
    mycursor = db.cursor()
    r_name = cmd[-1]
    meta_data += "'" + r_name + "'"
    data = read_query(db, meta_data)
    get_id = data[0][0]
    d_sql = 'DELETE FROM METADATA WHERE id='
    d_sql2 = 'DELETE FROM PARENT_CHILD WHERE id='
    d_sql += str(get_id) + ";"
    d_sql2 += str(get_id) + ";"
    mycursor.execute(d_sql)
    mycursor.execute(d_sql2)
    print("DONE")


def put(db, filename, path):
    mycursor = db.cursor()
    if filename == "case_id.csv":
        df = pd.read_csv(path)
        partitions = '"case_20","case_21"'

        mycursor.execute('''DROP TABLE case_20;''')
        mycursor.execute('''DROP TABLE case_21;''')

        create_sql = '''CREATE TABLE case_20 (
                          id INTEGER AUTO_INCREMENT PRIMARY KEY,
                          case_id TEXT NOT NULL,
                          db_year TEXT NOT NULL
                        );'''

        create2_sql = '''CREATE TABLE case_21 (
                                  id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                  case_id TEXT NOT NULL,
                                  db_year TEXT NOT NULL
                                );'''

        mycursor.execute(create_sql)
        mycursor.execute(create2_sql)
        for i, row in df.iterrows():
            if row['db_year'] == 2020:
                insert_sql = 'INSERT INTO case_20 (case_id, db_year) VALUES (%s, %s);'
                # val = (str(data["case_id"]), str(str(data["db_year"])))
                mycursor.execute(insert_sql, tuple(row))
                continue
            if row['db_year'] == 2021:
                insert_sql = 'INSERT INTO case_21 (case_id, db_year) VALUES (%s, %s);'
                # val = (str(data["case_id"]), str(str(data["db_year"])))
                mycursor.execute(insert_sql, tuple(row))
                continue
        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=1'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (1, filename, partitions))
        print("DONE!")

    if filename == "collision.csv":
        df = pd.read_csv(path, encoding='latin1')
        partitions = 'collision.csv, "coll_20","coll_21","coll_22"'
        mycursor.execute('''DROP TABLE coll_20;''')
        mycursor.execute('''DROP TABLE coll_21;''')
        mycursor.execute('''DROP TABLE coll_22;''')

        mycursor.execute('''CREATE TABLE coll_20(
                            id INTEGER AUTO_INCREMENT PRIMARY KEY,
                            case_id TEXT NOT NULL,
                            collision_date TEXT NOT NULL
                        
                        );''')

        mycursor.execute('''CREATE TABLE coll_21(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    case_id TEXT NOT NULL,
                                    collision_date TEXT NOT NULL

                                );''')

        mycursor.execute('''CREATE TABLE coll_22(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    case_id TEXT NOT NULL,
                                    collision_date TEXT NOT NULL

                                );''')
        # print(df)
        for i, row in df.iterrows():
            if (row['collision_date'] > "2020-1-1") & (row['collision_date'] < "2021-1-1"):
                insert_sql = 'INSERT INTO coll_20 (case_id, collision_date) VALUES (%s, %s);'
                val = (str(row["case_id"]), str(row["collision_date"]))
                mycursor.execute(insert_sql, val)
                continue

            if (row['collision_date'] > "2021-1-1") & (row['collision_date'] < "2022-1-1"):
                insert_sql = 'INSERT INTO coll_21 (case_id,collision_date) VALUES (%s,%s);'
                val = (str(row["case_id"]), str(row["collision_date"]))
                mycursor.execute(insert_sql, val)
                continue

            if (row['collision_date'] > "2022-1-1") & (row['collision_date'] < "2023-1-1"):
                insert_sql = 'INSERT INTO coll_22 (case_id,collision_date) VALUES (%s,%s);'
                val = (str(row["case_id"]), str(row["collision_date"]))
                mycursor.execute(insert_sql, val)
                continue
        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=2'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (2, filename, partitions))
        print("DONE!")

    if filename == "LA_County_COVID_Cases.csv":
        df = pd.read_csv(path)
        partitions = '"LA_County_COVID_Cases.csv","covid_20","covid_21","covid_22"'
        mycursor.execute('''DROP TABLE covid_20;''')
        mycursor.execute('''DROP TABLE covid_21;''')
        mycursor.execute('''DROP TABLE covid_22;''')
        mycursor.execute('''CREATE TABLE covid_20(
                            id INTEGER AUTO_INCREMENT PRIMARY KEY,
                            date TEXT NOT NULL
                        
                        );''')

        mycursor.execute('''CREATE TABLE covid_21(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    date TEXT NOT NULL

                                );''')

        mycursor.execute('''CREATE TABLE covid_22(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    date TEXT NOT NULL

                                );''')

        for i, row in df.iterrows():
            if (row['date'] > "2020-1-1") & (row['date'] < "2021-1-1"):
                insert_sql = 'INSERT INTO covid_20 (date) VALUES (%s);'
                val = (str(row["date"]))
                mycursor.execute(insert_sql, val)
                continue

            if (row['date'] > "2021-1-1") & (row['date'] < "2022-1-1"):
                insert_sql = 'INSERT INTO covid_21 (date) VALUES (%s);'
                val = (str(row["date"]))
                mycursor.execute(insert_sql, val)
                continue

            if (row['date'] > "2022-1-1") & (row['date'] < "2023-1-1"):
                insert_sql = 'INSERT INTO covid_22 (date) VALUES (%s);'
                val = (str(row["date"]))
                mycursor.execute(insert_sql, val)
                continue
        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=3'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (3, filename, partitions))
        print("DONE!")

    if filename == "LA_Weather.csv":
        df = pd.read_csv(path)
        partitions = '"LA_Weather.csv","weather_19","weather_20","weather_21","weather_22"'

        mycursor.execute('''DROP TABLE weather_19;''')
        mycursor.execute('''DROP TABLE weather_20;''')
        mycursor.execute('''DROP TABLE weather_21;''')
        mycursor.execute('''DROP TABLE weather_22;''')
        mycursor.execute('''CREATE TABLE weather_19(
                            id INTEGER AUTO_INCREMENT PRIMARY KEY,
                            date TEXT NOT NULL
                        );''')

        mycursor.execute('''CREATE TABLE weather_20(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    date TEXT NOT NULL
                                );''')

        mycursor.execute('''CREATE TABLE weather_21(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    date TEXT NOT NULL
                                );''')

        mycursor.execute('''CREATE TABLE weather_22(
                                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                    date TEXT NOT NULL
                                );''')

        for i, row in df.iterrows():
            # row["datetime"] = row["datatime"].strip().split(" ")
            # print(str(row["datetime"]))
            if (row['datetime'] > "2019-1-1") & (row['datetime'] < "2020-1-1"):
                insert_sql = 'INSERT INTO weather_19 (date) VALUES (%s);'
                row['datetime'] = pd.to_datetime(row.datetime)
                val = (str(row['datetime']), )
                mycursor.execute(insert_sql, val)
                continue

            if (row['datetime'] > "2020-1-1") & (row['datetime'] < "2021-1-1"):
                insert_sql = 'INSERT INTO weather_20 (date) VALUES (%s);'
                row['datetime'] = pd.to_datetime(row.datetime)
                val = (str(row['datetime']), )
                mycursor.execute(insert_sql, val)
                continue

            if (row['datetime'] > "2021-1-1") & (row['datetime'] < "2022-1-1"):
                insert_sql = 'INSERT INTO weather_21 (date) VALUES (%s);'
                row['datetime'] = pd.to_datetime(row.datetime)
                val = (str(row['datetime']), )
                mycursor.execute(insert_sql, val)
                continue

            if (row['datetime'] > "2022-1-1") & (row['datetime'] < "2023-1-1"):
                insert_sql = 'INSERT INTO weather_22 (date) VALUES (%s);'
                row['datetime'] = pd.to_datetime(row.datetime)
                val = (str(row['datetime']), )
                mycursor.execute(insert_sql, val)
                continue
        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=4'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (4, filename, partitions))
        print("DONE!")

    if filename == "parties.csv":
        df = pd.read_csv(path)
        partitions = '"parties.csv", "at_fault_0", "at_fault_1"'
        mycursor.execute('''DROP TABLE at_fault_0;''')
        mycursor.execute('''DROP TABLE at_fault_1;''')
        mycursor.execute('''CREATE TABLE at_fault_0(
                            id INTEGER AUTO_INCREMENT PRIMARY KEY,
                            case_id TEXT NOT NULL,
                            at_fault TEXT NOT NULL
                        );''')

        mycursor.execute('''CREATE TABLE at_fault_1(
                            id INTEGER AUTO_INCREMENT PRIMARY KEY,
                            case_id TEXT NOT NULL,
                            at_fault TEXT NOT NULL
                        );''')

        for i, row in df.iterrows():
            if row['at_fault'] == 0:
                insert_sql = 'INSERT INTO at_fault_0 (case_id, at_fault) VALUES (%s, %s);'
                val = (str(row["case_id"]), str(row["at_fault"]))
                mycursor.execute(insert_sql, val)
                continue

            if row['at_fault'] == 1:
                insert_sql = 'INSERT INTO at_fault_1 (case_id, at_fault) VALUES (%s, %s);'
                val = (str(row["case_id"]), str(row["at_fault"]))
                mycursor.execute(insert_sql, val)
                continue

        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=5'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (5, filename, partitions))
        print("DONE!")

    if filename == "victims.csv":
        df = pd.read_csv(path)
        partitions = '"victims.csv", "victim"'
        mycursor.execute('''DROP TABLE victim;''')
        mycursor.execute('''CREATE TABLE victim(
                                id INTEGER AUTO_INCREMENT PRIMARY KEY,
                                case_id TEXT NOT NULL,
                                victim_role TEXT NOT NULL,
                                victim_sex TEXT NOT NULL,
                                victim_age TEXT NOT NULL
                                
                            );''')

        for i, row in df.iterrows():
            insert_sql = 'INSERT INTO victim (case_id,victim_role, victim_sex,victim_age) VALUES (%s,%s, %s, %s);'
            val = (str(row["case_id"]), str(row["victim_role"]), str(row["victim_sex"]), str(row["victim_age"]))
            mycursor.execute(insert_sql, val)

        src_sql = 'INSERT INTO SOURCE (file_id, file_name, location) VALUES(%s, %s, %s);'
        d_sql2 = 'DELETE FROM SOURCE WHERE file_id=6'
        mycursor.execute(d_sql2)
        mycursor.execute(src_sql, (6, filename, partitions))
        print("DONE!")


def getPartitionLocations(db, file):
    filename = ["case_id.csv", "collision.csv", "LA_County_COVID_Cases.csv", "LA_Weather.csv", "parties.csv", "victims.csv"]
    if file not in filename:
        return "FILE NOT FOUND!"
    else:
        source_data = 'SELECT  location FROM SOURCE WHERE file_name='
        source_data += '"' + file + '"'
        # print(meta_data)
        data = read_query(db, source_data)
        return data[0][0]


def readPartition(db, file, partition):
    filename = ["case_id.csv", "collision.csv", "LA_County_COVID_Cases.csv", "LA_Weather.csv", "parties.csv",
                "victims.csv"]
    if file not in filename:
        return "FILE NOT FOUND!"
    else:
        partition = partition.strip().split(",")
        for name in partition:
            name = name.strip('"')
            data = 'SELECT *FROM '
            data = data + name + ";"
            get_data = read_query(db, data)
            return get_data


def cat(cmd):
    try:
        f = open(cmd, "r")
        return f.read()
    except IOError:
        print("Value not found in file")


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
# data = read_query(db, meta_data)
# result = put(db, "case_id.csv", "/Users/ellachen/PycharmProjects/pythonProject/data/case_id.csv")
# data = read_query(db, source_data)
# result = getPartitionLocations(db, 'case_id.csv')
# result = readPartition(db, "case_id.csv", '"case_20","case_21"')
# result = cat("/Users/ellachen/PycharmProjects/pythonProject/data/case_id.csv")
# print(result)
