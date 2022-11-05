import pandas as pd
from IPython.display import display
from pymongo import MongoClient


def dir_exist_check(db, command, last):
    conn = db.file
    com_list = list(filter(None, command.split('/')))
    # if last is True, we will count last dir into check list, if not we will ignore it(usually for mkdir)
    dr = ''
    if last:
        for i in com_list:
            dr = dr + i + '.'
    else:
        for i in com_list[0:-1]:
            dr = dr + i + '.'
    # get rid of last comma
    dr = dr[0:-1]

    find = conn.find({dr: {"$exists": True}})
    if len(list(find)) > 0:
        return True
    else:
        return False


# check if directory is valid
def command_vali_check(command):
    dir = command.split()[1]
    if len(command.split()) >= 3:
        return False
    elif dir.startswith("/") == False:
        return False
    else:
        return True


# connect with MongoDB
def connect():
    try:
        conn = MongoClient("mongodb://localhost:27017/")
        print("Connected successfully!!")
    except:
        print("Could not connect to MongoDB")

    # database name: edfs
    db = conn.edfs

    # Created or Switched to collection names: file
    # connection = db.file

    return db


# make command transfer to path in mongodb
def path_transfer(cmd):
    com_list = list(filter(None, cmd.split('/')))
    # dr in mongodb format
    dr = ''
    for i in com_list:
        dr = dr + i + '.'

    # get rid of last dot
    dr = dr[0:-1]

    return dr


# make directory
def mkdir(db, cmd):
    conn = db.file
    dr = path_transfer(cmd)
    # get id of root and then update into the file
    root_id = conn.find_one()["_id"]
    conn.update_one({"_id": root_id}, {"$set": {dr: {}}})
    print("done")


def ls(db, cmd):
    conn = db.file
    dr = path_transfer(cmd)
    root_id = conn.find_one()["_id"]
    all_path_list = conn.find({"_id": root_id}, {dr: 1, "_id": 0})

    # get directory as list, dr: Root.user , dr_list: [Root, user]
    dr_list = list(filter(None, dr.split('.')))
    # transfer from cursor to list and get first index value( is a dictionary)
    all_path = list(all_path_list)[0]
    # eg input: ls /user
    # eg output from ls command: {'Root': {'user': {'ss': {}, 'abc': {}}}}
    output_dit = all_path
    for i in dr_list:
        output_dit = output_dit[i]

    # finally output_dit get into final dict to output
    for i in output_dit:
        print(i)


def cat(db, path):
    conn = db.file
    dr = path_transfer(path)
    root_id = conn.find_one()["_id"]
    all_path_list = conn.find({"_id": root_id}, {dr: 1, "_id": 0})

    dr_list = list(filter(None, dr.split('.')))
    all_path = list(all_path_list)[0]

    output_list = all_path
    for i in dr_list:
        output_list = output_list[i]

    # print
    for i in output_list:
        conn = db[i]
        output = conn.find()
        print(list(output))


def rm(db, cmd):
    conn = db.file
    dr = path_transfer(cmd)
    root_id = conn.find_one()["_id"]
    # db.file.update({"_id": ObjectId("63434c7707d3d5baf84098d0")}, { $unset: {"Root.user": 1}})
    conn.update_one({"_id": root_id}, {"$unset": {dr: 1}})
    print("done")


def put(db, filename, path):
    path = "/Root" + path
    dr = path_transfer(path)

    if filename == "case_id.csv":
        dr = dr + ".case_id"
        partitions = ["case_20",
                      "case_21"]
        df = pd.read_csv("../data/case_id.csv", chunksize=1000)

        # drop existing table to avoid redundancy
        db.case_20.drop()
        db.case_21.drop()

        for trunk in df:
            df_20 = trunk[trunk['db_year'] == 2020]
            df_21 = trunk[trunk['db_year'] == 2021]

            dict_20 = df_20.to_dict("records")
            dict_21 = df_21.to_dict("records")

            # inserting
            if len(dict_20) > 0:
                conn = db.case_20
                conn.insert_many(dict_20)

            if len(dict_21) > 0:
                conn = db.case_21
                conn.insert_many(dict_21)

        print("done")

    elif filename == "collision.csv":
        dr = dr + ".collisions"
        partitions = ["coll_20",
                      "coll_21",
                      "coll_22"]

        df = pd.read_csv("../data/collision.csv", chunksize=1000, encoding='latin1')

        # drop existing table to avoid redundancy
        db.coll_20.drop()
        db.coll_21.drop()
        db.coll_22.drop()
        for trunk in df:
            trunk['collision_date'] = pd.to_datetime(trunk.collision_date)
            df_20 = trunk[(trunk['collision_date'] > "2020-1-1") & (trunk['collision_date'] < "2021-1-1")]
            df_21 = trunk[(trunk['collision_date'] > "2021-1-1") & (trunk['collision_date'] < "2022-1-1")]
            df_22 = trunk[(trunk['collision_date'] > "2022-1-1") & (trunk['collision_date'] < "2023-1-1")]

            dict_20 = df_20.to_dict("records")
            dict_21 = df_21.to_dict("records")
            dict_22 = df_22.to_dict("records")

            # inserting

            if len(dict_20) > 0:
                conn = db.coll_20
                conn.insert_many(dict_20)

            if len(dict_21) > 0:
                conn = db.coll_21
                conn.insert_many(dict_21)

            if len(dict_22) > 0:
                conn = db.coll_22
                conn.insert_many(dict_22)

        print("done")
    elif filename == "LA_County_COVID_Cases.csv":
        dr = dr + ".LA_County_COVID_Cases"
        partitions = ["covid_20",
                      "covid_21",
                      "covid_22"]

        df = pd.read_csv("../data/LA_County_COVID_Cases.csv", chunksize=1000)
        # drop existing table to avoid redundancy
        db.covid_20.drop()
        db.covid_21.drop()
        db.covid_22.drop()

        for trunk in df:
            trunk['date'] = pd.to_datetime(trunk.date)
            df_20 = trunk[(trunk['date'] > "2020-1-1") & (trunk['date'] < "2021-1-1")]
            df_21 = trunk[(trunk['date'] > "2021-1-1") & (trunk['date'] < "2022-1-1")]
            df_22 = trunk[(trunk['date'] > "2022-1-1") & (trunk['date'] < "2023-1-1")]

            dict_20 = df_20.to_dict("records")
            dict_21 = df_21.to_dict("records")
            dict_22 = df_22.to_dict("records")

            # inserting

            if len(dict_20) > 0:
                conn = db.covid_20
                conn.insert_many(dict_20)

            if len(dict_21) > 0:
                conn = db.covid_21
                conn.insert_many(dict_21)

            if len(dict_22) > 0:
                conn = db.covid_22
                conn.insert_many(dict_22)

        print("done")
    elif filename == "LA_Weather.csv":
        dr = dr + ".LA_Weather"

        partitions = ["weather_19",
                      "weather_20",
                      "weather_21",
                      "weather_22"]

        df = pd.read_csv("../data/LA_Weather.csv", chunksize=1000)
        # drop existing table to avoid redundancy
        db.weather_19.drop()
        db.weather_20.drop()
        db.weather_21.drop()
        db.weather_22.drop()

        for trunk in df:
            trunk['datetime'] = pd.to_datetime(trunk.datetime)
            df_19 = trunk[(trunk['datetime'] > "2019-1-1") & (trunk['datetime'] < "2020-1-1")]
            df_20 = trunk[(trunk['datetime'] > "2020-1-1") & (trunk['datetime'] < "2021-1-1")]
            df_21 = trunk[(trunk['datetime'] > "2021-1-1") & (trunk['datetime'] < "2022-1-1")]
            df_22 = trunk[(trunk['datetime'] > "2022-1-1") & (trunk['datetime'] < "2023-1-1")]

            dict_19 = df_19.to_dict("records")
            dict_20 = df_20.to_dict("records")
            dict_21 = df_21.to_dict("records")
            dict_22 = df_22.to_dict("records")

            # inserting
            if len(dict_19) > 0:
                conn = db.weather_19
                conn.insert_many(dict_19)

            if len(dict_20) > 0:
                conn = db.weather_20
                conn.insert_many(dict_20)

            if len(dict_21) > 0:
                conn = db.weather_21
                conn.insert_many(dict_21)

            if len(dict_22) > 0:
                conn = db.covid_22
                conn.insert_many(dict_22)

        print("done")
    elif filename == "parties.csv":
        dr = dr + ".parties"
        partitions = ["parties"]

        df = pd.read_csv("../data/parties.csv", chunksize=1000)
        # drop existing table to avoid redundancy
        db.parties.drop()

        for trunk in df:

            dict_p = trunk.to_dict("records")

            # inserting
            if len(dict_p) > 0:
                conn = db.parties
                conn.insert_many(dict_p)

        print("done")
    elif filename == "victims.csv":
        dr = dr + ".victims"
        partitions = ["victims"]

        df = pd.read_csv("../data/victims.csv", chunksize=1000)

        # drop existing table to avoid redundancy
        db.victims.drop()

        for trunk in df:

            dict_v = trunk.to_dict("records")

            # inserting
            if len(dict_v) > 0:
                conn = db.victims
                conn.insert_many(dict_v)

        print("done")
    # insert into the path and indicate the partitions

    conn = db.file

    # get id of root and then update into the file
    root_id = conn.find_one()["_id"]
    conn.update_one({"_id": root_id}, {"$set": {dr: partitions}})

    print("done")


def getPartitionLocations(db, path):
    conn = db.file
    dr = path_transfer(path)
    root_id = conn.find_one()["_id"]
    all_path_list = conn.find({"_id": root_id}, {dr: 1, "_id": 0})

    dr_list = list(filter(None, dr.split('.')))
    all_path = list(all_path_list)[0]

    output_list = all_path
    for i in dr_list:
        output_list = output_list[i]

    print(output_list)


def readPartition(db, path, partition_number):
    conn = db.file
    dr = path_transfer(path)
    root_id = conn.find_one()["_id"]
    all_path_list = conn.find({"_id": root_id}, {dr: 1, "_id": 0})

    dr_list = list(filter(None, dr.split('.')))
    all_path = list(all_path_list)[0]

    output_list = all_path
    for i in dr_list:
        output_list = output_list[i]
    partition_number -= 1

    conn = db[output_list[partition_number]]
    output = conn.find()

    print(list(output))


def main():
    # connect with mongodb
    db = connect()
    command = ''
    conn = db.file
    # check if root directory has been created, if not, create one
    find = conn.find()
    if len(list(find)) == 0:
        conn.insert_one({"Root": {}})

    while command != 'exit':

        command = input('')

        if command.startswith('mkdir'):

            # validation part
            vali = command_vali_check(command)
            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[6:]

            # get rid of 'mkdir'
            dir_vali = dir_exist_check(db, cmd, False)

            if dir_vali == False:
                print("Directory error")
                continue

            mkdir(db, cmd)

        elif command.startswith('ls'):
            vali = command_vali_check(command)
            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[3:]
            # get rid of 'ls'
            dir_vali = dir_exist_check(db, cmd, True)

            if dir_vali == False:
                print("Directory error")
                continue

            ls(db, cmd)

        elif command.startswith('cat'):
            vali = command_vali_check(command)

            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[4:]
            dir_vali = dir_exist_check(db, cmd, True)

            if dir_vali == False:
                print("Directory error")
                continue

            cat(db, cmd)

        elif command.startswith('rm'):
            if command == "rm /":
                print("Can not delete root directory")
                continue
            vali = command_vali_check(command)
            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[3:]
            # get rid of 'rm'
            dir_vali = dir_exist_check(db, cmd, True)

            if dir_vali == False:
                print("Directory error")
                continue

            rm(db, cmd)

        elif command.startswith('put'):
            cmd = command.split()
            put(db, cmd[1], cmd[2])

        elif command.startswith('getPartitionLocations'):
            vali = command_vali_check(command)

            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[22:]
            dir_vali = dir_exist_check(db, cmd, True)

            if dir_vali == False:
                print("Directory error")
                continue

            getPartitionLocations(db, cmd)

        elif command.startswith('readPartition'):
            vali = command_vali_check(command)

            if vali == False:
                print('Command error')
                continue

            cmd = "/Root" + command[14:]
            dir_vali = dir_exist_check(db, cmd, True)

            if dir_vali == False:
                print("Directory error")
                continue

            readPartition(db, cmd, 1)

        elif command == 'exit':
            continue

        else:
            print('wrong command')
            continue


if __name__ == "__main__":
    main()
