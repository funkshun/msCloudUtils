import sqlite3
from datetime import datetime as dt
from collections import defaultdict
import requests

DATABASE_FILE = 'msbwtData.sqlite'

def getConnection():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        return conn
    except Exception as e:
        log(e)
        return None

def createDB():
    
    conn = getConnection()
    c = conn.cursor()

    upDatasets = """
    CREATE TABLE IF NOT EXISTS datasets (
    id integer PRIMARY KEY,
    grouping text NOT NULL,
    name text NOT NULL
    );"""

    upHosts = """
    CREATE TABLE IF NOT EXISTS hosts(
    id integer PRIMARY KEY,
    url text NOT NULL
    );""" 

    upLocal = """
    CREATE TABLE IF NOT EXISTS local(
    id integer PRIMARY KEY,
    direc text NOT NULL,
    grouping text NOT NULL
    );"""

    upHostsRel = """
    CREATE TABLE IF NOT EXISTS remote_source(
    id integer PRIMARY KEY,
    data_id integer,
    host_id integer,
    FOREIGN KEY (data_id) REFERENCES datasets (id),
    FOREIGN KEY (host_id) REFERENCES hosts (id)
    );"""

    upLocalRel = """
    CREATE TABLE IF NOT EXISTS local_source(
    id integer PRIMARY KEY,
    data_id integer,
    local_id integer,
    FOREIGN KEY (data_id) REFERENCES datasets (id),
    FOREIGN KEY (local_id) REFERENCES local (id)
    );"""

    try:
        c.execute(upHosts)
        c.execute(upLocal)
        c.execute(upDatasets)
        c.execute(upHostsRel)
        c.execute(upLocalRel)
        conn.commit()
        return True
    except Exception as e:
        log(e)
        return False

def dropDB():

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("DROP TABLE IF EXISTS remote_source")
        c.execute("DROP TABLE IF EXISTS local_source")
        c.execute("DROP TABLE IF EXISTS local")
        c.execute("DROP TABLE IF EXISTS hosts")
        c.execute("DROP TABLE IF EXISTS datasets")
        conn.commit()
        return True
    except Exception as e:
        log(e)
        return False

def insertDataset(name, group):
    
    if name == "" or group == "":
        raise ValueError("Name and grouping must both be non-empty")
    
    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("INSERT INTO datasets(name, grouping) VALUES(?,?)", (name, group))
        conn.commit()
        return selectDataset(name=name, group=group)[0][0]
    except Exception as e:
        log(e)
        return -1

def selectDataset(idn=None, name=None, group=None):
    
    para = {}
    if idn is not None:
        para["id"] = str(idn)
    else:
        if name is not None:
            para["name"] = name
        if group is not None:
            para["grouping"] = group
    
    conn = getConnection()
    c = conn.cursor()

    try:
        rows = c.execute(constructSelect("datasets", **para)).fetchall()
        return rows
    except Exception as e:
        log(e)
        return None

def deleteDataset(idn=None, name=None, group=None):
    
    para = {}
    if idn is not None:
        para["id"] = str(idn)
    else:
        if name is not None:
            para["name"] = name
        if group is not None:
            para["grouping"] = group
    
    conn = getConnection()
    c = conn.cursor()

    try:
        rows = c.execute(constructDelete("datasets", **para)).fetchall()
        return rows
    except Exception as e:
        log(e)
        return None

def insertHost(url):

    if url == "":
        raise ValueError("URL cannot be empty.")
    
    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("INSERT INTO hosts(url) VALUES(?)", (url,))
        conn.commit()
        return selectHost(url=url)[0][0]
    except Exception as e:
        log(e)
        return -1

def selectHost(idn=None, url=None):
    
    para = {}
    if idn is not None:
        para["id"] = str(idn)
    elif url is not None:
        para["url"] = url

    conn = getConnection()
    c = conn.cursor()

    try:
        rows = c.execute(constructSelect("hosts", **para)).fetchall()
        return rows
    except Exception as e:
        log(e)
        return None

def deleteHost(idn=None, url=None):
    
    para = {}
    if idn is not None:
        para["id"] = str(idn)
    elif url is not None:
        para["url"] = url

    conn = getConnection()
    c = conn.cursor()

    try:
        rows = c.execute(constructDelete("hosts", **para)).fetchall()
        return rows
    except Exception as e:
        log(e)
        return None

def insertDirec(direc, group):

    if direc == "" or group == "":
        raise ValueError("direc and grouping must be non-empty")
    
    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("INSERT INTO local(direc, grouping) VALUES(?,?)", (direc, group))
        conn.commit()
        return selectDirec(direc=direc, group=group)
    except Exception as e:
        log(e)
        return -1

def selectDirec(idn=None, direc=None, group=None):

    para = {}
    if idn is not None:
        para["id"] = idn
    else:
        if direc is not None:
            para["direc"] = direc
        if group is not None:
            para["grouping"] = group

    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructSelect("local", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def deleteDirec(idn=None, direc=None, group=None):
    para = {}
    if idn is not None:
        para["id"] = idn
    else:
        if direc is not None:
            para["direc"] = direc
        if group is not None:
            para["grouping"] = group

    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructDelete("local", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def insertRemote(data_id=None, name=None, host_id=None, url=None):

    if (data_id is None and name is None) or (host_id is None and url is None):
        raise ValueError("Must pass at least one identifier for host and one for dataset")

    if data_id is None:
        data_id = selectDataset(name=name)[0][0]
        if data_id is None:
            raise NameError("Specified dataset name was not found")

    if host_id is None:
        host_id = selectHost(url=url)
        if host_id is None:
            raise NameError("Specified host url was not found")

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("INSERT INTO remote_source(data_id, host_id) VALUES(?,?)", (data_id, host_id))
        conn.commit()
        return selectRemote(data_id=data_id, host_id=host_id)[-1][0]
    except Exception as e:
        log(e)
        return -1

def selectRemote(idn=None, data_id=None, host_id=None):
    
    para ={}
    if idn is not None:
        para["id"] = idn
    else:
        if data_id is not None:
            para["data_id"] = data_id
        if host_id is not None:
            para["host_id"] = host_id
    
    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructSelect("remote_source", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def deleteRemote(idn=None, data_id=None, host_id=None):
    
    para ={}
    if idn is not None:
        para["id"] = idn
    else:
        if data_id is not None:
            para["data_id"] = data_id
        if host_id is not None:
            para["host_id"] = host_id
    
    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructDelete("remote_source", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def insertLocal(data_id=None, name=None, local_id=None, direc=None):

    if (data_id is None and name is None) or (local_id is None and direc is None):
        raise ValueError("Must pass at least one identifier for host and one for dataset")

    if data_id is None:
        data_id = selectDataset(name=name)[0][0]
        if data_id is None:
            raise NameError("Specified dataset name was not found")

    if local_id is None:
        local_id = selectLocal(direc=direc)
        if local_id is None:
            raise NameError("Specified local directory was not found")

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("INSERT INTO local_source(data_id, local_id) VALUES(?,?)", (data_id, local_id))
        conn.commit()
        return selectLocal(data_id=data_id, local_id=local_id)[-1][0]
    except Exception as e:
        log(e)
        return -1

def selectLocal(idn=None, data_id=None, local_id=None):
    
    para ={}
    if idn is not None:
        para["id"] = idn
    else:
        if data_id is not None:
            para["data_id"] = data_id
        if local_id is not None:
            para["local_id"] = local_id
    
    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructSelect("local_source", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def deleteLocal(idn=None, data_id=None, local_id=None):
    
    para ={}
    if idn is not None:
        para["id"] = idn
    else:
        if data_id is not None:
            para["data_id"] = data_id
        if local_id is not None:
            para["local_id"] = local_id
    
    conn = getConnection()
    c = conn.cursor()

    try:
        return c.execute(constructDelete("local_source", **para)).fetchall()
    except Exception as e:
        log(e)
        return None

def getAvailable():
    available = defaultdict(list)
    datasets = selectDataset()
    for dataset in datasets:
        if len(selectRemote(data_id=dataset[0])) > 0:
            available[dataset[1]].append((dataset[2], True))
        else:
            available[dataset[1]].append((dataset[2], False))
    return available

def updateRemote():
    
    hosts = selectHost()
    for host in hosts:
        url = host[1]
        try:
            res = requests.get(url + '/checkAlive')
            datasets = res.json()
        except Exception as e:
            log(e)
            continue
        
        newRels = []
        for dataset in datasets:
            rows = selectDataset(name=dataset)
            if len(rows) != 1 or rows is None:
                continue
            newRels.append((rows[0][0], host[0]))
                    
def updateLocal():
    pass

def constructSelect(table, **kwargs):
    """ Generates SQL for a SELECT statement matching the kwargs passed. """
    sql = list()
    sql.append("SELECT * FROM %s " % table)
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
    sql.append(";")
    return "".join(sql)

def constructDelete(table, **kwargs):
    """ Generates SQL for a SELECT statement matching the kwargs passed. """
    sql = list()
    sql.append("DELETE FROM %s " % table)
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
    sql.append(";")
    return "".join(sql)

def log(message):
    print(timestamp() + " " + str(message))

def timestamp():
    tm = dt.strftime(dt.now(), "%Y/%m/%d %H:%M:%S")
    return "[" + tm + "]"
