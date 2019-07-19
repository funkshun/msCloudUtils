import sqlite3

DATABASE_FILE = 'msbwtData.sqlite'

def getConnection():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        return conn
    except Exception as e:
        print(e)
        return None

def getAvailable():
    conn = sqlite3.connect(DATABASE_FILE)

# Creates database file and structure if it doesn't exist
def createDB():
    conn = getConnection()

    c = conn.cursor()

    upDatasets = """
    CREATE TABLE IF NOT EXISTS datasets (
    id integer PRIMARY KEY,
    group text NOT NULL,
    name text NOT NULL,
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
    group text
    );"""

    upHostsRel = """
    CREATE TABLE IF NOT EXISTS dataset_hosts(
    FOREIGN KEY (data_id) REFERENCES datasets (id),
    FOREIGN KEY (host_id) REFERENCES hosts (id)
    );"""

    upLocalRel = """
    CREATE TABLE IF NOT EXISTS dataset_local(
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
        print(e)
        return False

def dropDB():

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute("DROP TABLE IF EXISTS dataset_local")
        c.execute("DROP TABLE IF EXISTS dataset_hosts")
        c.execute("DROP TABLE IF EXISTS local")
        c.execute("DROP TABLE IF EXISTS hosts")
        c.execute("DROP TABLE IF EXISTS datasets")
        conn.commit()
        return True
    except Exception as e:
        print(e)
        return False

def selectHost(idn=None, url=None):

    baseQuery = """
    SELECT id, url FROM hosts WHERE
    """
    args = {
        'idn': idn,
        'url': url
    }

    first = True
    for key, value in args.items():
        if value is not None:
            if not first:
                baseQuery += " AND"
            first = False
            baseQuery += " {} = {}".format(key, value)

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute(baseQuery)
        return c.fetchall()
    except Exception as e:
        print(e)
        return None

def selectLocal(idn=None, direc=None):

    baseQuery = """
    SELECT id, direc FROM local WHERE
    """
    args = {
        'idn': idn,
        'direc':direc
    }

    first = True
    for key, value in args.items():
        if value is not None:
            if not first:
                baseQuery += " AND"
            first = False
            baseQuery += " {} = {}".format(key, value)

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute(baseQuery)
        return c.fetchall()
    except Exception as e:
        print(e)
        return None

def selectDataset(idn=None, group=None, name=None, host_id=None, local_id=None):
    baseQuery = """
    SELECT id, group, name, host_id, local_id FROM datasets WHERE
    """
    args = {
        'idn': idn,
        'group': group,
        'name': name,
        'host_id': host_id,
        'local_id': local_id
    }

    first = True
    for key, value in args.items():
        if value is not None:
            if not first:
                baseQuery += " AND"
            first = False
            baseQuery += " {} = {}".format(key, value)

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute(baseQuery)
        rows = c.fetchall()
        return rows
    except Exception as e:
        print(e)
        return None

def insertDataset(name, group, url=None, local=None):

    baseQuery = """
    INSERT INTO datasets(name, group) VALUES(?,?)
    """
    if name == "" or group == "":
        raise ValueError("Name and Group must both be non-empty")

    host, localDir = []
    if url is not None:
        try:
            host = selectHost(url=url)[0]
        except Exception as e:
            print("Failed to retrieve host. Ensure it exists: " + e)
            return -1

    if local is not None:
        try:
            localDir = selectLocal(direc=local)[0]
        except Exception as e:
            print("Failed to retrieve local directory: " + e)
            return -1

    conn = getConnection()
    c = conn.cursor()

    try:
        c.execute(baseQuery, (name, group))
        new_data = selectDataset(name=name, group=group)[0]
        if host != []:
            c.execute("INSERT INTO dataset_hosts(data_id, host_id) VALUES(?,?)", (new_data[0], host[0]))
        if local != []:
            c.execute("INSERT INTO dataset_local(data_id, local_id) VALUES(?,?)", (new_data[0], local[0]))
        conn.commit()
        return new_data[0]
    except Exception as e:
        print(e)
        return -1

def insertHost(url):

    conn = getConnection():
    c = conn.cursor()

    if url == "":
        raise ValueError("Invalid URL")

    try:
        c.execute("INSERT INTO hosts(url) VALUES(?,)", (?,))
        hst = selectHost(url=url)[0]
        conn.commit()
        return hst[0]
    except Exception as e:
        print(e)
        return -1
