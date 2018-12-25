import os
import pathlib
import sqlite3

def get_default_db_url():
    return os.path.join(str(pathlib.Path.home()),'dedupe_index.db')

def connect_to_database(db_url=None):
    if db_url is None:
        db_url = get_default_db_url()
    conn = sqlite3.connect(db_url)
    return conn

def setup_tables(conn):
    c = conn.cursor()
    
    sql_create_devices_table = """
        CREATE TABLE IF NOT EXISTS devices (
         id integer PRIMARY KEY,
         name text NOT NULL UNIQUE
        );"""
    c.execute(sql_create_devices_table)
    sql_create_files_table = """
        CREATE TABLE IF NOT EXISTS files (
         id integer PRIMARY KEY,
         type text NOT NULL,
         subtype text NOT NULL,
         relpath text NOT NULL,
         hash text,
         size integer,
         device_id integer,
         FOREIGN KEY (device_id) REFERENCES devices (id)
        );
        """
    c.execute(sql_create_files_table)
    sql_create_master_files_table = """
        CREATE TABLE IF NOT EXISTS master_files (
         id integer PRIMARY KEY,
         relpath text NOT NULL,
         hash text NOT NULL,
         device_id integer,
         file_id integer,
         FOREIGN KEY (device_id) REFERENCES devices (id)
         FOREIGN KEY (file_id) REFERENCES files (id)
        );
        """
    c.execute(sql_create_master_files_table)

def add_device(conn, data):
    sql_add_device = """
        INSERT INTO devices(name)
        VALUES(?)"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql_add_device, data)
        return cursor.lastrowid
    except:
        return get_device_id(conn, data[0])

def add_file(conn, data):
    keys = data.keys()
    values = [data[k] for k in keys]
    sql_add_file = """
        INSERT INTO files({})
        VALUES({})""".format(','.join(keys),','.join(['?']*len(values)))
    cursor = conn.cursor()
    cursor.execute(sql_add_file, values)
    return cursor.lastrowid

def add_master_file(conn, data):
    keys = data.keys()
    values = [data[k] for k in keys]
    sql_add_file = """
        INSERT INTO master_files({})
        VALUES({})""".format(','.join(keys),','.join(['?']*len(values)))
    cursor = conn.cursor()
    cursor.execute(sql_add_file, values)
    return cursor.lastrowid

def get_device_id(conn, device_name):
    sql_get_unique_hashes = """
        SELECT
            id
        FROM devices
        WHERE name=?"""
    cur = conn.execute(sql_get_unique_hashes,(device_name,))
    return cur.fetchone()[0]

def get_unique_hashes(conn, source_device, dest_device):
    dest_device_id = get_device_id(conn, dest_device)
    source_device_id = get_device_id(conn, source_device)
##    sql_get_unique_hashes = """
##        SELECT
##            files.hash as hash,
##            MIN(files.id) as file_id,
##            files.relpath
##        FROM files
##        JOIN devices
##            ON files.device_id=devices.id
##        WHERE devices.name=?
##        AND files.hash IS NOT NULL
##        AND NOT EXISTS (
##            SELECT * FROM master_files
##            WHERE master_files.hash=files.hash
##            AND master_files.device_id=?
##        )
##        GROUP BY hash
##    """
##    return conn.execute(sql_get_unique_hashes, (source_device, dest_device_id))
    sql_get_unique_hashes = """
        select files.hash, files.id, files.relpath
        from files
        join(
                select unique_files.id
                from (
                        select hash, min(id) as id
                        from files
                        where hash is not null
                        and device_id = ?
                        group by hash) unique_files
                left outer join (
                    select * from master_files
                    where master_files.device_id = ?
                ) master_files_filtered
                on unique_files.hash = master_files_filtered.hash
                where master_files_filtered.file_id is null
        ) unique_file_ids
        on files.id = unique_file_ids.id
        order by files.size asc
        """
    return conn.execute(sql_get_unique_hashes, (source_device_id, dest_device_id))
