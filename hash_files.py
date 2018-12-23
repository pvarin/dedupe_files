import os
import pathlib
import sqlite3
import magic
import hashlib
import argparse
import subprocess
import re

from database import connect_to_database
from hashing import md5


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('device_name', type=str, help='the name of the device you want to scan')
    parser.add_argument('rootdir', type=str, help='the location to start scanning')
    parser.add_argument('--filetype', type=str, default='image', help='the MIME-type of the files to consider')
    parser.add_argument('--subtypes', type=str, default='jpeg,tiff,x-canon-cr2,x-tga,vnd.adobe.photoshop', help='a comma separated list of MIME-subtypes')
    parser.add_argument('--db_url', type=str, default=None, help='the location of the database file')
    
    args = parser.parse_args()
    subtypes = args.subtypes.split(',')
    subtypes = ','.join(["'{}'".format(s) for s in subtypes])

    try:
        conn = connect_to_database(db_url=args.db_url)

        if 'all' in subtypes:
            sql_count_type = """
                SELECT COUNT(*)
                FROM files
                INNER JOIN devices
                ON files.device_id=devices.id
                WHERE files.type='{type}'
                AND files.size >= 200000
                AND files.hash is null
                AND devices.name='{device_name}';
                """.format(type=args.filetype, device_name=args.device_name, subtypes=subtypes)
        else:
            sql_count_type = """
                SELECT COUNT(*)
                FROM files
                INNER JOIN devices
                ON files.device_id=devices.id
                WHERE files.type='{type}'
                AND files.subtype in ({subtypes})
                AND files.size >= 200000
                AND files.hash is null
                AND devices.name='{device_name}';
                """.format(type=args.filetype, device_name=args.device_name, subtypes=subtypes)

        cursor = conn.execute(sql_count_type)
        count, = cursor.fetchone()
        print("Fetching {} {} files from database".format(count, args.filetype))
        
        if 'all' in subtypes:
            sql_select_type = """
                SELECT files.id, files.relpath
                FROM files
                INNER JOIN devices
                ON files.device_id=devices.id
                WHERE files.type='{type}'
                AND files.size >= 200000
                AND files.hash is null
                AND devices.name='{device_name}';
                """.format(type=args.filetype, device_name=args.device_name, subtypes=subtypes)
        else:
            sql_select_type = """
                SELECT files.id, files.relpath
                FROM files
                INNER JOIN devices
                ON files.device_id=devices.id
                WHERE files.type='{type}'
                AND files.subtype in ({subtypes})
                AND files.size >= 200000
                AND files.hash is null
                AND devices.name='{device_name}';
                """.format(type=args.filetype, device_name=args.device_name, subtypes=subtypes)

        cursor = conn.execute(sql_select_type)
        for i,data in enumerate(cursor):
            file_id, relpath = data
            fullpath = os.path.join(args.rootdir,relpath)
            md5_hash = md5(fullpath)
            sql_update_hash = """
                UPDATE files
                SET hash=?
                WHERE id=?
                """
            cur = conn.cursor()
            cur.execute(sql_update_hash, (md5_hash, file_id))
            
            if (i+1)%100 == 0:
                conn.commit()
                print("finished {} of {}".format(i+1, count))
        conn.commit()
    finally:
        conn.close()
