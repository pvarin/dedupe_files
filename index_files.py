import os
import magic
import argparse
import subprocess
import re

from database import (
    connect_to_database,
    setup_tables,
    add_device,
    add_file
)
from hashing import md5, crc32


def main(rootdir, name, blacklist=None):
    try:
        conn = connect_to_database()
        setup_tables(conn)
        device_id = add_device(conn, (name, ))
        index_files(conn, rootdir, device_id, blacklist=blacklist)
        conn.commit()
    finally:
        conn.close()


def get_mime_type(filename):
    try:
        # This should work most of the time.
        filetype, filesubtype = magic.from_file(filename, mime=True).split('/')
    except ValueError:
        # I found a wierd bug that didn't seem to affect the commandline example.
        cmd = ['file', '--mime-type', filename]
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        errcode = process.returncode
        if errcode != 0:
            raise Exception(err)
        filetype, filesubtype = out.decode('utf-8').strip().split(
            ': ')[-1].split('/')

    return filetype, filesubtype


def index_files(conn, rootdir, device_id, blacklist=None, hash_file=False):
    if blacklist is None:
        blacklist = []

    i = 0
    for root, dirs, files, in os.walk(rootdir):
        dirs[:] = [d for d in dirs if blacklist.search(d) is None]
        for file in files:
            data = {}
            fullpath = os.path.join(root, file)
            filetype, filesubtype = get_mime_type(fullpath)
            filesize = os.path.getsize(fullpath)
            relpath = os.path.relpath(fullpath, rootdir)
            print('type: {} subtype: {} size: {} relpath: {} fullpath: {}'.
                  format(filetype, filesubtype, filesize, relpath, fullpath))
            data = {
                'type': filetype,
                'subtype': filesubtype,
                'size': filesize,
                'relpath': relpath,
                'device_id': device_id,
            }
            if hash_file:
                data['hash'] = md5(fullpath)
            add_file(conn, data)
            i += 1
            if (i % 1000 == 0):
                conn.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'device_name',
        type=str,
        help='the name of the device you want to scan')
    parser.add_argument(
        'rootdir',
        type=str,
        help='the location to start scanning')
    parser.add_argument(
        '--blacklist',
        type=str,
        default=None,
        help='a comma seperated list of directories to ignore')

    args = parser.parse_args()

    # Construct a regex for the blacklist
    blacklist_regexps = [r'^\.', r'^\$']
    if args.blacklist:
        blacklist_dirs = ['^{}$'.format(d) for d in args.blacklist.split(',')]
    else:
        blacklist_dirs = []
    blacklist_total = blacklist_regexps + blacklist_dirs

    regex = '|'.join(['({})'.format(d) for d in blacklist_total])
    blacklist = re.compile(regex)

    # Run the main function.
    try:
        conn = connect_to_database()
        setup_tables(conn)
        device_id = add_device(conn, (args.device_name, ))
        index_files(conn, args.rootdir, device_id, blacklist=blacklist)
        conn.commit()
    finally:
        conn.close()