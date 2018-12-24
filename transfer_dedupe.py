import os
import argparse
import shutil

from database import connect_to_database, get_unique_hashes, add_master_file, get_device_id, add_device

def transfer_unique(conn, src_dev, src_root, dst_dev, dst_root):
    dst_device_id = get_device_id(conn, dst_dev)
    cur = get_unique_hashes(conn, src_dev, dst_dev)
    for i,c in enumerate(cur):
        file_hash, file_id, relpath = c
        src_fullpath = os.path.join(src_root,relpath)
        dst_fullpath = os.path.join(dst_root,relpath)
        print(('Transferring file: {src_path}\n' + 
               '               to: {dst_path}'
               ).format(src_path=src_fullpath,
                        dst_path=dst_fullpath))
        data = {
            'relpath': relpath,
            'hash': file_hash,
            'device_id': dst_device_id,
            'file_id': file_id
        }
        dst_dir = os.path.dirname(dst_fullpath)
        os.makedirs(dst_dir, exist_ok=True)
        try:
            shutil.copy2(src_fullpath, dst_fullpath)
            add_master_file(conn, data)
        except FileNotFoundError:
            continue
        # Save every 100 writes
        if i%100 == 0:
            conn.commit()
    conn.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source_device', type=str, help='the source device name')
    parser.add_argument(
        'source_root', type=str, help='the source root location')
    parser.add_argument(
        'dest_device', type=str, help='the dest device name')
    parser.add_argument(
        'dest_root', type=str, help='the dest root location')
    args = parser.parse_args()


    print(
        ('Transferring data from device: {src_dev} at {src_root}\n' + 
         '                    to device: {dst_dev} at {dst_root}')
        .format(
            src_dev=args.source_device,
            src_root=args.source_root,
            dst_dev=args.dest_device,
            dst_root=args.dest_root))

    try:
        conn = connect_to_database()
        print(add_device(conn, (args.dest_device,)))
        transfer_unique(conn, args.source_device, args.source_root, args.dest_device, args.dest_root)
    finally:
        conn.close()

"""
select hash from (
    select count(*) as num_collisions, files.hash
    from files
    join devices
    on files.device_id = devices.id
    where files.hash is not null
    and devices.name = '4000B'
    group by files.hash
)
where num_collisions > 1;
"""
