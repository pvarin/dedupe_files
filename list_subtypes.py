import argparse

from database import connect_to_database

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('device_name', type=str, help='the name of the device you want to scan')
    parser.add_argument('--filetype', type=str, default='image', help='the MIME-type of the files to consider')
    parser.add_argument('--db_url', type=str, default=None, help='the location of the database file')
    

    args = parser.parse_args()
    print(args.filetype)

    sql_select_subtypes = """
        SELECT DISTINCT files.type, files.subtype, count(*)
        FROM files
        INNER JOIN devices
        ON files.device_id=devices.id
        WHERE files.type='{type}'
        AND devices.name='{device_name}'
        GROUP BY files.type, files.subtype;
        """.format(type=args.filetype, device_name=args.device_name)

    sql_select_examples = """
        SELECT relpath
        FROM files
        INNER JOIN devices
        ON files.device_id=devices.id
        WHERE files.type=?
        AND files.subtype=?
        AND devices.name='{device_name}'
        LIMIT 3;
        """.format(device_name=args.device_name)

    try:
        conn = connect_to_database(db_url=args.db_url)
        cur = conn.execute(sql_select_subtypes)
        for c in cur:
            print("type: '{}' subtype: '{}' count: {}".format(*c))
            cursor = conn.execute(sql_select_examples,c[:2])
            for c1 in cursor:
                print('\t{}'.format(c1[0]))


    finally:
        conn.close()
