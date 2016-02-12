from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
import datetime, yaml

# Dates are inclusive
# https://blog.soundcloud.com/2008/10/17/cc/
DATE_START = datetime.datetime(2008, 10, 17)
DATE_END = datetime.datetime.today()
DATE_STRIDE = datetime.timedelta(days=1)
DATE_TO_OFFSET = datetime.timedelta(hours=23, minutes=59, seconds=59)

LICENSES = [
    # Currently, all no-rights-reserved calls give a 400 response
    #"no-rights-reserved",
    #####"all-rights-reserved",
    "cc-by",
    # Currently, all cc-by-nc calls give a 400 response
    #"cc-by-nc",
    "cc-by-nd",
    "cc-by-sa",
    "cc-by-nc-nd",
    "cc-by-nc-sa"
]

CREATES = [
"""
CREATE TABLE time_slices (
       time_slice_id INT           UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
       date_from     DATETIME      NOT NULL,
       date_to       DATETIME      NOT NULL,
       license       CHAR(20)      NOT NULL,
       worker        VARCHAR(20),
       next_href     VARCHAR(255)
);
""",
"""
-- We're not worried about url uniqueness as the results should be unique and
-- ordered by date.


CREATE TABLE soundcloud_tracks_by_date (
       id           INT               UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       permalink_url VARCHAR(255)     UNIQUE NOT NULL,
       download_url VARCHAR(255)      NOT NULL,
       license      CHAR(20)          NOT NULL,
       -- 100 character limit is from the web UI
       title        VARCHAR(100)      NOT NULL,
       description  TEXT              NOT NULL,
       created_at   DATETIME          NOT NULL,
       genre        VARCHAR(255)      NOT NULL,
       -- tag_list     VARCHAR(255)      NOT NULL,
       track_type   CHAR(14)          NOT NULL,
       -- 25 character limit is from the web UI
       username     VARCHAR(25)       NOT NULL,
       -- No limit from web UI, truncate if longer
       label_name   VARCHAR(255)      NOT NULL
);
""",
"""
CREATE INDEX permalink_url_index on soundcloud_tracks_by_date (permalink_url);
"""
]

INSERT_SHARD = \
"""INSERT INTO time_slices (date_from, date_to, license)
    VALUES (%(date_from)s, %(date_to)s, %(license)s)"""

def create_tables (cursor):
    print("Creating:")
    for create in CREATES:
        try:
            cursor.execute(create)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def datetime_py2sql (dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def create_shards (cursor):
    print("Initializing time slices.")
    start_date = DATE_START
    while start_date < DATE_END:
        end_date = start_date + DATE_TO_OFFSET
        for lic in LICENSES:
            cursor.execute(INSERT_SHARD,
                           {'date_from': start_date,
                            'date_to': end_date,
                            'license': lic})
        start_date = start_date + DATE_STRIDE

if __name__ == '__main__':
    config = yaml.load(open('config.yaml'))
    dbconfig = config['database']
    connection = mysql.connector.connect(user=dbconfig['user'],
                                         password=dbconfig['password'],
                                         host=dbconfig['host'],
                                         database=dbconfig['database'])
    cursor = connection.cursor()
    create_tables(cursor)
    create_shards(cursor)
    connection.commit()
    cursor.close()
    connection.close()
