import soundcloud
import datetime, logging, requests, time, yaml
import mysql.connector
from mysql.connector import errorcode

# This is the program that fetches data from soundcloud and saves it to the db.
# It finds shards that no other instances of this program are downloading
# then gets the matching results from the flickr api and inserts them.
# It handles errors a little but do watch the logs.

LOGLEVEL = 25

MAX_TRIES = 1

# Time (seconds) to sleep while waiting to retry. Make it relatively large.
SLEEP_TIME = 60

#NOTE: methods beginning "fetch" access the Internet
#      methods beginning "insert" and "update" access the database

RESULTS_PER_PAGE = 100

# Don't hit the API more often than once per second
API_HIT_TIME = 1 * 1001

# Ignore so if we're restarting halfway through a page we just skip already
# inserted images from the previous run, or for tracks returned by multiple
# searches.
INSERT_TRACK = \
"""INSERT IGNORE INTO soundcloud_tracks_by_date (permalink_url, download_url,
                                                 license, title, description,
                                                 created_at, genre, track_type,
                                                 username, label_name)
    VALUES (%(permalink_url)s, %(download_url)s, %(license)s, %(title)s,
            %(description)s, %(created_at)s, %(genre)s, %(track_type)s,
            %(username)s, %(label_name)s);"""

UPDATE_NEXT_HREF = \
"""UPDATE time_slices SET next_href=%(next_href)s
    WHERE time_slice_id=%(time_slice_id)s;"""

SELECT_UNFINISHED_TASK = \
"""SELECT * FROM time_slices
    WHERE worker = %(my_identifier)s AND next_href IS NOT NULL
    LIMIT 1;"""

# This is actually a canonical use of LAST_INSERT_ID()
UPDATE_FRESH_TASK = \
"""UPDATE time_slices SET worker = %(my_identifier)s,
                     time_slice_id = LAST_INSERT_ID(time_slice_id)
    WHERE worker IS NULL
    LIMIT 1;"""

SELECT_FRESH_TASK = \
"""SELECT * FROM time_slices WHERE time_slice_id = LAST_INSERT_ID();"""

class Worker (object):

    def __init__(self, config):
        self.identifier = config['worker']['id']
        self.reset()
        soundcloud_config = config['soundcloud']
        self.soundcloud = soundcloud.Client(client_id=soundcloud_config['client_id'])
        dbconfig = config['database']
        self.connection = mysql.connector.connect(user=dbconfig['user'],
                                                  password=dbconfig['password'],
                                                  host=dbconfig['host'],
                                                  database=dbconfig['database'],
                                                  autocommit=True)
        self.cursor = self.connection.cursor()

    def reset(self):
        self.licence = ''
        self.date_range = None
        self.next_href = None
        self.time_slice_id = -1

    def shutdown(self):
        self.cursor.close()
        self.connection.close()

    def escape (self, field):
        if field:
            field = field.replace("\n", "\\n")
            field = field.replace("\r", "\\r")
            field = field.replace("\t", "\\t")
        return field

    def insertTrack(self, track):
        self.cursor.execute(INSERT_TRACK,
                            {'download_url': track.download_url,
                             'license': track.license,
                             'permalink_url': track.permalink_url,
                             'title': self.escape(track.title),
                             'description': self.escape(track.description),
                             'created_at': track.created_at,
                             'genre': self.escape(track.genre),
                             'track_type': self.escape(track.track_type),
                             'username': self.escape(track.user['username']),
                             'label_name': self.escape(track.label_name)})

    def insertTracks(self, tracks):
        for track in tracks.collection:
            self.insertTrack(track)
        self.set_next_href(tracks)
        self.updateState()

    def updateState(self):
        self.cursor.execute(UPDATE_NEXT_HREF,
                            {'time_slice_id': self.time_slice_id,
                             'next_href': self.next_href})

    def set_next_href (self, tracks):
        try:
            self.next_href = tracks.next_href
        except Exception as e:
            self.next_href = None

    def initialFetch (self):
        try:
            tracks = self.soundcloud.get('/tracks',
                                         license=self.license,
                                         created_at=self.date_range,
                                         filter='public',
                                         order='created_at',
                                         limit=RESULTS_PER_PAGE,
                                         linked_partitioning=1)
            self.insertTracks(tracks)
        # We get an error when there are no more results. Meh.
        except requests.HTTPError as e:
            logging.log(LOGLEVEL, e)
            self.markTaskFinished()
            return
        except Exception as e:
            logging.error(e)
            exit(1)

    def subsequentFetch (self):
        try:
            tracks = self.soundcloud.get(self.next_href)
            self.insertTracks(tracks)
        # We get an error when there are no more results. Meh.
        except requests.HTTPError as e:
            print(e)
            self.markTaskFinished()
            return
        except Exception as e:
            print(e)

    def markTaskFinished(self):
        self.next_href = None
        self.updateState()

    def taskInProgress(self):
        return self.next_href != None

    def runTask(self, tries=0):
        if not self.next_href:
            self.initialFetch()
        while self.taskInProgress():
            try:
                started_at = int(round(time.time() * 1000))
                self.subsequentFetch()
                ended_at = int(round(time.time() * 1000))
                # Make sure we don't call the API too often
                execution_time = ended_at - started_at
                if execution_time < API_HIT_TIME:
                    time.sleep((API_HIT_TIME - execution_time) / 1000.0)
            except Exception as e:
                logging.error(e)
                time.sleep(SLEEP_TIME)
                if tries < MAX_TRIES:
                    logging.info("Retrying %s - %s", self.time_slice_id, e)
                    self.runTask(tries + 1)
                else:
                    self.markTaskFinished()
                    logging.error("Giving up on %s - %s", self.time_slice_id, e)
        self.markTaskFinished()

    def configureFromTask(self, task):
        (time_slice_id, date_from, date_to, lic, worker, next_href) = task
        self.time_slice_id = time_slice_id
        # The SQL datetime format is the same as used by Soundcloud
        self.date_range = {"from": date_from,
                           "to": date_to}
        self.license = lic
        self.next_href = next_href

    def selectUnfinishedTask(self):
        got_task = False
        self.cursor.execute(SELECT_UNFINISHED_TASK,
                            {'my_identifier': self.identifier})
        result = self.cursor.fetchone()
        if result is not None:
            got_task = True
            self.configureFromTask(result)
        return got_task

    def updateFreshTask(self):
        got_task = False
        self.cursor.execute(UPDATE_FRESH_TASK,
                            {'my_identifier':self.identifier})
        self.cursor.execute(SELECT_FRESH_TASK)
        result = self.cursor.fetchone()
        if result is not None:
            got_task = True
            self.configureFromTask(result)
        return got_task

    def logTask(self):
        logging.log(LOGLEVEL, "TIME_SLICE %s",
                    self.time_slice_id)

    def go(self):
        logging.log(LOGLEVEL, 'Starting unfinished tasks.')
        while self.selectUnfinishedTask():
            self.logTask()
            self.runTask()
        logging.log(LOGLEVEL, 'Finished unfinished tasks.')
        logging.log(LOGLEVEL, 'Starting fresh tasks.')
        while self.updateFreshTask():
            self.logTask()
            self.runTask()
        logging.log(LOGLEVEL, 'Finished fresh tasks.')
        logging.log(LOGLEVEL, 'Shutting down.')
        self.shutdown()
        logging.log(LOGLEVEL, 'Shut down.')

if __name__ == '__main__':
    logging.basicConfig(level=LOGLEVEL)
    config = yaml.load(open('config.yaml'))
    worker = Worker(config)
    worker.go()
