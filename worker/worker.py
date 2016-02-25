import errno, json, os.path, re, requests, shutil, signal, subprocess, yaml

################################################################################
# Config
################################################################################

TASKS_REQUEST_URL_PATH = 'fetch-tasks.php'
INSERT_FILENAME_URL_PATH = 'insert-filename.php'

SSH_ARGS = "ssh -i {0} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

################################################################################
# Downloading and rsyncing tracks
################################################################################

class TasksWorker (object):

    """A class that loops through the urls in a task batch, fetches the files,
       saves their filenames back to the database server, then rsyncs them
       to the file server."""

    def __init__ (self, config, tasks_desc):
        self.download_path = config['download_path']
        self.remote_path = config['rsync_remote_path']
        self.passkey = config['filename_insert_passkey']
        self.client_id = tasks_desc['config']['client_id']
        self.insert_filename_url = "{0}/{1}".format(config['control_server'],
                                                    INSERT_FILENAME_URL_PATH)
        self.urls = tasks_desc['urls']
        self.ssh_args = SSH_ARGS.format(config['ssh_key_path'])
        
    def idToFilename (self, track_id):
        return track_id + '.mp3'

    def idToPath (self, track_id):
        # Current max ids for SC are low 9 digits
        return os.path.join(*list(track_id.zfill(10)))

    def ensureDownloadDirs (self, path):
        try:
            os.makedirs(path)
        except OSError, exception:
            if exception.errno != errno.EEXIST:
                raise

    def saveTrack (self, filepath, response):
        with open(filepath, 'wb') as download_file:
            download_file.write(response.content)

    def fetchTrack (self, url):
        url_with_client_id = "{0}?client_id={1}".format(url, self.client_id)
        return requests.get(url_with_client_id)

    def process (self, url_id, url):
        track_id = [item for item in url.split('/') if item != "" ][-2]
        response = self.fetchTrack(url)
        # Would be nice to differentiate between bad credentials and newly locked file.
        # This is for the latter case.
        if response.status_code == 401:
            print "Couldn't access {0} (404), skipping.".format(url)
            return
        self.insertTrackFilename(track_id, response)
        file_directory = os.path.join(self.download_path,
                                      self.idToPath(track_id))
        filepath = os.path.join(file_directory, self.idToFilename(track_id))
        self.ensureDownloadDirs(file_directory)
        self.saveTrack (filepath, response)

    def insertTrackFilename(self, url_id, response):
        disposition = response.headers['content-disposition']
        filename = re.findall("filename=(.+)", disposition)[0]
        requests.post(self.insert_filename_url, {'pass':self.passkey,
                                                 'track_id': url_id,
                                                 'filename': filename})

    def rsync (self):
        rsync_args = ['/usr/bin/rsync',
                      '-e',
                      self.ssh_args,
                      '-r',
                      self.download_path,
                      self.remote_path]
        return_code = subprocess.call(rsync_args)
        if return_code == 0:
            print "rsync OK"
        else:
            print "rsync error {0}".format(return_code)

    def cleanup (self):
        shutil.rmtree(self.download_path)

    def serviceTasks(self):
        for task in self.urls:
            task_id = task['id']
            download_url = task['download_url']
            # Ignore empty download urls (download not enabled
            if download_url:
                print "{0} {1}".format(task_id, download_url)
                self.process(task_id, download_url)
            else:
                print "{0} NO DOWNLOAD.".format(task_id)
        # This will be false if no tracks in the batch were downloadable
        if os.path.exists(self.download_path):
            self.rsync()
            self.cleanup()

################################################################################
# Fetching tasks from the data server and servicing them
################################################################################

class Worker (object):

    """A class that loops, fetching batches of urls from the data server,
       and working on them."""

    def __init__ (self, config):
        self.config = config
        self.control_server = config['control_server']

    def jsonConfigCacheFilepath (self):
        return './task.json'
    
    def restorePreviousTaskConfig (self):
        try:
            with open(self.jsonConfigCacheFilepath(), 'r') as json_cachefile:
                config = json.loads(json_cachefile.read())
        except:
            # Absent or corrupt file
            config = False
        return config

    def fetchNewTaskConfig (self):
        response = requests.get("%s/%s" % (self.control_server,
                                           TASKS_REQUEST_URL_PATH))
        print "GET tasks. HTTP response code: {0}".format(response.status_code)
        with open(self.jsonConfigCacheFilepath(), 'w') as json_cachefile:
            print >>json_cachefile, response.text
        return json.loads(response.text)

    def workOnTasks (self, config, tasks_desc):
        tasks_worker = TasksWorker(config, tasks_desc)
        tasks_worker.serviceTasks()

    def work (self):
        while True:
            tasks_desc = self.restorePreviousTaskConfig()
            if not tasks_desc:
                tasks_desc = self.fetchNewTaskConfig()
            if (not tasks_desc) or len(tasks_desc) < 0:
                break
            print "First id: {0}".format(tasks_desc['urls'][0]['id'])
            self.workOnTasks(self.config, tasks_desc)
            os.remove(self.jsonConfigCacheFilepath())

################################################################################
# Main flow of control
################################################################################

if __name__ == "__main__":
    config = yaml.load(open('config.yaml'))
    worker = Worker(config)
    worker.work()
