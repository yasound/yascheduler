import time
from threading import Thread, Event
import settings
import requests
import json
from logger import Logger
from datetime import datetime


class CurrentSongManager(Thread):
    SONG_COUNT_MAX_PER_REQUEST = 100

    def __init__(self):
        Thread.__init__(self)

        self.quit = Event()
        self.WAIT_TIME = 0.2
        self.logger = Logger().log

        self.current_songs = []
        self.running = False

    def flush(self):
        self.current_songs = []

    def join(self, timeout=None):
        self.quit.set()
        super(CurrentSongManager, self).join(timeout)

    def run(self):
        self.running = True
        while not self.quit.is_set():
            self.report()

            # sleep
            time.sleep(self.WAIT_TIME)
        self.running = False

    def report(self):
        if len(self.current_songs) == 0:
            return

        failed = False
        songs = self.current_songs[:self.SONG_COUNT_MAX_PER_REQUEST]
        song_count = len(songs)
        self.logger.info('current songs: report %d songs (%d still in queue) ' % (song_count, len(self.current_songs) - song_count))
        try:
            #  send request...
            url = settings.YASOUND_SERVER + '/api/v1/songs_started/'
            payload = {'key': settings.SCHEDULER_KEY, 'data': songs}
            response = requests.post(url, data=json.dumps(payload))
            result = response.json
            if response.status_code != 200:
                failed = True
                self.logger.info('current songs request failed: status code = %d' % response.status_code)
            elif result['success'] == False:
                failed = True
                self.logger.info('current songs request error: %s' % result['error'])
        except Exception, e:
            failed = True
            self.logger.info('CurrentSongManager report exception: %s' % str(e))
        if failed == False:
            # songs have been correctly sent
            # remove from self.current_songs
            self.current_songs = self.current_songs[song_count:]

    def store(self, radio_uuid, song_id, play_date=None):
        if play_date == None:
            play_date = datetime.now()
        self.current_songs.append([radio_uuid, song_id, play_date.isoformat()])
