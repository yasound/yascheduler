import time
from threading import Thread, Event
import settings
import requests
import json
from logger import Logger
from datetime import datetime


class CurrentSongManager(Thread):

    def __init__(self):
        Thread.__init__(self)

        self.quit = Event()
        self.WAIT_TIME = 2
        self.logger = Logger().log

        self.current_songs = []

    def flush(self):
        self.current_songs = []

    def join(self, timeout=None):
        self.quit.set()
        super(CurrentSongManager, self).join(timeout)

    def run(self):
        while not self.quit.is_set():
            self.report()

            # sleep
            time.sleep(self.WAIT_TIME)

    def report(self):
        if len(self.current_songs) == 0:
            return

        self.logger.info('CurrentSongManager report %d songs' % len(self.current_songs))

        ok = True
        songs = self.current_songs
        # clear songs
        self.flush()

        try:
            #  send request...
            url = settings.YASOUND_SERVER + '/api/v1/songs_started/'
            payload = {'key': settings.SCHEDULER_KEY, 'data': songs}

            response = requests.post(url, data=json.dumps(payload))
            result = response.json
            if response.status_code != 200:
                ok = False
                self.logger.info('current songs request failed: status code = %d' % response.status_code)
            elif result['success'] == False:
                ok = False
                self.logger.info('current songs request error: %s' % result['error'])

        except Exception, e:
            ok = False
            self.logger.info('CurrentSongManager report exception: %s' % str(e))

        if ok == False:
            # songs have not been correctly sent
            # reinsert in self.current_songs at index 0
            self.current_songs[:0] = songs

    def store(self, radio_uuid, song_id, play_date=None):
        if play_date == None:
            play_date = datetime.now()
        self.current_songs.append([radio_uuid, song_id, play_date.isoformat()])
