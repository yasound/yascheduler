import time
from threading import Thread, Event
import settings
import requests
import json
from logger import Logger


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

        try:
            #  send request...
            url = settings.YASOUND_SERVER + '/api/v1/songs_started/'
            payload = {'key': settings.SCHEDULER_KEY, 'data': self.current_songs}
            response = requests.post(url, data=json.dumps(payload))
            result = response.json
            if response.status_code != 200:
                self.logger.info('current songs request failed: status code = %d' % response.status_code)
            elif result['success'] == False:
                self.logger.info('current songs request error: %s' % result['error'])

            # clear songs
            self.flush()

        except Exception, e:
            self.logger.info('CurrentSongManager report exception: %s' % str(e))

    def store(self, radio_uuid, song_id):
        self.current_songs.append([radio_uuid, song_id])
