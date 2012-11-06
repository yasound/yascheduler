import time
from threading import Thread
import settings
import requests
import json
from logger import Logger


class CurrentSongManager(Thread):
    WAIT_TIME = 15  # seconds

    def __init__(self):
        Thread.__init__(self)

        self.quit = False
        self.WAIT_TIME = 2
        self.logger = Logger().log

        self.current_songs = settings.MONGO_DB.scheduler.current_songs

    def flush(self):
        self.current_songs.remove()

    def run(self):
        while self.quit == False:
            self.report()

            # sleep
            time.sleep(self.WAIT_TIME)

    def report(self):
        docs = self.current_songs.find()
        reports = []
        for d in docs:
            data = [d['radio_uuid'], d['song_id']]
            reports.append(data)
        if len(reports) == 0:
            return

        self.logger.info('CurrentSongManager report %d songs' % len(reports))

        try:
            #  send request...
            url = settings.YASOUND_SERVER + '/api/v1/songs_started/'
            payload = {'key': settings.SCHEDULER_KEY, 'data': reports}
            response = requests.post(url, data=json.dumps(payload))
            result = response.json
            if response.status_code != 200:
                self.logger.info('current songs request failed: status code = %d' % response.status_code)
            elif result['success'] == False:
                self.logger.info('current songs request error: %s' % result['error'])

            # clear songs
            self.current_songs.remove()

        except Exception, e:
            self.logger.info('CurrentSongManager report exception: %s' % str(e))

    def store(self, radio_uuid, song_id):
        doc = {
                'radio_uuid': radio_uuid,
                'song_id': song_id
        }
        self.current_songs.update({'radio_uuid': radio_uuid}, doc, upsert=True)
