import settings
from pymongo import ASCENDING
# import time
# from threading import Thread, Event


# class StreamerChecker(Thread):
#     WAIT_TIME = 15  # seconds

#     def __init__(self, radio_scheduler):
#         Thread.__init__(self)
#         self.radio_scheduler = radio_scheduler
#         self.quit = Event()

#     def run(self):
#         while not self.quit.is_set():
#             # unregister dead streamers (those who haven't answered to ping message)
#             dead_streamers = self.radio_scheduler.streamers.find({'ping_status': self.radio_scheduler.STREAMER_PING_STATUS_WAITING})
#             for dead in dead_streamers:
#                 self.radio_scheduler.logger.info('unregister streamer %s, it seems to be dead', dead['name'])
#                 self.radio_scheduler.unregister_streamer(dead['name'])

#             # ping all streamers
#             self.radio_scheduler.ping_all_streamers()

#             # sleep
#             time.sleep(self.WAIT_TIME)

#     def join(self, timeout=None):
#         self.quit.set()
#         super(StreamerChecker, self).join(timeout)

class TransientRadioHistoryManager():
    TYPE_PLAYLIST_ADDED = 'playlist_added'
    TYPE_PLAYLIST_UPDATED = 'playlist_updated'
    TYPE_PLAYLIST_DELETED = 'playlist_deleted'

    playlist_event_types = (
        TYPE_PLAYLIST_ADDED,
        TYPE_PLAYLIST_UPDATED,
        TYPE_PLAYLIST_DELETED
        )

    TYPE_RADIO_ADDED = 'radio_added'
    TYPE_RADIO_DELETED = 'radio_deleted'

    radio_event_types = (
        TYPE_RADIO_ADDED,
        TYPE_RADIO_DELETED
        )

    def __init__(self):
        self.db = settings.MONGO_DB
        self.collection = self.db.scheduler.transient.radios

    def handle_events(self):
        while True:
            doc = self.collection.find_and_modify({}, sort={'updated': ASCENDING}, remove=True)
            if doc == None:
                break
            self.handle_event(doc)

    def handle_event(self, event_doc):
        event_type = event_doc['type']
        radio_uuid = event_doc['radio_uuid']
        playlist_id = event_doc['playlist_id']
        if event_type in self.radio_event_types and radio_uuid != None:
            self.handle_radio_event(event_type, radio_uuid)
        elif event_type in self.playlist_event_types and playlist_id != None:
            self.handle_playlist_event(event_type, playlist_id)

    def handle_radio_event(self, event_type, radio_uuid):
        pass

    def handle_playlist_event(self, event_type, playlist_id):
        pass
