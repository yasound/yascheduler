import settings
from pymongo import ASCENDING
import time
from threading import Thread, Event, Lock
import redis
from logger import Logger

logger = Logger().log


class RadioHistoryEventChecker(Thread):
    """
    thread which regularly handle events
    """
    WAIT_TIME = 10  # seconds

    def __init__(self, manager):
        Thread.__init__(self)
        self.manager = manager
        self.quit = Event()

    def run(self):
        while not self.quit.is_set():
            self.manager.handle_events()

            # sleep
            time.sleep(self.WAIT_TIME)

    def join(self, timeout=None):
        self.quit.set()
        super(RadioHistoryEventChecker, self).join(timeout)


class RadioHistoryRedisListener(Thread):
    """
    listens to redis messages from yaapp to handle new events
    """
    REDIS_DB = 0
    REDIS_CHANNEL = 'yaapp'

    def __init__(self, manager):
        Thread.__init__(self)
        self.manager = manager

    def run(self):
        r = redis.StrictRedis(host=settings.REDIS_HOST, db=self.REDIS_DB)
        self.pubsub = r.pubsub()
        channel = self.REDIS_CHANNEL
        self.pubsub.subscribe(channel)
        for message in self.pubsub.listen():
            if message.get('event_type') == 'radio_updated':
                logger.info('RadioHistoryRedisListener: radio_updated received')
                self.manager.handle_events()

    def join(self, timeout=None):
        self.pubsub.unsubscribe(self.REDIS_CHANNEL)
        super(RadioHistoryRedisListener, self).join(timeout)


class TransientRadioHistoryManager():
    """
    Gets events from yaapp when there are radio/playlist modifications
    and notifies yascheduler
    """
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

    def __init__(self, radio_event_handlers=[], playlist_event_handlers=[]):
        self.radio_event_handlers = radio_event_handlers
        self.playlist_event_handlers = playlist_event_handlers

        self.db = settings.MONGO_DB
        self.collection = self.db.scheduler.transient.radios

        self.lock = Lock()

        self.checker = RadioHistoryEventChecker(self)
        self.listener = RadioHistoryRedisListener(self)

    def start(self):
        self.checker.start()
        self.listener.start()

    def join(self, timeout=None):
        self.checker.join(timeout)
        self.listener.join(timeout)

    def handle_events(self):
        logger.info('TransientRadioHistoryManager handle events')
        self.lock.acquire()
        while True:
            doc = self.collection.find_and_modify({}, sort={'updated': ASCENDING}, remove=True)
            if doc == None:
                break
            self.handle_event(doc)
        self.lock.release()
        logger.info('TransientRadioHistoryManager handle events DONE')

    def handle_event(self, event_doc):
        event_type = event_doc['type']
        radio_uuid = event_doc['radio_uuid']
        playlist_id = event_doc['playlist_id']
        if event_type in self.radio_event_types and radio_uuid != None:
            self.handle_radio_event(event_type, radio_uuid)
        elif event_type in self.playlist_event_types and playlist_id != None:
            self.handle_playlist_event(event_type, radio_uuid, playlist_id)

    def handle_radio_event(self, event_type, radio_uuid):
        logger.info('TransientRadioHistoryManager: %s - %s' % (event_type, radio_uuid))
        for func in self.radio_event_handlers:
            func(event_type, radio_uuid)

    def handle_playlist_event(self, event_type, radio_uuid, playlist_id):
        logger.info('TransientRadioHistoryManager: %s - %s' % (event_type, playlist_id))
        for func in self.playlist_event_handlers:
            func(event_type, radio_uuid, playlist_id)
