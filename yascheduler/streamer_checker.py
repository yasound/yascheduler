import time
from threading import Thread
from gevent import Greenlet
import gevent

class StreamerChecker(Greenlet):
    WAIT_TIME = 15  # seconds

    def __init__(self, radio_scheduler):
        Greenlet.__init__(self)
        self.radio_scheduler = radio_scheduler

    def _run(self):
        quit = False
        while not quit:
            # unregister dead streamers (those who haven't answered to ping message)
            dead_streamers = self.radio_scheduler.streamers.find({'ping_status': self.radio_scheduler.STREAMER_PING_STATUS_WAITING})
            for dead in dead_streamers:
                self.radio_scheduler.logger.info('unregister streamer %s, it seems to be dead', dead['name'])
                self.radio_scheduler.unregister_streamer(dead['name'])

            # ping all streamers
            self.radio_scheduler.ping_all_streamers()

            # sleep
            gevent.sleep(self.WAIT_TIME)
