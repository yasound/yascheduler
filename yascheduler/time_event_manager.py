import settings
from datetime import datetime
from time import mktime
from blist import sortedlist
import cPickle
from threading import Thread, Event
import time
from logger import Logger
import os

logger = Logger().log

def time_event_type_to_string(time_event_type):
    if time_event_type == TimeEvent.EVENT_TYPE_NEW_HOUR_PREPARE:
        return 'new hour prepare'
    elif time_event_type == TimeEvent.EVENT_TYPE_NEW_TRACK_PREPARE:
        return 'track prepare'
    elif time_event_type == TimeEvent.EVENT_TYPE_NEW_TRACK_START:
        return 'track start'
    elif time_event_type == TimeEvent.EVENT_TYPE_TRACK_CONTINUE:
        return 'track continue'
    elif time_event_type == TimeEvent.EVENT_TYPE_CHECK_EXISTING_RADIOS:
        return 'check existing radios'
    elif time_event_type == TimeEvent.EVENT_TYPE_CHECK_PROGRAMMING:
        return 'check programming'
    return None


class TimeEvent():
    EVENT_TYPE_NEW_HOUR_PREPARE = 1
    EVENT_TYPE_NEW_TRACK_PREPARE = 2
    EVENT_TYPE_NEW_TRACK_START = 3
    EVENT_TYPE_TRACK_CONTINUE = 4
    EVENT_TYPE_CHECK_EXISTING_RADIOS = 5
    EVENT_TYPE_CHECK_PROGRAMMING = 6

    def __init__(self, event_type, date):
        self.event_type = event_type
        self.date = date
        self.t = mktime(self.date.timetuple()) + self.date.microsecond * 1e-6

    def __eq__(self, other):
        return self.t == other.t

    def __gt__(self, other):
        return self.t > other.t

    def __str__(self):
        return '%s - %s' % (self.event_type, self.date)


class TimeEventSaver(Thread):
    WAIT_TIME = 20  # seconds

    def __init__(self, manager):
        Thread.__init__(self)
        self.manager = manager
        self.quit = Event()

    def run(self):
        while not self.quit.is_set():
            self.manager.save()

            time.sleep(self.WAIT_TIME)

    def join(self, timeout=None):
        self.quit.set()
        super(TimeEventSaver, self).join(timeout)


class TimeEventManager():

    def __init__(self):
        ok = self.load()
        if ok == False:
            self.time_events = sortedlist()

        self.saver = TimeEventSaver(self)

    def start_saver(self):
        self.saver.start()

    def join_saver(self, timeout=None):
        self.saver.join(timeout)

    def clear(self):
        self.time_events = sortedlist()

    def count(self):
        return len(self.time_events)

    def insert(self, event):
        self.time_events.add(event)

    def pop_past_events(self, ref_date):
        events = []
        while len(self.time_events) > 0 and self.time_events[0].date <= ref_date:
            e = self.time_events.pop(0)
            events.append(e)
        return events

    def time_till_next(self, ref_date):
        if self.count() == 0:
            return None

        next_date = self.time_events[0].date
        diff_timedelta = next_date - datetime.now()
        seconds_to_wait = diff_timedelta.days * 86400 + diff_timedelta.seconds + diff_timedelta.microseconds / 1000000.0
        seconds_to_wait = max(seconds_to_wait, 0)
        return seconds_to_wait

    def scheduled_radios(self):
        uuids = set()
        for e in self.time_events:
            if hasattr(e, 'radio_uuid'):
                uuids.add(e.radio_uuid)
        return uuids

    def remove_radio_events(self, radio_uuid):
        for i in range(len(self.time_events) - 1, 0, -1):
            event = self.time_events[i]
            if hasattr(event, 'radio_uuid') and event.radio_uuid == radio_uuid:
                self.time_events.pop(i)

    def contains_event_type(self, event_type):
        for event in self.time_events:
            if event.event_type == event_type:
                return True
        return False

    def save(self):
        logger.info('save time events (%d elements)' % len(self.time_events))
        f = open(settings.TIME_EVENTS_SAVE_FILENAME, 'w')
        cPickle.dump(self.time_events, f)

    def load(self):
        logger.info('load time events')
        try:
            f = open(settings.TIME_EVENTS_SAVE_FILENAME, 'r')
        except:
            logger.info('load time events FAILED')
            return False
        time_events = cPickle.load(f)
        self.time_events = time_events
        logger.info('load time events OK (%d elements)' % len(self.time_events))
        return True

    def flush(self):
        try:
            os.remove(settings.TIME_EVENTS_SAVE_FILENAME)
        except Exception, e:
            self.logger.info('TimeEventManager flush exception: %s' % str(e))
