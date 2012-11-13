from datetime import datetime
from time import mktime
from blist import sortedlist


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


class TimeEventManager():

    def __init__(self):
        self.time_events = sortedlist()

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
                uuids.append(e.radio_uuid)
        return uuids

    def remove_radio_events(self, radio_uuid):
        for i in range(len(self.time_events) - 1, 0, -1):
            event = self.time_events[i]
            if hasattr(event, 'radio_uuid') and event.radio_uuid == radio_uuid:
                self.time_events.pop(i)

    def remove_past_events(self, ref_date):
        self.pop_past_events(ref_date)

    def contains_event_type(self, event_type):
        for event in self.time_events:
            if event.event_type == event_type:
                return True
        return False
