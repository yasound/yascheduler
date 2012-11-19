import settings
from pymongo import ASCENDING
from datetime import datetime


class TimeEventManager():
    EVENT_TYPE_NEW_HOUR_PREPARE = 'prepare_new_hour'
    EVENT_TYPE_NEW_TRACK_PREPARE = 'prepare_new_track'
    EVENT_TYPE_NEW_TRACK_START = 'start_new_track'
    EVENT_TYPE_TRACK_CONTINUE = 'continue_track'
    EVENT_TYPE_CHECK_PROGRAMMING = 'check_programming'

    def __init__(self):
        self.time_events = settings.MONGO_DB.scheduler.radios.events
        self.time_events.ensure_index('radio_uuid')
        self.time_events.ensure_index('date')
        self.time_events.ensure_index('type')

    def clear(self):
        self.time_events.drop()

    def count(self):
        return self.time_events.find().count()

    def insert(self, event_doc):
        self.time_events.insert(event_doc, safe=True)

    def pop_past_events(self, ref_date):
        events_query = {'date': {'$lte': ref_date}}
        events = list(self.time_events.find(events_query).sort([('date', ASCENDING)]))
        self.remove_past_events(ref_date)
        return events

    def time_till_next(self, ref_date):
        next_events = self.time_events.find({'date': {'$gt': ref_date}}, {'date': True}).sort([('date', ASCENDING)]).limit(1)
        if next_events is None or next_events.count() == 0:
            return None

        next_event = next_events[0]
        next_date = next_event['date']
        diff_timedelta = next_date - datetime.now()
        seconds_to_wait = diff_timedelta.days * 86400 + diff_timedelta.seconds + diff_timedelta.microseconds / 1000000.0
        seconds_to_wait = max(seconds_to_wait, 0)
        return seconds_to_wait

    def scheduled_radios(self):
        uuids = frozenset([doc.get('radio_uuid') for doc in self.time_events.find(fields={'radio_uuid': True})])
        return uuids

    def remove_radio_events(self, radio_uuid):
        self.time_events.remove({'radio_uuid': radio_uuid})

    def remove_past_events(self, ref_date):
        self.time_events.remove({'date': {'$lte': ref_date}})

    def contains_event_type(self, event_type):
        doc = self.time_events.find_one({'type': event_type}, fields={'type': True})
        return doc != None
