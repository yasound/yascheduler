import settings
from datetime import datetime


class RadioStateManager():

    def __init__(self):
        self.radio_states = settings.MONGO_DB.scheduler.radios.states
        self.radio_states.ensure_index('radio_uuid', unique=True)
        self.radio_states.ensure_index('song_end_time')

    def drop(self):
        self.radio_states.drop()

    def radio_state(self, radio_uuid):
        doc = self.radio_states.find_one({'radio_uuid': radio_uuid})
        if doc is None:
            return None
        radio_state = RadioState(doc)
        return radio_state

    def count(self, radio_uuid):
        count = self.radio_states.find({'radio_uuid': radio_uuid}, fields={'radio_uuid': True}).count()
        return count

    def remove(self, radio_uuid):
        # count = self.count(radio_uuid)
        # if count == 0:
        #     return False
        self.radio_states.remove({'radio_uuid': radio_uuid})
        return True

    def update(self, radio_state):
        doc = radio_state.as_doc()
        self.radio_states.update({'_id': radio_state._id}, doc, safe=True)

    def insert(self, radio_state):
        doc = radio_state.as_doc(include_id=False)
        self.radio_states.insert(doc, safe=True)

    def exists(self, radio_uuid):
        return self.count(radio_uuid) > 0

    def radio_uuids_for_master_streamer(self, master_streamer):
        return self.radio_states.find({'master_streamer': master_streamer}).distinct('radio_uuid')

    def broken_radios(self):
        """
        returns radios which are not being programmed or whose programming is broken_radios
        ie current song's end time does not exist or is over
        """
        now = datetime.now()
        docs = self.radio_states.find({'$or': [{'song_end_time': None}, {'song_end_time': {'$lt': now}}]})
        return docs


class RadioState:

    def __init__(self, data_dict={}):
        self.data_dict = data_dict
        self._id = data_dict.get('_id', None)
        self.radio_uuid = data_dict.get('radio_uuid', None)
        self.master_streamer = data_dict.get('master_streamer', None)
        self.song_id = data_dict.get('song_id', None)
        self.play_time = data_dict.get('play_time', None)
        self.show_id = data_dict.get('show_id', None)
        self.show_time = data_dict.get('show_time', None)
        self.song_end_time = None

    def as_doc(self, include_id=True):
        doc = {}
        doc['radio_uuid'] = self.radio_uuid
        doc['master_streamer'] = self.master_streamer
        doc['song_id'] = self.song_id
        doc['play_time'] = self.play_time
        doc['song_end_time'] = self.song_end_time
        doc['show_id'] = self.show_id
        doc['show_time'] = self.show_time
        if include_id:
            doc['_id'] = self._id
        return doc

    @property
    def is_playing(self):
        return self.master_streamer != None
