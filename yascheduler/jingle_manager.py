import settings
from datetime import datetime
from pymongo import ASCENDING
from logger import Logger

logger = Logger().log


class JingleManager():
    BETWEEN_SONGS_TYPE = 'between_songs'

    def __init__(self):
        self.model_db = settings.MONGO_DB.jingles
        self.db = settings.scheduler_db.jingle_status
        self.db.ensure_index("radio_uuid")
        self.db.ensure_index("model_id", unique=True)

    def reset(self):
        self.flush()
        for jingle in self.model_db.find():
            self.reset_jingle_info(jingle)

    def flush(self):
        self.db.drop()

    def count(self, radio_uuid=None):
        if radio_uuid == None:
            return self.db.count()
        else:
            return self.db.find({'radio_uuid': radio_uuid}).count()

    def jingles(self, radio_uuid=None):
        if radio_uuid == None:
            query = self.db.find().sort([('radio_uuid', ASCENDING)])
        else:
            query = self.db.find({'radio_uuid': radio_uuid}).sort([('radio_uuid', ASCENDING)])
        jingles = []
        for j in query:
            info = {
                'radio_uuid': j['radio_uuid'],
                'name': j['name'],
                'filename': j['filename'],
                'duration': j['duration'],
                'last_play_date': j['last_play_date'],
                'songs_to_wait': j['songs_to_wait'],
                'current_songs_to_wait': j['current_songs_to_wait']
            }
            jingles.append(info)
        return jingles

    def jingle_deleted(self, jingle_model_id):
        j = self.db.find_one({"model_id": jingle_model_id})
        if j == None:
            logger.info("jingle deleted: no jingle info for jingle with _id %s" % jingle_model_id)
            return

        self.remove_jingle_info(jingle_model_id)

    def jingle_added(self, jingle_model_id):
        j = self.model_db.find_one({"_id": jingle_model_id})
        if j == None:
            logger.info("jingle added: no jingle model with id %s" % jingle_model_id)
            return
        self.reset_jingle_info(j)

    def jingle_updated(self, jingle_model_id):
        j = self.model_db.find_one({"_id": jingle_model_id})
        if j == None:
            logger.info("jingle updated: no jingle model with id %s" % jingle_model_id)
            return
        self.reset_jingle_info(j)

    def remove_jingle_info(self, jingle_model_id):
        self.db.remove({"model_id": jingle_model_id})

    def reset_jingle_info(self, jingle_model):
        jingle_model_id = jingle_model['_id']
        radio_uuid = jingle_model['radio_uuid']
        filename = jingle_model['filename']
        name = jingle_model['name']
        duration = jingle_model.get('duration', None)
        if duration == None:
            logger.info('bad jingle doc (no "duration" field): %s' % jingle_model)
            return

        ok = False
        songs_to_wait = 0
        for schedule in jingle_model['schedule']:
            if schedule['type'] == self.BETWEEN_SONGS_TYPE:
                value = int(schedule['range'])
                if ok == True:
                    songs_to_wait = min(songs_to_wait, value)
                else:
                    songs_to_wait = value
                ok = True

        if ok == False:
            logger.info("reset jingle info: ERROR jingle manager can only handle jingles of type 'between_songs' => cannot handle %s" % jingle_model)
            self.remove_jingle_info(jingle_model_id)
            return

        doc = {
            'model_id': jingle_model_id,
            'radio_uuid': radio_uuid,
            'filename': filename,
            'duration': duration,
            'name': name,
            'songs_to_wait': songs_to_wait,
            'current_songs_to_wait': 0,
            'reset_date': datetime.now(),
            'last_play_date': None
        }
        self.db.update({'model_id': jingle_model_id}, doc, upsert=True)

    def get_jingle(self, radio_uuid):
        # decrement songs to wait for every jingle
        self.db.update({'radio_uuid': radio_uuid, 'current_songs_to_wait': {'$gt': 0}}, {'$inc': {'current_songs_to_wait': -1}}, multi=True)

        # get jingles ready to be chosen
        jingles = self.db.find({"radio_uuid": radio_uuid, "current_songs_to_wait": 0}).sort([('last_play_date', ASCENDING)])

        j = None
        if jingles != None and jingles.count() > 0:
            j = jingles[0]
            # update chosen jingle info
            self.db.update({'_id': j['_id']}, {'$set':
                {
                    'current_songs_to_wait': j['songs_to_wait'],
                    'last_play_date': datetime.now()
                }
            })
        return j
