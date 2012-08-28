import settings
from logger import Logger
from settings import yaapp_session_maker, yasound_session_maker
from models.yaapp_alchemy_models import Radio, Playlist, SongInstance, SongMetadata
from models.yasound_alchemy_models import YasoundSong
from datetime import datetime, timedelta, date
import time
from pymongo import ASCENDING, DESCENDING
from sqlalchemy import or_
import random
from threading import Thread, Lock
import redis
import json


class Track:
    def __init__(self, filename, duration, song=None, show=None):
        self.filename = filename
        self.duration = duration
        self.song = song
        self.show = show

    @property
    def is_song(self):
        return self.song is not None

    @property
    def is_from_show(self):
        return self.show is not None

    def __str__(self):
        if self.is_from_show:
            return '%s - %d seconds ***** show: "%s" song: "%s"' % (self.filename, self.duration, self.show['name'], self.song)
        elif self.is_song:
            return '%s - %d seconds ***** song: "%s"' % (self.filename, self.duration, self.song)
        return '%s - %d seconds' % (self.filename, self.duration)


class RedisListener(Thread):
    WAIT_TIME = 0.020  # seconds

    TYPE_MESSAGE_PLAY_RADIO = 'play_radio'
    TYPE_MESSAGE_STOP_RADIO = 'stop_radio'

    REDIS_LISTEN_CHANNEL = 'audiostreamer'

    def __init__(self, radio_scheduler):
        Thread.__init__(self)
        self.radio_scheduler = radio_scheduler

    def run(self):
        r = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)
        r = r.pubsub()
        channel = self.REDIS_LISTEN_CHANNEL
        r.subscribe(channel)
        quit = False
        while not quit:
            for message in r.listen():
                if message.get('type') != 'message':
                    continue
                data_str = message.get('data')
                data = json.loads(data_str)
                if data.get('type', None) == self.TYPE_MESSAGE_PLAY_RADIO:
                    self.radio_scheduler.receive_play_radio_message(data)
                elif data.get('type', None) == self.TYPE_MESSAGE_STOP_RADIO:
                    self.radio_scheduler.receive_stop_radio_message(data)
            time.sleep(self.WAIT_TIME)


class RadioScheduler():
    EVENT_TYPE_NEW_HOUR = 'new_hour'
    EVENT_TYPE_NEW_TRACK_PREPARE = 'prepare_new_track'
    EVENT_TYPE_NEW_TRACK_START = 'start_new_track'

    MESSAGE_TYPE_PLAY = 'play'
    MESSAGE_TYPE_RADIO_STARTED = 'radio_started'
    MESSAGE_TYPE_RADIO_STOPPED = 'radio_stopped'
    MESSAGE_TYPE_RADIO_EXISTS = 'radio_exists'

    DEFAULT_SECONDS_TO_WAIT = 0.050  # 50 milliseconds

    SONG_PREPARE_DURATION = 5  # seconds

    REDIS_PUBLISH_CHANNEL = 'yastream'

    def __init__(self):
        self.logger = Logger().log

        self.radio_events = settings.MONGO_DB.scheduler.radios.events
        self.radio_states = settings.MONGO_DB.scheduler.radios.states
        self.radio_states.ensure_index('radio_id', unique=True)

        self.yaapp_alchemy_session = yaapp_session_maker()
        self.yasound_alchemy_session = yasound_session_maker()

        self.shows = settings.MONGO_DB.shows

        self.current_step_time = datetime.now()
        self.last_step_time = self.current_step_time

        self.lock = Lock()

        self.cure_radio_events()

        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)

    def test(self):
        m = {'type': 'prout', 'name': 'blabla'}
        self.send_message(m)

    def run(self):
        # starts thread to listen to redis events
        thread = RedisListener(self)
        thread.start()

        #restart radios which should be active
        for radio_state in self.radio_states.find():
            radio_id = radio_state.get('radio_id', None)
            if radio_id is None:
                continue
            event_count = self.radio_events.find({'radio_id': radio_id, 'date': {'$gte': datetime.now()}}).count()
            print 'radio %d nb events %d' % (radio_id, event_count)
            # if there are events for this radio, next track will be computed
            # else, restart the radio
            if event_count == 0:
                self.start_radio(radio_id, radio_state['master_streamer'])

        quit = False
        self.last_step_time = datetime.now()
        while not quit:
            self.current_step_time = datetime.now()

            # find events between last step and now
            self.lock.acquire(True)
            events = self.radio_events.find({'date': {'$gt': self.last_step_time, '$lte': self.current_step_time}})
            self.lock.release()
            for e in events:
                # handle event
                self.handle_event(e)
                # remove event from list
                self.lock.acquire(True)
                self.radio_events.remove({'_id': e['_id']})
                self.lock.release()

            # find next event
            self.lock.acquire(True)
            next_events = self.radio_events.find().sort([('date', ASCENDING)]).limit(1)
            self.lock.release()
            next_event = None
            if next_events is not None and next_events.count() == 1:
                next_event = next_events[0]

            # compute seconds to wait until next event
            seconds_to_wait = self.DEFAULT_SECONDS_TO_WAIT
            if next_event is not None:
                next_date = next_event['date']
                diff_timedelta = next_date - datetime.now()
                seconds_to_wait = diff_timedelta.days * 86400 + diff_timedelta.seconds + diff_timedelta.microseconds / 1000000.0

            # waits until next event
            time.sleep(seconds_to_wait)

            # store date for next step
            self.last_step_time = self.current_step_time

    def handle_event(self, event):
        event_type = event.get('type', None)
        if event_type == self.EVENT_TYPE_NEW_HOUR:
            self.handle_new_hour(event)
        elif event_type == self.EVENT_TYPE_NEW_TRACK_START:
            self.handle_new_track_start(event)
        elif event_type == self.EVENT_TYPE_NEW_TRACK_PREPARE:
            self.handle_new_track_prepare(event)
        else:
            print 'event "%s" can not be handled: unknown type' % event

    def handle_new_hour(self, event):
        print self.EVENT_TYPE_NEW_HOUR
        #TODO

    def handle_new_track_start(self, event):
        print 'track start %s' % datetime.now().time().isoformat()
        radio_id = event.get('radio_id', None)
        if not radio_id:
            return
        song_id = event.get('song_id', None)
        show_id = event.get('show_id', None)

        radio_state = self.radio_states.find_one({'radio_id': radio_id})
        radio_state['song_id'] = song_id
        radio_state['play_time'] = self.current_step_time
        if show_id is None:
            radio_state['show_id'] = None
            radio_state['show_time'] = None
        elif ('show_id' not in radio_state) or (radio_state['show_id'] != show_id):
            # it's a new show
            radio_state['show_id'] = show_id
            radio_state['show_time'] = self.current_step_time
        # update the radio state in mongoDB
        self.radio_states.update({'_id': radio_state['_id']}, radio_state, safe=True)

        if song_id is not None:
            # a song is played (not a jingle)
            # 1 - update SongInstance status: play_count and last_play_time
            # 2 - update Radio status:  current_song
            self.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.id == song_id).update({SongInstance.play_count: SongInstance.play_count + 1, SongInstance.last_play_time: self.current_step_time})
            self.yaapp_alchemy_session.query(Radio).filter(Radio.id == radio_id).update({Radio.current_song_id: song_id})
            self.yaapp_alchemy_session.commit()
            #
            #TODO: report song as played => MONGO_DB.reports
            #

    # new 'track prepare' event has been received:
    def handle_new_track_prepare(self, event):
        print 'prepare track %s' % datetime.now().time().isoformat()
        radio_id = event.get('radio_id', None)
        if not radio_id:
            return
        delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        self.prepare_track(radio_id, delay_before_play)

    #   1 - get next track and send it to the streamer
    #   2 - create 'track start' event for this new track
    #   3 - create next 'track prepare' event
    def prepare_track(self, radio_id, delay_before_play):
        track = self.get_next_track(radio_id, delay_before_play)
        track_filename = track.filename
        track_duration = track.duration

        # 1 - send message to streamer
        dest_streamer = self.radio_states.find_one({'radio_id': radio_id})['master_streamer'] #  dest_streamer is the radio's master streamer
        self.send_prepare_track_message(radio_id, track_filename, delay_before_play, dest_streamer)

        # 2 store 'track start' event
        event = {
                'type': self.EVENT_TYPE_NEW_TRACK_START,
                'date': self.current_step_time + timedelta(seconds=delay_before_play),
                'radio_id': radio_id,
                'filename': track_filename,
        }
        if track.is_song:
            event['song_id'] = track.song.id  # add the song id in the event if the track is a song
        if track.is_from_show:
            event['show_id'] = track.show
        self.lock.acquire(True)
        self.radio_events.insert(event, safe=True)
        self.lock.release()

        # 3 - store next 'track prepare' event
        next_delay_before_play = self.SONG_PREPARE_DURATION
        next_date = self.current_step_time + timedelta(seconds=delay_before_play) + timedelta(seconds=track_duration) - timedelta(seconds=next_delay_before_play)
        event = {
                'type': self.EVENT_TYPE_NEW_TRACK_PREPARE,
                'date': next_date,
                'radio_id': radio_id,
                'delay_before_play': next_delay_before_play,
        }
        self.lock.acquire(True)
        self.radio_events.insert(event, safe=True)
        self.lock.release()

    # get current show if exists
    def get_current_show(self, shows, play_time):
        current = None
        week_days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
        for s in shows:
            show_days = s['days']
            show_duration = s['duration']
            show_time = s['time']
            show_start_time = datetime.strptime(show_time, '%H:%M:%S.%f').time()
            show_end_time = (datetime.strptime(show_time, '%H:%M:%S.%f') + timedelta(seconds=show_duration)).time()
            # be dure to handle every case, included cases where a show starts a day and ends the day after
            # today 23:00 => tomorrow 1:00
            # or yesterday 23:00 => today 1:00
            today_start_date = datetime.combine(date.today(), show_start_time)
            if play_time >= today_start_date:
                end_date = today_start_date + timedelta(seconds=show_duration)
                if play_time < end_date:
                    start_day = week_days[today_start_date.weekday()]
                    if start_day in show_days:
                        current = s
                        break
            today_end_date = datetime.combine(date.today(), show_end_time)
            if play_time < today_end_date:
                start_date = today_end_date - timedelta(seconds=show_duration)
                if play_time >= start_date:
                    start_day = week_days[start_date.weekday()]
                    if start_day in show_days:
                        current = s
                        break
        return current

    def get_next_track(self, radio_id, delay_before_play):
        play_time = self.current_step_time + timedelta(seconds=delay_before_play)

        playlists = self.yaapp_alchemy_session.query(Playlist).filter(Playlist.radio_id == radio_id)
        playlist_ids = [x[0] for x in playlists.values(Playlist.id)]
        shows = self.shows.find({'playlist_id': {'$in': playlist_ids}, 'enabled': True}).sort([('time', DESCENDING)])
        # check if one of those shows is currently 'on air'
        show_current = self.get_current_show(shows, play_time)

        #FIXME: handle all cases (jingles, time jingles)
        track = None
        if show_current:
            track = self.get_song_in_show(radio_id, show_current['_id'], play_time)
        if track is None:
            track = self.get_song_default(radio_id, play_time)
        return track

    # returns Track object
    def get_random_song(self, playlist, play_time):
        time_limit = play_time - timedelta(hours=3)
        # SongInstance playlist == playlist
        # SongInstance enabled == True
        # SongInstance last_play_time is None or < time_limit
        # SongMetadata yasound_song_id > 0
        # order by last_play_time
        query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist.id, SongInstance.enabled == True, or_(SongInstance.last_play_time < time_limit, SongInstance.last_play_time == None), SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
        count = query.count()
        if count == 0:  # try without time limit
            query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist.id, SongInstance.enabled == True, SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
            count = query.count()
        if count == 0:
            self.logger.info('no song available for playlist %d' % playlist.id)
            return None

        frequencies = [x[0] for x in query.values(SongInstance.frequency)]
        # use frequency * frequency to have high frequencies very different from low frequencies
        # multiply frequency weight by a date factor to have higher probabilities for songs not played since a long time (date factor = 1 for older song, 0.15 for more recent one)
        first_idx_factor = 1
        last_idx_factor = 0.15
        if (count - 1) == 0:
            date_factor_func = lambda x: 1
        else:
            date_factor_func = lambda x: ((last_idx_factor - first_idx_factor) / (count - 1)) * x + first_idx_factor
        weights = [x * x * date_factor_func(idx) for idx, x in enumerate(frequencies)]
        r = random.random()
        sum_weight = sum(weights)
        rnd = r * sum_weight
        index = -1
        for i, w in enumerate(weights):
            rnd -= w
            if rnd <= 0:
                index = i
                break
        if index == -1:
            if count > 0:
                index = 0
            else:
                return None

        song = query.limit(index + 1)[index]
        yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
        track = Track(yasound_song.filename, yasound_song.duration, song=song)
        return track

    def get_song_default(self, radio_id, play_time):
        playlist = self.yaapp_alchemy_session.query(Playlist).filter(Playlist.radio_id == radio_id, Playlist.name == 'default').first()
        if not playlist:
            return None
        track = self.get_random_song(playlist, play_time)
        return track

    def get_song_in_show(self, radio_id, show_id, play_time):
        show = self.shows.find_one({'_id': show_id})
        if show is None:
            return None

        playlist = self.yaapp_alchemy_session.query(Playlist).get(show['playlist_id'])
        if not playlist:
            return None
        random_play = show['random_play']
        if random_play:
            track = self.get_random_song(playlist, play_time)
            track.show = show
        else:
            radio_state = self.radio_states.find_one({'radio_id': radio_id})
            previous_order = 0
            if radio_state['show_id'] is not None:
                # song stored in radio_state is a song from the show, so it has a valid order value
                previous_song = self.yaapp_alchemy_session.query(SongInstance).get(radio_state['song_id'])
                previous_order = previous_song.order
            song = self.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.playlist_id == playlist.id, SongInstance.order > previous_order).first()
            if song is None:
                return None
            yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
            track = Track(yasound_song.filename, yasound_song.duration, song=song, show=show)
        return track

    def get_radio_jingle(self, radio_id):
        print 'get radio jingle'

    def get_time_jingle(self, radio_id):
        print 'get time jingle'

    def send_prepare_track_message(self, radio_id, track_filename, delay, dest_streamer):
        message = {'type': self.MESSAGE_TYPE_PLAY,
                    'radio_id': radio_id,
                    'filename': track_filename,
                    'delay': delay
        }
        self.send_message(message, dest_streamer)

    def send_radio_started_message(self, radio_id, dest_streamer):
        message = {'type': self.MESSAGE_TYPE_RADIO_STARTED,
                    'radio_id': radio_id,
                    'master_streamer': dest_streamer
        }
        self.send_message(message, dest_streamer)

    #  send message to notify the streamer that the radio is already handled by another streamer: master_streamer
    def send_radio_exists_message(self, radio_id, dest_streamer, master_streamer):
        message = {'type': self.MESSAGE_TYPE_RADIO_EXISTS,
                    'radio_id': radio_id,
                    'master_streamer': master_streamer
        }
        self.send_message(message, dest_streamer)

    def send_radio_stopped_message(self, radio_id, dest_streamer):
        message = {'type': self.MESSAGE_TYPE_RADIO_STOPPED,
                    'radio_id': radio_id
        }
        self.send_message(message, dest_streamer)

    def send_message(self, message, streamer=None):
        m = json.dumps(message)
        channel = self.REDIS_PUBLISH_CHANNEL
        if streamer is not None:
            channel += '.%s' % (streamer)
        self.redis.publish(channel, m)

    def receive_play_radio_message(self, data):
        radio_id = data.get('radio_id', None)
        streamer = data.get('streamer', None)
        if radio_id is None or streamer is None:
            return
        radio_state = self.radio_states.find_one({'radio_id': radio_id})
        print 'radio_state:'
        print radio_state
        if radio_state is None:
            #  create radio
            self.send_radio_started_message(radio_id, streamer)
            self.start_radio(radio_id, streamer)
        else:
            # radio already exists
            master_streamer = radio_state.get('master_streamer', None)
            self.send_radio_exists_message(radio_id, streamer, master_streamer)

    def receive_stop_radio_message(self, data):
        radio_id = data.get('radio_id', None)
        streamer = data.get('streamer', None)
        self.stop_radio(radio_id)
        self.send_radio_stopped_message(radio_id, streamer)

    def start_radio(self, radio_id, master_streamer):
        self.clean_radio(radio_id)
        # create radio state
        radio_state = {'radio_id': radio_id,
                        'master_streamer': master_streamer
        }
        self.radio_states.insert(radio_state, safe=True)
        # prepare first track
        self.prepare_track(radio_id, 0)  # no delay

    def stop_radio(self, radio_id):
        self.clean_radio(radio_id)

    def clean_radio_events(self, radio_id):
        self.lock.acquire(True)
        self.radio_events.remove({'radio_id': radio_id})
        self.lock.release()

    def clean_radio_states(self, radio_id):
        self.radio_states.remove({'radio_id': radio_id})

    def clean_radio(self, radio_id):
        self.clean_radio_events(radio_id)
        self.clean_radio_states(radio_id)

    def cure_radio_events(self):
        self.lock.acquire(True)
        self.radio_events.remove({'date': {'$lt': datetime.now()}})
        self.lock.release()
