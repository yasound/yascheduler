import settings
from logger import Logger
# from settings import yaapp_session_maker, yasound_session_maker
from models.yaapp_alchemy_models import Radio, Playlist, SongInstance, SongMetadata
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User
from datetime import datetime, timedelta, date
import time
from pymongo import ASCENDING, DESCENDING
from sqlalchemy import or_
import random
from threading import Thread, Lock
import json
import requests
from streamer_checker import StreamerChecker
from redis_listener import RedisListener
from track import Track
from redis_publisher import RedisPublisher
from radio_state import RadioStateManager, RadioState
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


class RadioScheduler():
    EVENT_TYPE_NEW_HOUR_PREPARE = 'prepare_new_hour'
    EVENT_TYPE_NEW_TRACK_PREPARE = 'prepare_new_track'
    EVENT_TYPE_NEW_TRACK_START = 'start_new_track'
    EVENT_TYPE_TRACK_CONTINUE = 'continue_track'
    EVENT_TYPE_CHECK_EXISTING_RADIOS = 'check_existing_radios'
    EVENT_TYPE_CHECK_PROGRAMMING = 'check_programming'
    EVENT_TYPE_REPORT_SONG_STARTED = 'report_songs_started'

    STREAMER_PING_STATUS_OK = 'ok'
    STREAMER_PING_STATUS_WAITING = 'waiting'

    DEFAULT_SECONDS_TO_WAIT = 0.050  # 50 milliseconds

    SONG_PREPARE_DURATION = 5  # seconds
    CROSSFADE_DURATION = 1  # seconds
    CHECK_EXISTING_RADIOS_PERIOD = 10 * 60
    CHECK_PROGRAMMING_PERIOD = 30
    REPORT_SONGS_STARTED_PERIOD = 2

    def __init__(self, enable_ping_streamers=True, flush=False, enable_programming_check=False):
        self.current_step_time = datetime.now()
        self.last_step_time = self.current_step_time

        self.logger = Logger().log
        self.publisher = RedisPublisher('yastream')

        self.enable_ping_streamers = enable_ping_streamers
        self.flush_before_run = flush
        self.enable_programming_check = enable_programming_check

        self.mongo_scheduler = settings.MONGO_DB.scheduler

        self.radio_events = self.mongo_scheduler.radios.events
        self.radio_events.ensure_index('radio_uuid')
        self.radio_events.ensure_index('date')
        self.radio_events.ensure_index('type')

        self.radio_state_manager = RadioStateManager()

        self.streamers = self.mongo_scheduler.streamers
        self.streamers.ensure_index('name', unique=True)

        self.listeners = self.mongo_scheduler.listeners
        self.listeners.ensure_index('radio_uuid')
        self.listeners.ensure_index('session_id', unique=True)

        self.songs_started = self.mongo_scheduler.songs_started

        # self.yaapp_alchemy_session = yaapp_session_maker()
        # self.yasound_alchemy_session = yasound_session_maker()
        session_factory = sessionmaker(bind=settings.yaapp_alchemy_engine)
        self.yaapp_alchemy_session = scoped_session(session_factory)

        session_factory = sessionmaker(bind=settings.yasound_alchemy_engine)
        self.yasound_alchemy_session = scoped_session(session_factory)

        self.shows = settings.MONGO_DB.shows

        self.lock = Lock()

        # remove past events (those which sould have occured when the scheduler was off)
        self.cure_radio_events()

    def clear_mongo(self):
        self.radio_events.drop()
        self.radio_state_manager.drop()

        self.streamers.drop()
        self.listeners.drop()

    def flush(self):
        self.logger.debug('flush...')
        self.clear_mongo()
        self.logger.debug('flushed')

    def run(self):
        self.last_step_time = datetime.now()

        # starts thread to listen to redis events
        self.redis_listener = RedisListener(self)
        self.redis_listener.start()

        # flush if needed
        if self.flush_before_run:
            self.flush()
            return

        # starts streamer checker thread
        if self.enable_ping_streamers:
            checker = StreamerChecker(self)
            checker.start()

        # check existing radios
        self.check_existing_radios()

        # prepare track for radios with no event in the future (events which should have occured when the scheduler was off and which have been cured)
        for radio_state_doc in self.radio_state_manager.radio_states.find():
            radio_uuid = radio_state_doc.get('radio_uuid', None)
            if radio_uuid is None:
                continue
            event_count = self.radio_events.find({'radio_uuid': radio_uuid}).count()
            # if there are events for this radio, next track will be computed later
            # else, prepare a new track now
            if event_count == 0:
                delay_before_play = 0
                crossfade_duration = 0
                self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)

        # add the event for the next hour if it does not exist yet
        hour_event_count = self.radio_events.find({'type': self.EVENT_TYPE_NEW_HOUR_PREPARE}).count()
        if hour_event_count == 0:
            self.add_next_hour_event()

        # add the event to check the existing radios if it does not exist yet
        check_event_count = self.radio_events.find({'type': self.EVENT_TYPE_CHECK_EXISTING_RADIOS}).count()
        if check_event_count == 0:
            self.add_next_check_radios_event(self.CHECK_EXISTING_RADIOS_PERIOD)

        # add the event to check if there is no problem with radios' programming
        if self.enable_programming_check:
            self.add_next_check_programming_event(self.CHECK_PROGRAMMING_PERIOD)

        # add the event to report song started if it does not exist
        if self.radio_events.find({'type': self.EVENT_TYPE_REPORT_SONG_STARTED}).count() == 0:
            self.add_next_report_songs_event()

        quit = False
        while not quit:
            self.current_step_time = datetime.now()

            # find events between last step and now
            self.lock.acquire(True)
            events = self.radio_events.find({'date': {'$lte': self.current_step_time}})
            self.lock.release()
            self.logger.info('loop... handle %d events' % events.count())

            for e in events:
                # handle event
                self.handle_event(e)
                # remove event from list
                self.lock.acquire(True)
                self.radio_events.remove({'_id': e['_id']})
                self.lock.release()

            # find next event
            self.lock.acquire(True)
            next_events = self.radio_events.find({'date': {'$gt': self.current_step_time}}).sort([('date', ASCENDING)]).limit(1)
            self.lock.release()
            next_event = None
            if next_events is not None and next_events.count() >= 1:
                next_event = next_events[0]
            else:
                self.logger.debug('next events: %s' % next_events)

            # compute seconds to wait until next event
            seconds_to_wait = self.DEFAULT_SECONDS_TO_WAIT
            if next_event is not None:
                next_date = next_event['date']
                diff_timedelta = next_date - datetime.now()
                seconds_to_wait = diff_timedelta.days * 86400 + diff_timedelta.seconds + diff_timedelta.microseconds / 1000000.0
                seconds_to_wait = max(seconds_to_wait, 0)
                self.logger.debug('%s seconds until next event' % seconds_to_wait)
                self.logger.debug('next event: %s' % next_event)

            # waits until next event
            time.sleep(seconds_to_wait)

            # store date for next step
            self.last_step_time = self.current_step_time

    def handle_event(self, event):
        event_type = event.get('type', None)
        self.logger.info('handle event ___%s___ (%s)' % (event_type, event['date']))
        if event_type == self.EVENT_TYPE_NEW_HOUR_PREPARE:
            self.handle_new_hour_prepare(event)
        elif event_type == self.EVENT_TYPE_NEW_TRACK_START:
            self.handle_new_track_start(event)
        elif event_type == self.EVENT_TYPE_NEW_TRACK_PREPARE:
            self.handle_new_track_prepare(event)
        elif event_type == self.EVENT_TYPE_TRACK_CONTINUE:
            self.handle_track_continue(event)
        elif event_type == self.EVENT_TYPE_CHECK_EXISTING_RADIOS:
            self.handle_check_existing_radios(event)
        elif event_type == self.EVENT_TYPE_CHECK_PROGRAMMING:
            self.handle_check_programming(event)
        elif event_type == self.EVENT_TYPE_REPORT_SONG_STARTED:
            self.handle_report_songs(event)
        else:
            self.logger.info('event "%s" can not be handled: unknown type' % event)

    def handle_check_programming(self, event):
        self.check_programming()
        # add next check event
        self.add_next_check_programming_event(self.CHECK_PROGRAMMING_PERIOD)

    def handle_check_existing_radios(self, event):
        self.check_existing_radios()
        # add next check event
        self.add_next_check_radios_event(self.CHECK_EXISTING_RADIOS_PERIOD)

    def handle_report_songs(self, event):
        self.report_songs_started()
        # add next report event
        self.add_next_report_songs_event()

    def handle_new_hour_prepare(self, event):
        self.logger.info('prepare new hour %s' % datetime.now().time().isoformat())
        delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        time = event.get('time', None)  # time value is a string like '17:17:13.129150'
        if time is None:
            self.logger.info('time event does not contain a valid "time" param: %s', event)
            return

        query = {
                    'master_streamer':  {'$exists': True, '$ne': None},
                    'song_id':          {'$exists': True, '$ne': None},
                    'play_time':        {'$exists': True, '$ne': None}
        }
        radio_uuids = self.radio_state_manager.radio_states.find(query).distinct('radio_uuid')
        for uuid in radio_uuids:
            jingle_track = self.get_time_jingle_track(uuid, time)
            if jingle_track is not None:
                radio_state = self.radio_state_manager.radio_state(uuid)
                current_song_played = datetime.now() + timedelta(delay_before_play) - radio_state.play_time  # offset in current song where to restart playing after jingle
                current_song_id = radio_state.song_id

                # tell master streamer to play the jingle file
                delay = delay_before_play
                self.publisher.send_prepare_track_message(uuid, jingle_track.filename, delay, 0, crossfade_duration, radio_state.master_streamer)

                # remove netx "prepare track" event since the current song is paused
                self.radio_events.remove({'type': self.EVENT_TYPE_NEW_TRACK_PREPARE, 'radio_uuid': uuid})

                # store event to restart playing current song after jingle has finished
                event = {
                            'type': self.EVENT_TYPE_TRACK_CONTINUE,
                            'date': self.current_step_time + timedelta(seconds=(delay_before_play + jingle_track.duration - self.CROSSFADE_DURATION - self.SONG_PREPARE_DURATION)),
                            'radio_uuid': uuid,
                            'song_id': current_song_id,
                            'offset': current_song_played,
                            'delay_before_play': self.SONG_PREPARE_DURATION,
                            'crossfade_duration': self.CROSSFADE_DURATION
                }
                self.lock.acquire(True)
                self.radio_events.insert(event, safe=True)
                self.lock.release()

        # add next hour event
        self.add_next_hour_event()

    def handle_track_continue(self, event):
        """
        restart a track after it has been paused (for example, because of a time jingle)
        """
        self.logger.info('track continue %s' % datetime.now().time().isoformat())
        radio_uuid = event.get('radio_uuid', None)
        song_id = event.get('song_id', None)
        offset = event.get('offset', None)
        delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        if radio_uuid is None:
            self.logger.info('"track continue" event does not contain a valid radio uuid   (%s)', event)
            return
        if song_id is None or offset is None:
            # event does not contain a valid song id and play offset =>  start a new song !
            self.logger.info('"track continue" event does not contain a valid song id and play offset: start a new song !   (%s)', event)
            self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)
            return

        song = self.yaapp_alchemy_session.query(SongInstance).get(song_id)
        yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
        track = Track(yasound_song.filename, yasound_song.duration, song_id=song_id)
        self.play_track(radio_uuid, track, delay_before_play, offset, crossfade_duration)

    def handle_new_track_start(self, event):
        # self.logger.info('track start %s' % datetime.now().time().isoformat())
        # debug duration
        begin = datetime.now()
        #
        radio_uuid = event.get('radio_uuid', None)
        if not radio_uuid:
            self.logger.debug('handle_new_track_start ERROR: radio uuid is none in event')
            return
        song_id = event.get('song_id', None)
        show_id = event.get('show_id', None)

        self.logger.debug('track start (%s - %s)' % (radio_uuid, song_id))

        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        radio_state.song_id = song_id
        radio_state.play_time = self.current_step_time
        if show_id is None:
            radio_state.show_id = None
            radio_state.show_time = None
        elif radio_state.show_id != show_id:
            # it's a new show
            radio_state.show_id = show_id
            radio_state.show_time = self.current_step_time
        # update the radio state in mongoDB
        self.radio_state_manager.update(radio_state)

        if song_id is not None:
            # store song played in order to notify yaapp in the next EVENT_TYPE_REPORT_SONG_STARTED event
            doc = {
                    'radio_uuid': radio_uuid,
                    'song_id': song_id
                    }
            self.songs_started.insert(doc, safe=True)


        # debug duration
        elapsed = datetime.now() - begin
        self.logger.debug('- handle_new_track_start %s' % elapsed)
        #

    def handle_new_track_prepare(self, event):
        """
        new 'track prepare' event has been received
        we have a delay before a new track must be played in order to prepare this track
        """
        # debug duration
        begin = datetime.now()
        #
        # self.logger.info('prepare track %s' % datetime.now().time().isoformat())
        radio_uuid = event.get('radio_uuid', None)
        if not radio_uuid:
            return
        delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)
        # debug duration
        elapsed = datetime.now() - begin
        self.logger.debug('handle_new_track_prepare %s' % elapsed)
        #

    def play_track(self, radio_uuid, track, delay, offset, crossfade_duration):
        """
        1 - if the radio is currently listened (ie if a streamer builds its mp3 stream), ask the streamer to play this track
        2 - add a future event to prepare the next track
        """
        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        master_streamer = radio_state.master_streamer  # dest_streamer is the radio's master streamer
        if master_streamer is not None:
            message = self.publisher.send_prepare_track_message(radio_uuid, track.filename, delay, offset, crossfade_duration, master_streamer)
        else:
            message = None

        # 2 - store next 'track prepare' event
        next_delay_before_play = self.SONG_PREPARE_DURATION
        next_crossfade_duration = self.CROSSFADE_DURATION
        next_date = self.current_step_time + timedelta(seconds=(delay + track.duration - offset - next_crossfade_duration - next_delay_before_play))
        self.logger.debug('store next prepare track (file DURATION: %s)' % track.duration)
        event = {
                'type': self.EVENT_TYPE_NEW_TRACK_PREPARE,
                'date': next_date,
                'radio_uuid': radio_uuid,
                'delay_before_play': next_delay_before_play,
                'crossfade_duration': next_crossfade_duration
        }
        self.lock.acquire(True)
        self.radio_events.insert(event, safe=True)
        self.lock.release()

        return message

    def prepare_track(self, radio_uuid, delay_before_play, crossfade_duration):
        """
        1 - get next track
        2 - create 'track start' event for this new track
        3 - send it to the streamer
        4 - create next 'track prepare' event
        """
        # debug duration
        begin = datetime.now()
        #
        self.logger.debug('\nprepare track (%s)' % radio_uuid)
        # 1 get next track
        track = self.get_next_track(radio_uuid, delay_before_play)
        if track is None:
            self.logger.debug('prepare_track ERROR: cannot get next track')
            return None
        track_filename = track.filename
        offset = 0

        # 2 store 'track start' event
        event = {
                'type': self.EVENT_TYPE_NEW_TRACK_START,
                'date': self.current_step_time + timedelta(seconds=delay_before_play),
                'radio_uuid': radio_uuid,
                'filename': track_filename,
        }
        if track.is_song:
            event['song_id'] = track.song_id  # add the song id in the event if the track is a song
        if track.is_from_show:
            event['show_id'] = track.show_id
        self.lock.acquire(True)
        self.radio_events.insert(event, safe=True)
        self.lock.release()

        # 3 send message to the streamer
        # 4 store next "prepare track" event
        message = self.play_track(radio_uuid, track, delay_before_play, offset, crossfade_duration)

        # debug duration
        elapsed = datetime.now() - begin
        self.logger.debug('%s prepare_track\n' % elapsed)
        #
        return message  # for test purpose

    def get_current_show(self, radio_id, play_time):
        """
        get current show if exists
        """
        playlists = self.yaapp_alchemy_session.query(Playlist).filter(Playlist.radio_id == radio_id)
        playlist_ids = [x[0] for x in playlists.values(Playlist.id)]
        shows = self.shows.find({'playlist_id': {'$in': playlist_ids}, 'enabled': True}).sort([('time', DESCENDING)])
        current = None
        week_days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
        for s in shows:
            show_days = s['days']
            show_duration = s['duration']
            show_time = s['time']
            show_start_time = datetime.strptime(show_time, '%H:%M:%S.%f').time()
            show_end_time = (datetime.strptime(show_time, '%H:%M:%S.%f') + timedelta(seconds=show_duration)).time()
            # be sure to handle every case, included cases where a show starts a day and ends the day after
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

    def get_next_track(self, radio_uuid, delay_before_play):
        """
        a track can be a jingle, a song in a show or a song in the default playlist
        """
        play_time = self.current_step_time + timedelta(seconds=delay_before_play)

        radios_data = list(self.yaapp_alchemy_session.query(Radio).filter(Radio.uuid == radio_uuid).values(Radio.id))
        if len(radios_data) == 0:
            return None
        radio_id = radios_data[0][0]

        # 1 check if we have to play an inter-song jingle
        track = self.get_radio_jingle_track(radio_uuid)

        if track is None:  # 2 check if we must be playing a show
            # check if one of the radio shows is currently 'on air'
            show_current = self.get_current_show(radio_id, play_time)
            if show_current:
                track = self.get_song_in_show(radio_uuid, show_current['_id'], play_time)

        if track is None:  # 3 choose a song in the default playlist
            track = self.get_song_default(radio_id, play_time)

        return track

    def get_random_song(self, playlist_id, play_time):
        """
        returns Track object
        """
        time_limit = play_time - timedelta(hours=3)
        # SongInstance playlist.id == playlist_id
        # SongInstance enabled == True
        # SongInstance last_play_time is None or < time_limit
        # SongMetadata yasound_song_id > 0
        # order by last_play_time
        query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, or_(SongInstance.last_play_time < time_limit, SongInstance.last_play_time == None), SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
        songs_data = list(query.values(SongInstance.frequency, SongInstance.id))
        count = len(songs_data)

        if count == 0:  # try without time limit
            query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
            songs_data = list(query.values(SongInstance.frequency, SongInstance.id))
            count = len(songs_data)

        if count == 0:
            self.logger.info('no song available for playlist %d' % playlist_id)
            return None

        # use frequency * frequency to have high frequencies very different from low frequencies
        # multiply frequency weight by a date factor to have higher probabilities for songs not played since a long time (date factor = 1 for older song, 0.15 for more recent one)
        first_idx_factor = 1
        last_idx_factor = 0.15
        if (count - 1) == 0:
            date_factor_func = lambda x: 1
        else:
            date_factor_func = lambda x: ((last_idx_factor - first_idx_factor) / (count - 1)) * x + first_idx_factor
        weights = [data[0] * data[0] * date_factor_func(idx) for idx, data in enumerate(songs_data)]
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
        song_id = songs_data[index][1]
        metadata_id = list(self.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.id == song_id).values(SongInstance.metadata_id))[0][0]
        y_id = list(self.yaapp_alchemy_session.query(SongMetadata).filter(SongMetadata.id == metadata_id).values(SongMetadata.yasound_song_id))[0][0]
        yasound_song_data = list(self.yasound_alchemy_session.query(YasoundSong).filter(YasoundSong.id == y_id).values(YasoundSong.filename, YasoundSong.duration))[0]
        filename = yasound_song_data[0]
        duration = yasound_song_data[1]
        track = Track(filename, duration, song_id=song_id)
        return track

    def get_song_default(self, radio_id, play_time):  # use radio id instead of uuid to filter playlists
        playlists_data = list(self.yaapp_alchemy_session.query(Playlist).filter(Playlist.radio_id == radio_id, Playlist.name == 'default').values(Playlist.id))
        if len(playlists_data) == 0:
            return None
        playlist_id = playlists_data[0][0]
        track = self.get_random_song(playlist_id, play_time)
        return track

    def get_song_in_show(self, radio_uuid, show_id, play_time):
        show = self.shows.find_one({'_id': show_id})
        if show is None:
            return None

        playlist_id = show['playlist_id']
        if playlist_id is None:
            return None
        random_play = show['random_play']
        if random_play:
            track = self.get_random_song(playlist_id, play_time)
            track.show_id = show_id
        else:
            radio_state = self.radio_state_manager.radio_state(radio_uuid)
            previous_order = 0
            if radio_state.show_id is not None:
                # song stored in radio_state is a song from the show, so it has a valid order value
                previous_song = self.yaapp_alchemy_session.query(SongInstance).get(radio_state.song_id)
                previous_order = previous_song.order
            song = self.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.playlist_id == playlist.id, SongInstance.order > previous_order).first()
            if song is None:
                return None
            yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
            track = Track(yasound_song.filename, yasound_song.duration, song_id=song.id, show_id=show_id)
        return track

    def get_radio_jingle_track(self, radio_uuid):
        # self.logger.info('get radio jingle track')
        return None  # TODO

    def get_time_jingle_track(self, radio_uuid, time):
        """
        time value is a string like '17:17:13.129150'
        """
        self.logger.info('get time jingle track')
        return None  # TODO

    def receive_test_message(self, data):
        info = data.get('info', None)
        dest_streamer = data.get('streamer', None)

        self.publisher.send_test_message(info, dest_streamer)

    def receive_play_radio_message(self, data):
        radio_uuid = data.get('radio_uuid', None)
        streamer = data.get('streamer', None)
        if radio_uuid is None or streamer is None:
            return
        if self.redis_listener.yaapp_alchemy_session.query(Radio).filter(Radio.uuid == radio_uuid).count() == 0:
            # radio unknown: send 'radio_unknpwn' message to the streamer
            message = self.publisher.send_radio_unknown_message(radio_uuid, streamer)
            return

        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        if radio_state is None or radio_state.master_streamer is None:
            # start radio if it does not exist and play it
            message = self.publisher.send_radio_started_message(radio_uuid, streamer)
            self.play_radio(radio_uuid, streamer)
        else:
            # radio already exists
            master_streamer = radio_state.master_streamer
            message = self.publisher.send_radio_exists_message(radio_uuid, streamer, master_streamer)

        # #FIXME: to check...
        if self.radio_events.find({'radio_uuid': radio_uuid, 'type': self.EVENT_TYPE_NEW_TRACK_PREPARE}).count() == 0:
            self.prepare_track(radio_uuid, 0, 0)
        return message

    def receive_stop_radio_message(self, data):
        radio_uuid = data.get('radio_uuid', None)
        streamer = data.get('streamer', None)
        radio_existed = self.stop_radio(radio_uuid)
        message = None
        if radio_existed:
            message = self.publisher.send_radio_stopped_message(radio_uuid, streamer)
        return message

    def receive_user_authentication_message(self, data):
        """
        receive a message asking for a user user_authentication
        send a authentication message to the streamer in response,
        that message contains a user id and the HD permission.
        if no auth params are provided:
            it's an anonymous client: user id = None
        if auth_token param is provided:
            ask yaapp to verify the token and return the authenticated user
        if username and api_key are provided:
            check in the db that it corresponds to a valid user
            => this method is implemented for backward compatibility !!!
                new streamers must not use this kind of authentication method, use 'auth_token' instead
        """
        streamer = data.get('streamer', None)
        if streamer is None:
            self.logger.debug('user auth: message does not contain streamer info')
            return
        auth_token = data.get('auth_token', None)
        if auth_token is not None:  # auth with token given by yaapp and passed to the streamer by the client
            # ask yaapp if this token is valid and the user associated
            self.logger.debug('user auth: auth_token = %s', auth_token)
            url = settings.YASOUND_SERVER + '/api/v1/check_streamer_auth_token/%s/' % (auth_token)
            r = requests.get(url)
            data = r.json
            self.logger.debug('user auth: check_streamer_auth_token returns => %s' % data)
            user_id = None
            if data is not None:
                user_id = data.get('user_id', None)
            response = {'auth_token': auth_token,
                        'user_id': user_id
            }
            self.logger.debug('user auth: yaapp answered => user_id = %s' % user_id)
        else:
            username = data.get('username', None)
            api_key = data.get('api_key', None)
            if username is not None and api_key is not None:  # auth with username and api_key (for old clients compatibility)
                self.logger.debug('user auth: username = %s, api_key = %s', (username, api_key))
                user = self.redis_listener.yaapp_alchemy_session.query(User).filter(User.username == username).first()
                user_id = None
                if user.api_key.key == api_key:
                    user_id = user.id
                response = {'username': username,
                            'api_key': api_key,
                            'user_id': user_id
                }
                self.logger.debug('user auth: checked in db => user_id = %s', user_id)
            else:  # no auth : anonymous client
                self.logger.debug('user auth: no auth params given => anonymous')
                response = {'user_id': None}

        if response['user_id'] is not None:
            hd_enabled = self.is_hd_enabled(user_id)
        else:
            hd_enabled = False
        response['hd'] = hd_enabled

        response['type'] = self.publisher.MESSAGE_TYPE_USER_AUTHENTICATION
        self.publisher.send_message(response, streamer)
        return response

    def receive_register_streamer_message(self, data):
        streamer = data.get('streamer', None)
        if streamer is None:
            return
        self.register_streamer(streamer)

    def receive_unregister_streamer_message(self, data):
        streamer = data.get('streamer', None)
        if streamer is None:
            return
        self.unregister_streamer(streamer)

    def receive_register_listener_message(self, data):
        radio_uuid = data.get('radio_uuid', None)
        user_id = data.get('user_id', None)
        session_id = data.get('session_id', None)
        if radio_uuid is None or session_id is None:
            self.logger.debug('cannot register listener: radio_uuid or session_id is None')
            return
        return self.register_listener(radio_uuid, user_id, session_id)

    def receive_unregister_listener_message(self, data):
        session_id = data.get('session_id', None)
        if session_id is None:
            return
        return self.unregister_listener(session_id)

    def receive_pong_message(self, data):
        """
        receive a 'pong' message from the streamer in response to a 'ping' message
        the streamer is alive
        """
        streamer_name = data.get('streamer', None)
        if streamer_name is None:
            return
        streamer = self.streamers.find_one({'name': streamer_name})
        if streamer is None:
            # this streamer is not alive (the pong message is maybe late, so the streamer has been considered as dead)
            self.logger.debug('received "pong" message for a streamer (%s) which is not alive' % streamer_name)
            return
        streamer['ping_status'] = self.STREAMER_PING_STATUS_OK
        self.streamers.update({'name': streamer_name}, streamer, safe=True)

    def is_hd_enabled(self, user_id):
        """
        get HD permission for a user
        """
        if user_id is None:
            return False
        user = self.redis_listener.yaapp_alchemy_session.query(User).get(user_id)
        return user.userprofile.hd_enabled

    def start_radio(self, radio_uuid):
        """
        start radio programming
        no need to be connected to a streamer which builds the mp3 stream
        """
        self.clean_radio(radio_uuid)
        # create radio state
        radio_state_doc = {'radio_uuid': radio_uuid}
        radio_state = RadioState(radio_state_doc)
        self.radio_state_manager.insert(radio_state)
        # prepare first track
        self.prepare_track(radio_uuid, 0, 0)  # no delay, no crossfade

    def play_radio(self, radio_uuid, streamer):
        """
        this play function is called when a streamer ask to play a radio
        """
        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        if radio_state != None and radio_state.master_streamer != None:
            return

        already_started = radio_state != None
        # if the radio does not exist, start it
        if not already_started:
            self.logger.debug('play radio %s: need to start radio' % radio_uuid)
            self.start_radio(radio_uuid)  # start_radio function prepares a new track to play
            radio_state = self.radio_state_manager.radio_state(radio_uuid)
            radio_state.master_streamer = streamer
            self.radio_state_manager.update(radio_state)
            self.logger.debug('play radio %s: need to start radio OK' % radio_uuid)
            return

        # store the master streamer ref in the radio state
        radio_state.master_streamer = streamer
        self.radio_state_manager.update(radio_state)
        if radio_state.song_id is None:
            self.logger.debug('play radio %s: no current song id, prepare new track' % radio_uuid)
            self.prepare_track(radio_uuid, 0, 0)
            self.logger.debug('play radio %s: new track prepared' % radio_uuid)
            return

        # the radio already exists, so there is a current programmed track (but not played by the streamer)
        # tell the streamer to play the track at the right offset
        # update radio state with master streamer ref
        self.logger.debug('play radio %s: already exists, need to send prepare track msg' % radio_uuid)
        song_id = int(radio_state.song_id)
        song_play_time = radio_state.play_time
        song = self.yaapp_alchemy_session.query(SongInstance).get(song_id)
        yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
        track = Track(yasound_song.filename, yasound_song.duration, song_id=song_id)
        delay = 0  # FIXME: or self.SONG_PREPARE_DURATION ?
        elapsed_timedelta = self.current_step_time - song_play_time
        offset = delay + elapsed_timedelta.days * (24 * 60 * 60) + elapsed_timedelta.seconds + elapsed_timedelta.microseconds / 1000000.0
        crossfade_duration = 0
        self.publisher.send_prepare_track_message(radio_uuid, track.filename, delay, offset, crossfade_duration, streamer)
        self.logger.debug('play radio %s: prepare track msg sent' % radio_uuid)

    def stop_radio(self, radio_uuid):
        """
        clean radio info
        return True if the radio existed
        """
        exists = self.radio_state_manager.exists(radio_uuid)
        if exists:
            self.radio_has_stopped(radio_uuid)  # send "radio stop" request to yaapp
        # self.clean_radio(radio_uuid)  # FIXME: to check
        # this radio is not managed by a streamer anymore
        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        radio_state.master_streamer = None
        self.radio_state_manager.update(radio_state)
        return exists

    def add_next_hour_event(self):
        t = datetime.now().time()
        h = (t.hour + 1) % 24
        t = t.replace(hour=h, minute=0, second=0, microsecond=0)
        delay_before_play = self.SONG_PREPARE_DURATION
        date = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)  # next hour
        event = {
                    'type': self.EVENT_TYPE_NEW_HOUR_PREPARE,
                    'delay_before_play': delay_before_play,
                    'crossfade_duration': self.CROSSFADE_DURATION,
                    'time': t.isoformat(),
                    'date': date
                }
        self.radio_events.insert(event, safe=True)

    def add_next_check_radios_event(self, seconds):
        date = datetime.now() + timedelta(seconds=seconds)
        event = {
                    'type': self.EVENT_TYPE_CHECK_EXISTING_RADIOS,
                    'date': date
                }
        self.radio_events.insert(event, safe=True)

    def add_next_check_programming_event(self, seconds):
        date = datetime.now() + timedelta(seconds=seconds)
        event = {
                    'type': self.EVENT_TYPE_CHECK_PROGRAMMING,
                    'date': date
                }
        self.radio_events.insert(event, safe=True)

    def add_next_report_songs_event(self):
        date = datetime.now() + timedelta(seconds=self.REPORT_SONGS_STARTED_PERIOD)
        event = {
                    'type': self.EVENT_TYPE_REPORT_SONG_STARTED,
                    'date': date
                }
        self.radio_events.insert(event, safe=True)

    def check_programming(self):
        self.logger.info('### check radios programming: verify that every radio will receive "prepare track" event in the furute')
        scheduler_radio_uuids = self.radio_state_manager.radio_states.find().distinct('radio_uuid')
        for uuid in scheduler_radio_uuids:
            event_count = self.radio_events.find({'type': self.EVENT_TYPE_NEW_TRACK_PREPARE, 'radio_uuid': uuid}).count()
            if event_count == 0:
                self.logger.info('!!!!! radio %s: programming broken' % uuid)
        self.logger.info('### check radios programming: DONE')

    def check_existing_radios(self):
        self.logger.debug('*** check existing radios: add new radios and remove deleted radios')
        scheduler_radio_uuids = set(self.radio_state_manager.radio_states.find().distinct('radio_uuid'))
        radios = self.yaapp_alchemy_session.query(Radio).filter(Radio.ready == True, Radio.origin == 0) # origin == 0 is for radios created in yasound
        db_radio_uuids = set([r.uuid for r in radios])

        # uuids to remove from radio states (radio not ready anymore)
        to_remove = scheduler_radio_uuids.difference(db_radio_uuids)

        for uuid in to_remove:
            self.radio_state_manager.remove(uuid)

        # uuids to add to radio states (new radios)
        to_add = db_radio_uuids.difference(scheduler_radio_uuids)

        for uuid in to_add:
            self.start_radio(uuid)

        self.logger.debug('*** check existing radios: DONE')

    def report_songs_started(self):
        docs = self.songs_started.find()
        reports = []
        for d in docs:
            data = [d['radio_uuid'], d['song_id']]
            reports.append(data)
        if len(reports) == 0:
            return
        # send request...
        url = settings.YASOUND_SERVER + '/api/v1/songs_started/'
        payload = {'key': settings.SCHEDULER_KEY, 'data': reports}
        response = requests.post(url, data=json.dumps(payload))
        result = response.json
        if response.status_code != 200:
            self.logger.info('songs started request failed: status code = %d' % response.status_code)
        elif result['success'] == False:
            self.logger.info('songs started request error: %s' % result['error'])

        # clear songs
        self.songs_started.drop()

    def clean_radio_events(self, radio_uuid):
        self.lock.acquire(True)
        self.radio_events.remove({'radio_uuid': radio_uuid})
        self.lock.release()

    def clean_radio_state(self, radio_uuid):
        """
        return True if the radio existed and has been removed, False if it didn't exist
        """
        return self.radio_state_manager.remove(radio_uuid)

    def clean_radio_listeners(self, radio_uuid):
        self.listeners.remove({'radio_uuid': radio_uuid})

    def clean_radio(self, radio_uuid):
        """
        return True if the radio existed, False if not
        """
        self.clean_radio_events(radio_uuid)
        res = self.clean_radio_state(radio_uuid)
        self.clean_radio_listeners(radio_uuid)
        return res

    def cure_radio_events(self):
        """
        remove all radio events older than now
        """
        self.lock.acquire(True)
        self.radio_events.remove({'date': {'$lt': datetime.now()}})
        self.lock.release()

    def register_streamer(self, streamer_name):
        streamer = {'name': streamer_name,
                    'ping_status': self.STREAMER_PING_STATUS_OK
            }
        self.streamers.insert(streamer)

    def unregister_streamer(self, streamer_name):
        # clean info for radios wich have this streamer as master streamer
        self.logger.debug('unregister_streamer: streamer_name = %s' % (streamer_name))
        radio_uuids = self.radio_state_manager.radio_uuids_for_master_streamer(streamer_name)
        for radio_uuid in radio_uuids:
            self.logger.debug('stopping radio %s' % (radio_uuid))
            self.stop_radio(radio_uuid)
        # remove streamer from streamer list
        self.logger.debug('removing streamer from streamer list')
        self.streamers.remove({'name': streamer_name})

    def register_listener(self, radio_uuid, user_id, session_id):
        """
        register a client,
        the listening session is identified by a unique session_id token given by the streamer
        store the listener status in mongodb (including the listening start date)
        and notify yaapp that a client (connected user or anonymous) started listening to a radio
        """
        # send 'user started listening' request
        url_params = {'key': settings.SCHEDULER_KEY}
        if user_id is not None and user_id != '':
            user_id = int(user_id)
            user = self.redis_listener.yaapp_alchemy_session.query(User).get(user_id)
            if user is not None:
                url_params['username'] = user.username
                url_params['api_key'] = user.api_key.key
            else:
                self.logger.debug('cannot register listener with id %d (does not correspond to a valid user)' % user_id)
        url = settings.YASOUND_SERVER + '/api/v1/radio/%s/start_listening/' % (radio_uuid)
        self.logger.debug('post start_listening request to yaapp')
        requests.post(url, params=url_params)
        self.logger.debug('start_listening request posted')

        # store listener
        listener = {'session_id': session_id,
                    'user_id': user_id,
                    'radio_uuid': radio_uuid,
                    'start_date': datetime.now()
        }
        self.listeners.insert(listener)
        return listener  # for test purpose

    def unregister_listener(self, session_id):
        """
        unregister client,
        use session_id given by the streamer to retrieve the listener status
        compute listening session duration
        and notify yaapp that the client stopped listening and pass the listening duration
        """
        query = {'session_id': session_id}
        listener = self.listeners.find_one(query)
        if listener is None:
            return (None, 0)
        radio_uuid = listener['radio_uuid']
        user_id = listener['user_id']
        start_date = listener['start_date']
        duration_timedelta = datetime.now() - start_date
        seconds = int(duration_timedelta.days * 86400 + duration_timedelta.seconds + duration_timedelta.microseconds / 1000000.0)

        # remove listener
        self.listeners.remove(query)

        # send 'user stopped listening' request
        url_params = {
                        'key': settings.SCHEDULER_KEY,
                        'listening_duration': seconds
        }
        if user_id is not None:
            user = self.redis_listener.yaapp_alchemy_session.query(User).get(user_id)
            url_params['username'] = user.username
            url_params['api_key'] = user.api_key.key
        url = settings.YASOUND_SERVER + '/api/v1/radio/%s/stop_listening/' % (radio_uuid,)
        requests.post(url, params=url_params)

        return (listener, seconds)  # for test purpose

    def radio_has_stopped(self, radio_uuid):
        """
        remove all listeners for this radio
        compute total listening duration for all the clients
        notify yaapp that the radio has stopped
        """
        now = datetime.now()
        total_seconds = 0
        query = {'radio_uuid': radio_uuid}
        listeners = self.listeners.find(query)
        for l in listeners:
            start_date = l['start_date']
            duration_timedelta = now - start_date
            seconds = int(duration_timedelta.days * 86400 + duration_timedelta.seconds + duration_timedelta.microseconds / 1000000.0)
            total_seconds += seconds
        # remove radio listeners
        self.clean_radio_listeners(radio_uuid)

        # send 'radio stopped' request
        url_params = {'listening_duration': total_seconds,
                        'key': settings.SCHEDULER_KEY
        }
        url = settings.YASOUND_SERVER + '/api/v1/radio/%s/stopped/' % (radio_uuid)
        requests.post(url, url_params)

    def ping_streamer(self, streamer_name):
        """
        send ping message via redis to the streamer to check if it's alive
        """
        streamer = {'name': streamer_name,
                    'ping_status': self.STREAMER_PING_STATUS_WAITING
        }
        self.streamers.update({'name': streamer_name}, streamer, safe=True)
        self.publisher.send_ping_message(streamer_name)

    def ping_all_streamers(self):
        streamers = self.streamers.find()
        for streamer in streamers:
            self.ping_streamer(streamer['name'])
