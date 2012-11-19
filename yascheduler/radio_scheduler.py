import settings
from logger import Logger
from models.yaapp_alchemy_models import Radio, Playlist, SongInstance
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User
from datetime import datetime, timedelta, date
import time
from pymongo import DESCENDING
import requests
from streamer_checker import StreamerChecker
from redis_listener import RedisListener
from track import Track
from redis_publisher import RedisPublisher
from radio_state import RadioStateManager, RadioState
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from playlist_manager import PlaylistManager
from current_song_manager import CurrentSongManager
from time_event_manager import TimeEventManager, TimeEvent
from radio_history import TransientRadioHistoryManager


class RadioScheduler():
    STREAMER_PING_STATUS_OK = 'ok'
    STREAMER_PING_STATUS_WAITING = 'waiting'

    DEFAULT_SECONDS_TO_WAIT = 0.050  # 50 milliseconds

    SONG_PREPARE_DURATION = 5  # seconds
    CROSSFADE_DURATION = 1  # seconds
    CHECK_PROGRAMMING_PERIOD = 30

    def __init__(self, enable_ping_streamers=True, enable_programming_check=False, enable_time_profiling=False):
        self.quit = False

        self.current_step_time = datetime.now()
        self.last_step_time = self.current_step_time

        self.logger = Logger().log

        self.enable_ping_streamers = enable_ping_streamers
        self.enable_programming_check = enable_programming_check
        self.enable_time_profiling = enable_time_profiling

        session_factory = sessionmaker(bind=settings.yaapp_alchemy_engine)
        self.yaapp_alchemy_session = scoped_session(session_factory)

        session_factory = sessionmaker(bind=settings.yasound_alchemy_engine)
        self.yasound_alchemy_session = scoped_session(session_factory)

        self.mongo_scheduler = settings.MONGO_DB.scheduler

        self.streamers = self.mongo_scheduler.streamers
        self.streamers.ensure_index('name', unique=True)

        self.listeners = self.mongo_scheduler.listeners
        self.listeners.ensure_index('radio_uuid')
        self.listeners.ensure_index('session_id', unique=True)

        self.shows = settings.MONGO_DB.shows

        # tools
        self.publisher = RedisPublisher('yastream')
        self.redis_listener = RedisListener(self)
        self.radio_state_manager = RadioStateManager()
        self.streamer_checker = StreamerChecker(self)
        self.playlist_manager = PlaylistManager()
        self.current_song_manager = CurrentSongManager()
        self.event_manager = TimeEventManager()
        self.history_manager = TransientRadioHistoryManager([self.handle_radio_history_event], [self.handle_playlist_history_event, self.playlist_manager.handle_playlist_history_event])

    def clear_mongo(self):
        self.event_manager.clear()
        self.radio_state_manager.drop()

        self.streamers.drop()
        self.listeners.drop()

    def flush(self):
        self.logger.debug('flush...')
        self.clear_mongo()
        self.playlist_manager.flush()
        self.current_song_manager.flush()
        self.logger.debug('flushed')

    def stop(self):
        self.quit = True

    def run(self):
        self.last_step_time = datetime.now()

        # starts thread to listen to redis events
        self.redis_listener.start()

        self.playlist_manager.start_thread()
        self.current_song_manager.start()

        # starts streamer checker thread
        if self.enable_ping_streamers:
            self.streamer_checker.start()

        self.history_manager.start()
        self.event_manager.start_saver()

        if self.radio_state_manager.radio_states.find_one() == None:  # no radios
            self.set_radios()

        # prepare track for radios with no event in the future (events which should have occured when the scheduler was off and which have been cured)
        self.logger.info('preparing tracks')
        self.logger.info('radio_events.count() = %d' % (self.event_manager.count()))
        uuids = self.event_manager.scheduled_radios()
        for radio_state_doc in self.radio_state_manager.radio_states.find(fields={'radio_uuid': True}):
            radio_uuid = radio_state_doc.get('radio_uuid', None)
            if radio_uuid is None:
                continue

            # if there are events for this radio, next track will be computed later
            # else, prepare a new track now
            if radio_uuid not in uuids:
                self.logger.info('prepare track for broken radio %s' % radio_uuid)
                delay_before_play = 0
                crossfade_duration = 0
                self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)
        self.logger.info('preparing tracks: DONE')

        # add the event for the next hour if it does not exist yet
        if self.event_manager.contains_event_type(TimeEvent.EVENT_TYPE_NEW_HOUR_PREPARE) == False:
            self.add_next_hour_event()

        # add the event to check if there is no problem with radios' programming
        if self.enable_programming_check:
            self.add_next_check_programming_event(self.CHECK_PROGRAMMING_PERIOD)

        while not self.quit:
            if self.enable_time_profiling:
                time_profile_begin = datetime.now()

            self.current_step_time = datetime.now()

            # get events to handle
            events = self.event_manager.pop_past_events(self.current_step_time)
            self.logger.info('...........................................')

            event_count = 0
            for e in events:
                # handle event
                self.handle_event(e)
                event_count += 1

            # compute seconds to wait until next event
            seconds_to_wait = self.event_manager.time_till_next(self.current_step_time)
            if seconds_to_wait == None:
                seconds_to_wait = self.DEFAULT_SECONDS_TO_WAIT

            if self.enable_time_profiling:
                    self.logger.debug('....... %d events handled' % event_count)
                    if seconds_to_wait == 0:
                        self.logger.debug('....... need to process next events NOW !!!')
                    else:
                        self.logger.debug('....... wait for %s seconds' % seconds_to_wait)

            # store next step date
            self.last_step_time = self.current_step_time

            if self.enable_time_profiling:
                elapsed = datetime.now() - time_profile_begin
                elapsed_sec = elapsed.seconds + elapsed.microseconds / 1000000.0
                percent = elapsed_sec / (elapsed_sec + seconds_to_wait) * 100
                self.logger.info('main loop: process = %s seconds / wait = %s seconds (%s%%)' % (elapsed_sec, seconds_to_wait, percent))

            # waits until next event
            time.sleep(seconds_to_wait)

        self.logger.info('radio scheduler main loop is over')
        self.history_manager.join()
        self.streamer_checker.join()
        self.redis_listener.join()
        self.playlist_manager.join_thread()
        self.current_song_manager.join()
        self.event_manager.join_saver()

        self.logger.info('radio scheduler "run" is over')

    def handle_event(self, event):
        event_type = event.event_type
        if event_type == TimeEvent.EVENT_TYPE_NEW_HOUR_PREPARE:
            self.handle_new_hour_prepare(event)
        elif event_type == TimeEvent.EVENT_TYPE_NEW_TRACK_START:
            self.handle_new_track_start(event)
        elif event_type == TimeEvent.EVENT_TYPE_NEW_TRACK_PREPARE:
            self.handle_new_track_prepare(event)
        elif event_type == TimeEvent.EVENT_TYPE_TRACK_CONTINUE:
            self.handle_track_continue(event)
        elif event_type == TimeEventManager.EVENT_TYPE_CHECK_PROGRAMMING:
            self.handle_check_programming(event)
        else:
            self.logger.info('event "%s" can not be handled: unknown type' % event)

    def handle_check_programming(self, event):
        self.check_programming()
        # add next check event
        self.add_next_check_programming_event(self.CHECK_PROGRAMMING_PERIOD)

    def handle_new_hour_prepare(self, event):
        self.logger.info('prepare new hour %s' % datetime.now().time().isoformat())
        # delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        # crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        # time = event.get('time', None)  # time value is a string like '17:17:13.129150'
        delay_before_play = event.delay_before_play
        crossfade_duration = event.crossfade_duration
        time = event.time
        if time is None:
            self.logger.info('time event does not contain a valid "time" param')
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

                # remove next "prepare track" event since the current song is paused
                self.event_manager.remove_radio_events(uuid)

                # store event to restart playing current song after jingle has finished
                # event = {
                #             'type': TimeEventManager.EVENT_TYPE_TRACK_CONTINUE,
                #             'date': self.current_step_time + timedelta(seconds=(delay_before_play + jingle_track.duration - self.CROSSFADE_DURATION - self.SONG_PREPARE_DURATION)),
                #             'radio_uuid': uuid,
                #             'song_id': current_song_id,
                #             'offset': current_song_played,
                #             'delay_before_play': self.SONG_PREPARE_DURATION,
                #             'crossfade_duration': self.CROSSFADE_DURATION
                # }
                d = self.current_step_time + timedelta(seconds=(delay_before_play + jingle_track.duration - self.CROSSFADE_DURATION - self.SONG_PREPARE_DURATION))
                event = TimeEvent(TimeEvent.EVENT_TYPE_TRACK_CONTINUE, d)
                event.radio_uuid = uuid
                event.song_id = current_song_id
                event.offset = current_song_played
                event.delay_before_play = self.SONG_PREPARE_DURATION
                event.crossfade_duration = self.CROSSFADE_DURATION
                self.event_manager.insert(event)

        # add next hour event
        self.add_next_hour_event()

    def handle_track_continue(self, event):
        """
        restart a track after it has been paused (for example, because of a time jingle)
        """
        self.logger.info('track continue %s' % datetime.now().time().isoformat())
        # radio_uuid = event.get('radio_uuid', None)
        # song_id = event.get('song_id', None)
        # offset = event.get('offset', None)
        # delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        # crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        radio_uuid = event.radio_uuid
        song_id = event.song_id
        offset = event.get('offset', None)
        delay_before_play = event.delay_before_play
        crossfade_duration = event.crossfade_duration
        if radio_uuid is None:
            self.logger.info('"track continue" event does not contain a valid radio uuid')
            return
        if song_id is None or offset is None:
            # event does not contain a valid song id and play offset =>  start a new song !
            self.logger.info('"track continue" event does not contain a valid song id and play offset: start a new song !')
            self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)
            return

        song = self.yaapp_alchemy_session.query(SongInstance).get(song_id)
        yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
        track = Track(yasound_song.filename, yasound_song.duration, song_id=song_id)
        self.play_track(radio_uuid, track, delay_before_play, offset, crossfade_duration)

    def handle_new_track_start(self, event):
        # self.logger.info('track start %s' % datetime.now().time().isoformat())
        # radio_uuid = event.get('radio_uuid', None)
        # if not radio_uuid:
        #     self.logger.debug('handle_new_track_start ERROR: radio uuid is none in event')
        #     return
        # song_id = event.get('song_id', None)
        # show_id = event.get('show_id', None)
        # song_duration = event.get('track_duration', 0)
        radio_uuid = event.radio_uuid
        if not radio_uuid:
            self.logger.debug('handle_new_track_start ERROR: radio uuid is none in event')
            return
        song_id = event.song_id
        show_id = event.show_id if hasattr(event, 'show_id') else None
        song_duration = event.track_duration

        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        radio_state.song_id = song_id
        radio_state.play_time = self.current_step_time
        radio_state.song_end_time = radio_state.play_time + timedelta(seconds=song_duration)
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
            # store song played in order to notify yaapp
            self.current_song_manager.store(radio_uuid, song_id)

    def handle_new_track_prepare(self, event):
        """
        new 'track prepare' event has been received
        we have a delay before a new track must be played in order to prepare this track
        """
        # self.logger.info('prepare track %s' % datetime.now().time().isoformat())
        # radio_uuid = event.get('radio_uuid', None)
        radio_uuid = event.radio_uuid
        if not radio_uuid:
            return
        # delay_before_play = event.get('delay_before_play', self.SONG_PREPARE_DURATION)
        # crossfade_duration = event.get('crossfade_duration', self.CROSSFADE_DURATION)
        delay_before_play = event.delay_before_play
        crossfade_duration = event.crossfade_duration
        self.prepare_track(radio_uuid, delay_before_play, crossfade_duration)

    def play_track(self, radio_uuid, track, delay, offset, crossfade_duration):
        """
        1 - if the radio is currently listened (ie if a streamer builds its mp3 stream), ask the streamer to play this track
        2 - add a future event to prepare the next track
        """
        radio_state = self.radio_state_manager.radio_state(radio_uuid)
        if radio_state is not None and radio_state.master_streamer is not None:  # dest_streamer is the radio's master streamer
            message = self.publisher.send_prepare_track_message(radio_uuid, track.filename, delay, offset, crossfade_duration, radio_state.master_streamer)
        else:
            message = None

        # 2 - store next 'track prepare' event
        next_delay_before_play = self.SONG_PREPARE_DURATION
        next_crossfade_duration = self.CROSSFADE_DURATION
        next_date = self.current_step_time + timedelta(seconds=(delay + track.duration - offset - next_crossfade_duration - next_delay_before_play))
        # event = {
        #         'type': TimeEventManager.EVENT_TYPE_NEW_TRACK_PREPARE,
        #         'date': next_date,
        #         'radio_uuid': radio_uuid,
        #         'delay_before_play': next_delay_before_play,
        #         'crossfade_duration': next_crossfade_duration
        # }
        event = TimeEvent(TimeEvent.EVENT_TYPE_NEW_TRACK_PREPARE, next_date)
        event.radio_uuid = radio_uuid
        event.delay_before_play = next_delay_before_play
        event.crossfade_duration = next_crossfade_duration
        self.event_manager.insert(event)

        return message

    def prepare_track(self, radio_uuid, delay_before_play, crossfade_duration):
        """
        1 - get next track
        2 - create 'track start' event for this new track
        3 - send it to the streamer
        4 - create next 'track prepare' event
        """
        # 1 get next track
        track = self.get_next_track(radio_uuid, delay_before_play)

        if track is None:
            #FIXME: it crashes
            # self.logger.debug('prepare_track ERROR: cannot get next track => remove radio %s' % radio_uuid)
            # self.remove_radio(radio_uuid)
            return None
        track_filename = track.filename
        track_duration = track.duration
        offset = 0

        # 2 store 'track start' event
        # event = {
        #         'type': TimeEventManager.EVENT_TYPE_NEW_TRACK_START,
        #         'date': self.current_step_time + timedelta(seconds=delay_before_play),
        #         'radio_uuid': radio_uuid,
        #         'filename': track_filename,
        #         'track_duration': track_duration
        # }
        # if track.is_song:
        #     event['song_id'] = track.song_id  # add the song id in the event if the track is a song
        # if track.is_from_show:
        #     event['show_id'] = track.show_id
        event = TimeEvent(TimeEvent.EVENT_TYPE_NEW_TRACK_START, self.current_step_time + timedelta(seconds=delay_before_play))
        event.radio_uuid = radio_uuid
        event.filename = track_filename
        event.track_duration = track_duration
        if track.is_song:
            event.song_id = track.song_id  # add the song id in the event if the track is a song
        if track.is_from_show:
            event.song_id = track.show_id

        self.event_manager.insert(event)

        # 3 send message to the streamer
        # 4 store next "prepare track" event
        message = self.play_track(radio_uuid, track, delay_before_play, offset, crossfade_duration)
        return message  # for test purpose

    def get_current_show(self, radio_uuid, play_time):
        """
        get current show if exists
        """
        playlists = self.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.uuid == radio_uuid)
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

        track = self.get_radio_jingle_track(radio_uuid)

        if track is None:  # 2 check if we must be playing a show
            # check if one of the radio shows is currently 'on air'
            show_current = self.get_current_show(radio_uuid, play_time)
            if show_current:
                # track = self.get_song_in_show(radio_uuid, show_current['_id'], play_time)
                track = self.playlist_manager.track_in_playlist(show_current['playlist_id'])

        if track is None:  # 3 choose a song in the default playlist
            # track = self.get_song_default(radio_id, play_time)
            track = self.playlist_manager.track_in_radio(radio_uuid)

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

        # if radio's programming is broken
        # ie current song has been played and no next song has been programmed
        if radio_state.song_end_time != None and radio_state.song_end_time < datetime.now():
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
                self.logger.debug('user auth: username = %s, api_key = %s' % (username, api_key))
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
        self.unregister_streamer(streamer)
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

    def remove_radio(self, radio_uuid):
        self.radio_state_manager.remove(radio_uuid)
        self.clean_radio_events(radio_uuid)

    def set_radios(self):
        radios = self.yaapp_alchemy_session.query(Radio).all()
        for r in radios:
            self.start_radio(r.uuid)

    def add_next_hour_event(self):
        t = datetime.now().time()
        h = (t.hour + 1) % 24
        t = t.replace(hour=h, minute=0, second=0, microsecond=0)
        delay_before_play = self.SONG_PREPARE_DURATION
        d = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)  # next hour
        # event = {
        #             'type': TimeEventManager.EVENT_TYPE_NEW_HOUR_PREPARE,
        #             'delay_before_play': delay_before_play,
        #             'crossfade_duration': self.CROSSFADE_DURATION,
        #             'time': t.isoformat(),
        #             'date': d
        #         }
        event = TimeEvent(TimeEvent.EVENT_TYPE_NEW_HOUR_PREPARE, d)
        event.delay_before_play = delay_before_play
        event.crossfade_duration = self.CROSSFADE_DURATION
        event.time = t.isoformat()
        self.event_manager.insert(event)

    def add_next_check_programming_event(self, seconds):
        d = datetime.now() + timedelta(seconds=seconds)
        # event = {
        #             'type': TimeEventManager.EVENT_TYPE_CHECK_PROGRAMMING,
        #             'date': d
        #         }
        event = TimeEvent(TimeEvent.EVENT_TYPE_CHECK_PROGRAMMING, d)
        self.event_manager.insert(event)

    def check_programming(self):
        self.logger.info('### check radios programming: verify that current song end time is not over for every radio')
        for doc in self.radio_state_manager.broken_radios():
            radio_uuid = doc['radio_uuid']
            self.logger.info('!!!!! radio %s: programming broken' % radio_uuid)
        self.logger.info('### check radios programming: DONE')

    def handle_radio_history_event(self, event_type, radio_uuid):
        if event_type == TransientRadioHistoryManager.TYPE_RADIO_ADDED:
            self.logger.info('radio %s created' % radio_uuid)
            self.start_radio(radio_uuid)
        elif event_type == TransientRadioHistoryManager.TYPE_RADIO_DELETED:
            self.logger.info('radio %s deleted' % radio_uuid)
            self.remove_radio(radio_uuid)

    def handle_playlist_history_event(self, event_type, radio_uuid, playlist_id):
        if event_type == TransientRadioHistoryManager.TYPE_PLAYLIST_ADDED or event_type == TransientRadioHistoryManager.TYPE_PLAYLIST_UPDATED:
            if self.radio_state_manager.exists(radio_uuid) == False:
                self.start_radio(radio_uuid)

    def clean_radio_events(self, radio_uuid):
        self.event_manager.remove_radio_events(radio_uuid)

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
#
