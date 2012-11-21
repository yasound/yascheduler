from threading import Thread, Event
import settings
from logger import Logger
from datetime import datetime, timedelta
from track import Track
import random
import time
from query_manager import query_yasound_song, query_current_song, query_next_ordered_songs, query_next_ordered_songs_from_order0, query_old_songs, query_songs, query_playlist, query_enabled_playlists, query_radio_default_playlist, query_random_song


class PlaylistBuilder(Thread):

    SONG_COUNT_TO_PREPARE = 50
    MIN_SONG_COUNT = 3
    CHECK_PLAYLIST_PERIOD = 10 * 60

    TYPE_PLAYLIST_ADDED = 1
    TYPE_PLAYLIST_DELETED = 2
    TYPE_PLAYLIST_UPDATED = 3

    def __init__(self):
        Thread.__init__(self)

        self.logger = Logger().log

        self.playlist_collection = settings.MONGO_DB.scheduler.playlists
        self.playlist_collection.ensure_index('playlist_id', unique=True)
        self.playlist_collection.ensure_index('radio_uuid')
        self.playlist_collection.ensure_index('playlist_is_default')
        self.playlist_collection.ensure_index('update_date')
        self.playlist_collection.ensure_index('song_count')

        self.playlist_events = settings.MONGO_DB.scheduler.playlist_events
        self.playlist_collection.ensure_index('playlist_id')

        # access to shows
        self.shows = settings.MONGO_DB.shows

        self.quit = Event()

    def clear_data(self):
        self.playlist_collection.remove()
        self.playlist_collection.drop()

    def join(self, timeout=None):
        self.quit.set()
        super(PlaylistBuilder, self).join(timeout)

    def run(self):
        # if there is no playlist stored, insert all playlists from db
        if self.playlist_collection.find_one() == None:  # empty
            self.set_playlists()

        while not self.quit.is_set():
            self.logger.debug('PlaylistBuilder.....')

            # 1 - add/delete/update playlists
            self.handle_playlist_events()

            # 2 - compute songs for playlists whose song queue contains less than x songs
            # it includes newly created playlists
            docs = self.playlist_collection.find({'song_count': {'$lt': self.MIN_SONG_COUNT}, 'enabled': True})
            for doc in docs:
                self.update_songs(doc)

            # 3 - sleep
            time.sleep(1)

    def playlist_count(self):
        return self.playlist_collection.count()

    def update_songs(self, playlist_doc):
        songs = self.build_songs(playlist_doc)
        song_count = len(songs)
        playlist_doc['songs'] = songs
        playlist_doc['song_count'] = song_count
        playlist_doc['update_date'] = datetime.now()
        if song_count == 0:
            playlist_doc['enabled'] = False  # if we cannot build songs for this playlist, consider it as disabled until it's updated
        self.playlist_collection.update({'_id': playlist_doc['_id']}, playlist_doc)

    def build_songs(self, playlist_doc):
        playlist_id = playlist_doc['playlist_id']
        show_id = playlist_doc['show_id']
        show_random_play = playlist_doc['show_random_play']
        songs = []
        if show_id == None or show_random_play == True:
            songs = self.build_random_songs(playlist_id)
        else:
            songs = self.build_ordered_songs(playlist_id)
        return songs

    def build_ordered_songs(self, playlist_id):
        # follow song order
        current_song_order = query_current_song(playlist_id).order
        next_songs = query_next_ordered_songs(playlist_id, current_song_order)
        songs = list(next_songs)
        # if there isn't enough songs, get songs with order lower than current_song_order
        if len(songs) < self.SONG_COUNT_TO_PREPARE:
            next_songs = query_next_ordered_songs_from_order0(playlist_id, current_song_order)
            songs.append(list(next_songs))

        songs_data = []
        for s in songs:
            yasound_song = query_yasound_song(s.song_metadata.yasound_song_id)
            song_id = s.id
            filename = yasound_song.filename
            duration = yasound_song.duration
            data = {
                    'song_id': song_id,
                    'filename': filename,
                    'duration': duration
            }
            songs_data.append(data)
        return songs_data

    def build_random_songs(self, playlist_id):
        time_limit = datetime.now() - timedelta(hours=3)
        # SongInstance playlist.id == playlist_id
        # SongInstance enabled == True
        # SongInstance last_play_time is None or < time_limit
        # SongMetadata yasound_song_id > 0
        # order by last_play_time
        query = query_old_songs(playlist_id, time_limit)
        songs_data = list(query)
        count = len(songs_data)

        if count == 0:  # try without time limit
            query = query_songs(playlist_id)
            songs_data = list(query)
            count = len(songs_data)

        if count == 0:
            self.logger.info('no song available for playlist %d' % playlist_id)
            return []

        first_idx_factor = 1
        last_idx_factor = 0.15
        if (count - 1) == 0:
            date_factor_func = lambda x: 1
        else:
            date_factor_func = lambda x: ((last_idx_factor - first_idx_factor) / (count - 1)) * x + first_idx_factor
        weighted_songs = []
        total_weight = 0
        for idx, data in enumerate(songs_data):
            frequency = data.frequency
            weight = frequency * frequency * date_factor_func(idx)

            weighted_songs.append((weight, data))
            total_weight += weight

        todo = min(self.SONG_COUNT_TO_PREPARE, count)
        songs = []
        for i in range(todo):
            rnd = random.random() * total_weight
            index = -1
            for i, x in enumerate(weighted_songs):
                weight = x[0]
                rnd -= weight
                if rnd <= 0:
                    index = i
                    break
            if index == -1:
                index = 0
            weighted_song = weighted_songs.pop(index)  # get song data at index and remove element from list
            song_weight = weighted_song[0]
            total_weight -= song_weight

            song_data = weighted_song[1]
            yasound_song = query_yasound_song(song_data.song_metadata.yasound_song_id)
            song = {
                    'song_id': song_data.id,
                    'filename': yasound_song.filename,
                    'duration': yasound_song.duration
            }
            songs.append(song)
        return songs

    def _playlist_added_internal(self, playlist_object):
        # is it a default playlist ?
        if playlist_object.name == 'default':
            doc = {
                    'playlist_id': playlist_object.id,
                    'playlist_is_default': True,
                    'show_id': None,
                    'show_random_play': None,
                    'radio_uuid': playlist_object.radio.uuid,
                    'update_date': None,
                    'songs': [],
                    'song_count': 0,
                    'enabled': True
            }
            self.playlist_collection.update({'playlist_id': doc['playlist_id']}, doc, upsert=True)
            return True

        # is it a show playlist ?
        show = self.shows.find_one({'playlist_id': playlist_object.id})
        if show != None:
            doc = {
                    'playlist_id': playlist_object.id,
                    'playlist_is_default': False,
                    'show_id': show['_id'],
                    'show_random_play': show['random_play'],
                    'radio_uuid': playlist_object.radio.uuid,
                    'update_date': None,
                    'songs': [],
                    'song_count': 0,
                    'enabled': True
            }
            self.playlist_collection.update({'playlist_id': doc['playlist_id']}, doc, upsert=True)
            return True

        return False

    def playlist_added(self, playlist_id):
        doc = {
            'playlist_id': playlist_id,
            'event_type': self.TYPE_PLAYLIST_ADDED
        }
        self.playlist_events.insert(doc, safe=True)

    def playlist_deleted(self, playlist_id):
        doc = {
            'playlist_id': playlist_id,
            'event_type': self.TYPE_PLAYLIST_DELETED
        }
        self.playlist_events.insert(doc, safe=True)

    def playlist_updated(self, playlist_id):
        doc = {
            'playlist_id': playlist_id,
            'event_type': self.TYPE_PLAYLIST_UPDATED
        }
        self.playlist_events.insert(doc, safe=True)

    def handle_playlist_events(self):
        while True:
            event = self.playlist_events.find_and_modify({}, remove=True)
            if event == None:
                break
            playlist_id = event['playlist_id']
            event_type = event['event_type']
            if event_type == self.TYPE_PLAYLIST_ADDED:
                playlist = query_playlist(playlist_id)
                self._playlist_added_internal(playlist)
            elif event_type == self.TYPE_PLAYLIST_DELETED:
                self.playlist_collection.remove({'playlist_id': playlist_id})
            elif event_type == self.TYPE_PLAYLIST_UPDATED:
                # the playlist has been updated, if it was impossible to build songs for it, now it may have changed, so consider it as enabled
                playlist_doc = self.playlist_collection.find_and_modify({'playlist_id': playlist_id}, update={'$set': {'enabled': True, 'song_count': 0, 'songs': []}})
                if playlist_doc is None:
                    self.playlist_added(playlist_id)

    def set_playlists(self):
        playlists = query_enabled_playlists()
        for p in playlists:
            self._playlist_added_internal(p)


class PlaylistManager():

    def __init__(self):
        self.logger = Logger().log

        # start a thread to build songs list for every playlist
        self.builder = PlaylistBuilder()

    def start_thread(self):
        self.builder.start()

    def join_thread(self, timeout=None):
        self.builder.join(timeout)

    def flush(self):
        self.builder.clear_data()

    def track_in_playlist(self, playlist_id):
        playlist_doc = self.builder.playlist_collection.find_one({'playlist_id': playlist_id}, {'songs': {'$slice': 1}})

        if playlist_doc is None:
            self.logger.info('Playlist Manager - track_in_playlist: no prepared playlist %s' % playlist_id)
            song = self._random_song(playlist_id)

        elif playlist_doc['songs'] is None or playlist_doc['song_count'] == 0 or len(playlist_doc['songs']) == 0:
            self.logger.info('Playlist Manager - track_in_playlist: no ready song for playlist %s' % playlist_id)
            song = self._random_song(playlist_id)

        else:
            # get first prepared song, remove it and decrement song_count
            song = playlist_doc['songs'][0]
            self.builder.playlist_collection.update({'playlist_id': playlist_id}, {'$pop': {'songs': -1}, '$inc': {'song_count': -1}})

        if song == None:
            return None

        filename = song['filename']
        duration = song['duration']
        song_id = song['song_id']
        show_id = playlist_doc.get('show_id', None)
        track = Track(filename, duration, song_id, show_id)
        return track

    def track_in_radio(self, radio_uuid):
        # look for 'default' playlist
        playlist_doc = self.builder.playlist_collection.find_one({'radio_uuid': radio_uuid, 'playlist_is_default': True}, {'songs': {'$slice': 1}})

        song = None
        if playlist_doc is None:
            self.logger.info('Playlist Manager - track_in_radio: no prepared playlist for radio %s' % radio_uuid)
            playlist = query_radio_default_playlist(radio_uuid)
            if playlist is None:
                return None
            song = self._random_song(playlist.id)

        elif playlist_doc['songs'] is None or playlist_doc['song_count'] == 0 or len(playlist_doc['songs']) == 0:
            self.logger.info('Playlist Manager - track_in_radio: no ready song for radio %s' % radio_uuid)
            playlist_id = playlist_doc['playlist_id']
            if playlist_id == None:
                playlist = query_radio_default_playlist(radio_uuid)
                if playlist is None:
                    return None
                else:
                    playlist_id = playlist.id
            song = self._random_song(playlist_id)

        else:
            # get first prepared song, remove it and decrement song_count
            song = playlist_doc['songs'][0]
            self.builder.playlist_collection.update({'playlist_id': playlist_doc['playlist_id']}, {'$pop': {'songs': -1}, '$inc': {'song_count': -1}})

        if song == None:
            return None

        filename = song['filename']
        duration = song['duration']
        song_id = song['song_id']
        show_id = None
        track = Track(filename, duration, song_id, show_id)
        return track

    def _random_song(self, playlist_id):
        song = query_random_song(playlist_id)
        if song == None:
            return None
        yasound_song_id = song.song_metadata.yasound_song_id
        if yasound_song_id == None:
            return None
        yasound_song = query_yasound_song(yasound_song_id)
        if yasound_song == None:
            return None
        data = {
                'song_id': song.id,
                'filename': yasound_song.filename,
                'duration': yasound_song.duration
        }
        return data

    def handle_playlist_history_event(self, event_type, radio_uuid, playlist_id):
        from radio_history import TransientRadioHistoryManager
        if event_type == TransientRadioHistoryManager.TYPE_PLAYLIST_ADDED:
            self.builder.playlist_added(playlist_id)
        elif event_type == TransientRadioHistoryManager.TYPE_PLAYLIST_DELETED:
            self.builder.playlist_deleted(playlist_id)
        elif event_type == TransientRadioHistoryManager.TYPE_PLAYLIST_UPDATED:
            self.builder.playlist_updated(playlist_id)
