from threading import Thread
import settings
from logger import Logger
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_
from models.yaapp_alchemy_models import Playlist, SongInstance, SongMetadata, Radio
from models.yasound_alchemy_models import YasoundSong
from datetime import datetime, timedelta
from track import Track
import random
import time


class PlaylistBuilder(Thread):

    SONG_COUNT_TO_PREPARE = 50
    MIN_SONG_COUNT = 3

    def __init__(self):
        Thread.__init__(self)

        self.logger = Logger().log

        self.playlist_collection = settings.MONGO_DB.scheduler.playlists
        self.playlist_collection.ensure_index('playlist_id')
        self.playlist_collection.ensure_index('radio_uuid')
        self.playlist_collection.ensure_index('playlist_is_default')
        self.playlist_collection.ensure_index('update_date')
        self.playlist_collection.ensure_index('song_count')

        # access to yaapp db
        session_factory = sessionmaker(bind=settings.yaapp_alchemy_engine)
        self.yaapp_alchemy_session = scoped_session(session_factory)

        # access to yasound db
        session_factory = sessionmaker(bind=settings.yasound_alchemy_engine)
        self.yasound_alchemy_session = scoped_session(session_factory)

        # access to shows
        self.shows = settings.MONGO_DB.shows

        self.quit = False

    def clear_data(self):
        self.playlist_collection.remove()
        self.playlist_collection.drop()

    def run(self):
        while self.quit == False:
            self.logger.debug('PlaylistBuilder.....')

            # 1 - create entries for new playlists
            # and remove old ones
            self.check_playlists()

            # 2 - compute songs for newly created playlists
            docs = self.playlist_collection.find({'update_date': None})
            for doc in docs:
                self.update_songs(doc)

            # 3 - compute songs for playlists whose song queue contains less than x songs
            docs = self.playlist_collection.find({'song_count': {'$lt': self.MIN_SONG_COUNT}})
            for doc in docs:
                self.update_songs(doc)

            # 4 - sleep
            time.sleep(1)

    def playlist_count(self):
        return self.playlist_collection.count()

    def update_songs(self, playlist_doc):
        playlist_id = playlist_doc['playlist_id']
        show_id = playlist_doc['show_id']
        songs = []
        if show_id == None:
            songs = self.build_random_songs(playlist_id)
        else:
            songs = self.build_show_songs(playlist_id, show_id)

        playlist_doc['songs'] = songs
        playlist_doc['song_count'] = len(songs)
        playlist_doc['update_date'] = datetime.now()
        self.playlist_collection.update({'_id': playlist_doc['_id']}, playlist_doc)

    def build_show_songs(self, playlist_id, show_id):
        show = self.shows.find_one({'_id': show_id})
        if show == None:
            return self.build_random_songs(playlist_id)
        random_play = show['random_play']
        if random_play:
            return self.build_random_songs(playlist_id)

        # follow song order
        current_song_order = self.builder.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id).order_by(SongInstance.last_play_time).first().order
        next_songs = self.builder.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id, SongInstance.order > current_song_order).order_by(SongInstance.order)
        songs = list(next_songs)
        # if there isn't enough songs, get songs with order lower than current_song_order
        if len(songs) < self.SONG_COUNT_TO_PREPARE:
            next_songs = self.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id, SongInstance.order <= current_song_order).order_by(SongInstance.order)
            songs.append(list(next_songs))

        songs_data = []
        for s in songs:
            yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(s.song_metadata.yasound_song_id)
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
        query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, or_(SongInstance.last_play_time < time_limit, SongInstance.last_play_time == None), SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
        songs_data = list(query)
        count = len(songs_data)

        if count == 0:  # try without time limit
            query = self.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
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
            yasound_song = self.yasound_alchemy_session.query(YasoundSong).get(song_data.song_metadata.yasound_song_id)
            song = {
                    'song_id': song_data.id,
                    'filename': yasound_song.filename,
                    'duration': yasound_song.duration
            }
            songs.append(song)
        return songs

    def check_playlists(self):
        """
        be sure there is a document in playlist_collection for every radio default playlist and show playlist,
        add the new ones,
        remove those which no longer exist
        """
        ids = self.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.ready == True, Playlist.name == 'default').values(Playlist.id)
        default_playlists = set([x[0] for x in ids])

        show_playlists = set()
        playlist_to_show = {}
        shows = self.shows.find()
        for s in shows:
            show_id = s['_id']
            playlist_id = s['playlist_id']
            show_playlists.add(playlist_id)
            playlist_to_show[playlist_id] = show_id

        in_db = default_playlists.union(show_playlists)
        in_collection = set(self.playlist_collection.find().distinct('playlist_id'))

        remove_from_collection = in_collection.difference(in_db)
        add_to_collection = in_db.difference(in_collection)

        # remove playlists which should not be in collection anymore
        self.playlist_collection.remove({'playlist_id': {'$in': list(remove_from_collection)}})

        # add new playlists
        for p_id in add_to_collection:
            # the playlist can be from a show OR the radio default playlist
            show_id = playlist_to_show.get(p_id, None)
            is_default = show_id is None

            playlist = self.yaapp_alchemy_session.query(Playlist).get(p_id)
            radio_uuid = playlist.radio.uuid
            doc = {
                    'playlist_id': p_id,
                    'playlist_is_default': is_default,
                    'show_id': show_id,
                    'radio_uuid': radio_uuid,
                    'update_date': None,
                    'songs': [],
                    'song_count': 0
            }
            self.playlist_collection.insert(doc, safe=True)


class PlaylistManager():

    def __init__(self, start_builder_thread=False):
        self.logger = Logger().log

        # start a thread to build songs list for every playlist
        self.builder = PlaylistBuilder()
        if start_builder_thread:
            self.builder.start()

    def start_thread(self):
        self.builder.start()

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
            playlist = self.builder.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.uuid == radio_uuid, Playlist.name == 'default').first()
            if playlist is None:
                return None
            song = self._random_song(playlist.id)

        elif playlist_doc['songs'] is None or playlist_doc['song_count'] == 0 or len(playlist_doc['songs']) == 0:
            self.logger.info('Playlist Manager - track_in_radio: no ready song for radio %s' % radio_uuid)
            playlist_id = playlist_doc['playlist_id']
            if playlist_id == None:
                playlist = self.builder.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.uuid == radio_uuid, Playlist.name == 'default').first()
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
        song = self.builder.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id).order_by(SongInstance.last_play_time).first()
        if song == None:
            return None
        yasound_song = self.builder.yasound_alchemy_session.query(YasoundSong).get(song.song_metadata.yasound_song_id)
        if yasound_song == None:
            return None
        data = {
                'song_id': song.id,
                'filename': yasound_song.filename,
                'duration': yasound_song.duration
        }
        return data