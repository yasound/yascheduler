import random
from unittest import TestCase
import settings
from models.yaapp_alchemy_models import Radio, Playlist, SongMetadata, SongInstance
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User, UserProfile, ApiKey
from radio_scheduler import RadioScheduler
import time
from radio_state import RadioStateManager, RadioState
from playlist_manager import PlaylistManager
from radio_history import TransientRadioHistoryManager
from datetime import datetime

def clean_db(yaapp_session, yasound_session):
    yaapp_session.query(Radio).delete()
    yaapp_session.query(Playlist).delete()
    yaapp_session.query(SongInstance).delete()
    yaapp_session.query(SongMetadata).delete()

    yaapp_session.query(User).delete()
    yaapp_session.query(UserProfile).delete()
    yaapp_session.query(ApiKey).delete()

    yaapp_session.commit()

    yasound_session.query(YasoundSong).delete()
    yasound_session.commit()


class Test(TestCase):

    def setUp(self):
        self.scheduler = RadioScheduler()
        self.yaapp_session = self.scheduler.yaapp_alchemy_session
        self.yasound_session = self.scheduler.yasound_alchemy_session
        clean_db(self.yaapp_session, self.yasound_session)

        self.scheduler.flush()

    def test_sqlalchemy_models(self):
        count = self.yasound_session.query(YasoundSong).count()
        self.assertEqual(count, 0)

        count = self.yaapp_session.query(Radio).count()
        self.assertEqual(count, 0)

        # test object creation
        m = SongMetadata('The Beatles', 'Help', 'Ticket to Ride')
        self.yaapp_session.add(m)
        count = self.yaapp_session.query(SongMetadata).count()
        self.assertEqual(count, 1)

        u = User('mat')
        self.yaapp_session.add(u)
        u = self.yaapp_session.query(User).first()
        key = 'keykeykey'
        api_key = ApiKey(u, key)
        self.yaapp_session.add(api_key)

        self.yaapp_session.commit()

        u = self.yaapp_session.query(User).first()
        self.assertEqual(u.api_key.key, key)

        api_key = self.yaapp_session.query(ApiKey).first()
        self.assertEqual(api_key.user_id, u.id)

        self.yaapp_session.commit()

    def test_authentication(self):
        u = User('mat')
        self.yaapp_session.add(u)
        u = self.yaapp_session.query(User).first()

        profile = UserProfile('mat profile', u)
        hd_enabled = True
        profile.enable_hd(hd_enabled)
        self.yaapp_session.add(profile)

        key = 'keykeykey'
        api_key = ApiKey(u, key)
        self.yaapp_session.add(api_key)

        self.yaapp_session.commit()

        username = u.username
        api_key = u.api_key.key
        message_data = {'username': username,
                        'api_key': api_key,
                        'streamer': 'streamer'
        }
        resp = self.scheduler.receive_user_authentication_message(message_data)
        self.assertIsNotNone(resp['user_id'])
        self.assertEqual(resp['user_id'], u.id)
        self.assertEqual(resp['username'], username)
        self.assertEqual(resp['api_key'], api_key)
        self.assertEqual(resp['hd'], hd_enabled)

        resp = self.scheduler.receive_user_authentication_message({'streamer': 'streamer'})
        self.assertIsNotNone(resp)
        self.assertIsNone(resp['user_id'])

        self.yaapp_session.commit()
        self.yasound_session.commit()

    def test_client_register(self):
        u = User('mat')
        self.yaapp_session.add(u)
        u = self.yaapp_session.query(User).first()

        key = 'keykeykey'
        api_key = ApiKey(u, key)
        self.yaapp_session.add(api_key)

        self.yaapp_session.commit()

        listener_count = self.scheduler.listeners.count()
        self.assertEqual(listener_count, 0)

        radio_uuid = 'radio1'
        user_id = u.id
        session_id = 'session1'
        register_data = {'streamer': 'streamer1',
                        'radio_uuid': radio_uuid,
                        'user_id': user_id,
                        'session_id': session_id
        }
        listener = self.scheduler.receive_register_listener_message(register_data)
        self.assertIsNotNone(listener)
        self.assertEqual(listener['radio_uuid'], radio_uuid)
        self.assertEqual(listener['user_id'], user_id)
        self.assertEqual(listener['session_id'], session_id)
        self.assertIsNotNone(listener['start_date'])

        listener_count = self.scheduler.listeners.count()
        self.assertEqual(listener_count, 1)

        wait = 1
        time.sleep(wait)
        unregister_data = {'session_id': session_id}
        listener, duration = self.scheduler.receive_unregister_listener_message(unregister_data)
        self.assertIsNotNone(listener)
        self.assertGreaterEqual(duration, wait)

        listener_count = self.scheduler.listeners.count()
        self.assertEqual(listener_count, 0)

        self.yaapp_session.commit()
        self.yasound_session.commit()


class TestRadioState(TestCase):

    def setUp(self):
        self.manager = RadioStateManager()
        self.manager.drop()

    def test_state(self):
        radio_uuid = '123456'
        doc = {
                'radio_uuid': radio_uuid
        }
        radio_state = RadioState(doc)
        self.assertEqual(radio_state.radio_uuid, radio_uuid)
        self.assertIsNone(radio_state.master_streamer)
        self.assertFalse(radio_state.is_playing)

        master_streamer = 'master'
        radio_state.master_streamer = master_streamer
        self.assertTrue(radio_state.is_playing)

        doc2 = radio_state.as_doc()
        self.assertEqual(doc2['radio_uuid'], radio_uuid)
        self.assertEqual(doc2['master_streamer'], master_streamer)

    def test_manager(self):
        radio_uuid = '123456'
        doc = {
                'radio_uuid': radio_uuid
        }
        radio_state = RadioState(doc)

        self.assertEqual(self.manager.count(radio_uuid), 0)
        self.manager.insert(radio_state)
        self.assertEqual(self.manager.count(radio_uuid), 1)

        s = self.manager.radio_state(radio_uuid)
        self.assertIsNotNone(s)
        self.assertEqual(s.radio_uuid, radio_uuid)
        self.assertIsNotNone(s._id)

        master_streamer = 'streamer'
        song_id = 789
        s.master_streamer = master_streamer
        s.song_id = song_id
        self.manager.update(s)

        s = self.manager.radio_state(radio_uuid)
        self.assertEqual(s.master_streamer, master_streamer)
        self.assertEqual(s.song_id, song_id)

        exists = self.manager.exists(radio_uuid)
        self.assertTrue(exists)

        uuids = self.manager.radio_uuids_for_master_streamer(master_streamer)
        self.assertEqual(1, len(uuids))
        self.assertEqual(uuids[0], radio_uuid)

        self.manager.remove(radio_uuid)
        self.assertEqual(self.manager.count(radio_uuid), 0)


class TestExistingRadiosCheck(TestCase):
    def setUp(self):
        self.scheduler = RadioScheduler()
        self.yaapp_session = self.scheduler.yaapp_alchemy_session
        self.yasound_session = self.scheduler.yasound_alchemy_session
        clean_db(self.yaapp_session, self.yasound_session)
        self.scheduler.flush()


class TestPlaylistManager(TestCase):
    def setUp(self):
        self.manager = PlaylistManager(start_builder_thread=False)
        self.manager.builder.clear_data()

        self.yaapp_session = self.manager.builder.yaapp_alchemy_session
        self.yasound_session = self.manager.builder.yasound_alchemy_session
        clean_db(self.yaapp_session, self.yasound_session)

    def test_check_playlists(self):
        radio_uuid1 = 'uuid1'
        r1 = Radio('mat radio 1', radio_uuid1)
        r1.ready = True
        self.yaapp_session.add(r1)
        r1 = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid1).first()

        radio_uuid2 = 'uuid2'
        r2 = Radio('mat radio 2', radio_uuid2)
        r2.ready = True
        self.yaapp_session.add(r2)
        r2 = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid2).first()

        radio_uuid3 = 'uuid3'
        r3 = Radio('mat radio 3', radio_uuid3)
        r3.ready = False
        self.yaapp_session.add(r3)
        r3 = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid3).first()

        p1_default = Playlist('default', r1)
        self.yaapp_session.add(p1_default)

        p2_default = Playlist('default', r2)
        self.yaapp_session.add(p2_default)

        p3_default = Playlist('default', r3)
        self.yaapp_session.add(p3_default)

        self.assertEqual(self.yaapp_session.query(Radio).count(), 3)
        self.assertEqual(self.yaapp_session.query(Playlist).count(), 3)

        self.assertEqual(self.manager.builder.playlist_count(), 0)
        self.manager.builder.check_playlists()
        self.assertEqual(self.manager.builder.playlist_count(), 2)  # 3 radios, but only 2 ready ones

        self.yaapp_session.commit()

    def test_songs(self):
        radio_uuid = 'uuid'
        r = Radio('my radio', radio_uuid)
        r.ready = True
        self.yaapp_session.add(r)
        r = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid).first()

        name = 'default'
        p = Playlist(name, r)
        self.yaapp_session.add(p)
        p = self.yaapp_session.query(Playlist).filter(Playlist.name == name, Playlist.radio_id == r.id).first()

        self.yaapp_session.commit()

        yasound_song_count = 50
        for i in range(yasound_song_count):
            name = 'song-%d' % i
            artist_name = 'artist-%d' % i
            album_name = 'album-%d' % i
            y = YasoundSong(name=name, artist_name=artist_name, album_name=album_name)
            self.yasound_session.add(y)
            y = self.yasound_session.query(YasoundSong).filter(YasoundSong.name == name, YasoundSong.artist_name == artist_name, YasoundSong.album_name == album_name).first()

            metadata = SongMetadata(name=y.name, artist_name=y.artist_name, album_name=y.album_name, duration=y.duration, yasound_song_id=y.id)
            self.yaapp_session.add(metadata)

        song_count = 50
        for i in range(song_count):
            idx = i % yasound_song_count
            metadata = self.yaapp_session.query(SongMetadata).all()[idx]
            song = SongInstance(song_metadata=metadata)
            song.playlist_id = p.id
            self.yaapp_session.add(song)

        self.assertEqual(self.manager.builder.playlist_count(), 0)
        self.manager.builder.check_playlists()
        self.assertEqual(self.manager.builder.playlist_count(), 1)

        # test song processing
        songs = self.manager.builder.build_random_songs(p.id)
        self.assertIsNotNone(songs)
        self.assertNotEqual(len(songs), 0)
        self.assertEqual(len(songs), self.manager.builder.SONG_COUNT_TO_PREPARE)

        # test doc update
        playlist_doc = self.manager.builder.playlist_collection.find_one({'playlist_id': p.id})
        self.manager.builder.update_songs(playlist_doc)
        playlist_doc = self.manager.builder.playlist_collection.find_one({'playlist_id': p.id})
        self.assertIsNotNone(playlist_doc)
        songs = playlist_doc['songs']
        self.assertIsNotNone(songs)
        self.assertNotEqual(len(songs), 0)
        self.assertEqual(len(songs), self.manager.builder.SONG_COUNT_TO_PREPARE)

        self.yaapp_session.commit()
        self.yasound_session.commit()


class TestRadioHistoryManager(TestCase):

    class EventHandler():
        def __init__(self):
            self.radio_events = []
            self.playlist_events = []

        def handle_radio_event(self, event_type, radio_uuid):
            self.radio_events.append((event_type, radio_uuid))

        def handle_playlist_event(self, event_type, playlist_id):
            self.playlist_events.append((event_type, playlist_id))

    def setUp(self):
        self.event_handler = self.EventHandler()

        self.manager = TransientRadioHistoryManager(self.event_handler.handle_radio_event, self.event_handler.handle_playlist_event)
        self.manager.collection.remove()

    def test_event_handling(self):
        self.assertEqual(0, self.manager.collection.count())

        # create test events
        radio_event_count = 5
        for i in range(radio_event_count):
            now = datetime.now()
            doc = {
                'created': now,
                'updated': now,
                'radio_uuid': 'radio-%d' % i,
                'playlist_id': None,
                'type': TransientRadioHistoryManager.TYPE_RADIO_ADDED
            }
            self.manager.collection.insert(doc)

        playlist_event_count = 5
        for i in range(playlist_event_count):
            now = datetime.now()
            doc = {
                'created': now,
                'updated': now,
                'radio_uuid': 'radio-%d' % i,
                'playlist_id': i,
                'type': TransientRadioHistoryManager.TYPE_PLAYLIST_ADDED
            }
            self.manager.collection.insert(doc)

        self.assertEqual(radio_event_count + playlist_event_count, self.manager.collection.count())

        self.manager.handle_events()
        self.assertEqual(0, self.manager.collection.count())

        self.assertEqual(radio_event_count, len(self.event_handler.radio_events))
        self.assertEqual(playlist_event_count, len(self.event_handler.playlist_events))

    def test_scheduler_integration(self):
        self.scheduler = RadioScheduler()
        self.scheduler.flush()
        self.yaapp_session = self.scheduler.yaapp_alchemy_session
        self.yasound_session = self.scheduler.yasound_alchemy_session

        # add objects
        radio_uuid1 = 'uuid1'
        r1 = Radio('mat radio 1', radio_uuid1)
        r1.ready = True
        self.yaapp_session.add(r1)
        r1 = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid1).first()

        radio_uuid2 = 'uuid2'
        r2 = Radio('mat radio 2', radio_uuid2)
        r2.ready = True
        self.yaapp_session.add(r2)
        r2 = self.yaapp_session.query(Radio).filter(Radio.uuid == radio_uuid2).first()

        p1_default = Playlist('default', r1)
        self.yaapp_session.add(p1_default)
        p1_default = self.yaapp_session.query(Playlist).filter(Playlist.radio_id == r1.id, Playlist.name == 'default').first()

        p2_default = Playlist('default', r2)
        self.yaapp_session.add(p2_default)
        p2_default = self.yaapp_session.query(Playlist).filter(Playlist.radio_id == r2.id, Playlist.name == 'default').first()

        self.yaapp_session.commit()

        # store history events (which should have been stored by yaapp in the real configuration)
        now = datetime.now()
        doc = {
            'created': now,
            'updated': now,
            'radio_uuid': radio_uuid1,
            'playlist_id': None,
            'type': TransientRadioHistoryManager.TYPE_RADIO_ADDED
        }
        self.scheduler.history_manager.collection.insert(doc)

        now = datetime.now()
        doc = {
            'created': now,
            'updated': now,
            'radio_uuid': radio_uuid2,
            'playlist_id': None,
            'type': TransientRadioHistoryManager.TYPE_RADIO_ADDED
        }
        self.scheduler.history_manager.collection.insert(doc)

        now = datetime.now()
        doc = {
            'created': now,
            'updated': now,
            'radio_uuid': radio_uuid1,
            'playlist_id': p1_default.id,
            'type': TransientRadioHistoryManager.TYPE_PLAYLIST_ADDED
        }
        self.scheduler.history_manager.collection.insert(doc)

        now = datetime.now()
        doc = {
            'created': now,
            'updated': now,
            'radio_uuid': radio_uuid2,
            'playlist_id': p2_default.id,
            'type': TransientRadioHistoryManager.TYPE_PLAYLIST_ADDED
        }
        self.scheduler.history_manager.collection.insert(doc)

        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 0)
        self.scheduler.history_manager.handle_events()
        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 2)

        self.yaapp_session.commit()
