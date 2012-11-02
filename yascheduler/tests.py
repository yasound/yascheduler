import random
from unittest import TestCase
import settings
from models.yaapp_alchemy_models import Radio, Playlist, SongMetadata, SongInstance
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User, UserProfile, ApiKey
from radio_scheduler import RadioScheduler
import time
from radio_state import RadioStateManager, RadioState
from playlist_manager import PlaylistManager, PlaylistBuilder

def clean_db(yaapp_session, yasound_session):
    yaapp_session.query(Radio).delete()
    yaapp_session.query(Playlist).delete()
    yaapp_session.query(SongInstance).delete()
    yaapp_session.query(SongMetadata).delete()

    yaapp_session.query(User).delete()
    yaapp_session.query(UserProfile).delete()
    yaapp_session.query(ApiKey).delete()

    yasound_session.query(YasoundSong).delete()

class Test(TestCase):

    def setUp(self):
        self.scheduler = RadioScheduler()
        self.yaapp_session = self.scheduler.yaapp_alchemy_session
        self.yasound_session = self.scheduler.yasound_alchemy_session
        clean_db(self.yaapp_session, self.yasound_session)

        self.scheduler.clear_mongo()

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

        u = self.yaapp_session.query(User).first()
        self.assertEqual(u.api_key.key, key)

        api_key = self.yaapp_session.query(ApiKey).first()
        self.assertEqual(api_key.user_id, u.id)

        self.yaapp_session.commit()
        self.yasound_session.commit()

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

        res = self.manager.remove('wrong_uuid')
        self.assertFalse(res)

        res = self.manager.remove(radio_uuid)
        self.assertTrue(res)

        self.assertEqual(self.manager.count(radio_uuid), 0)


class TestExistingRadiosCheck(TestCase):
    def setUp(self):
        self.scheduler = RadioScheduler()
        self.yaapp_session = self.scheduler.yaapp_alchemy_session
        self.yasound_session = self.scheduler.yasound_alchemy_session
        clean_db(self.yaapp_session, self.yasound_session)
        self.scheduler.clear_mongo()

    def test(self):
        r1 = Radio('mat radio 1', 'uuid1')
        r1.ready = True
        self.yaapp_session.add(r1)

        r2 = Radio('mat radio 2', 'uuid2')
        r2.ready = True
        self.yaapp_session.add(r2)

        r3 = Radio('mat radio 3', 'uuid3')
        r3.ready = False
        self.yaapp_session.add(r3)

        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 0)
        self.scheduler.check_existing_radios()
        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 2)

        self.assertEqual(self.yaapp_session.query(Radio).filter(Radio.ready == True).count(), 2)
        self.yaapp_session.query(Radio).filter(Radio.ready == False).update({'ready': True})
        self.assertEqual(self.yaapp_session.query(Radio).filter(Radio.ready == True).count(), 3)

        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 2)
        self.scheduler.check_existing_radios()
        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 3)

        self.yaapp_session.query(Radio).filter(Radio.uuid == 'uuid1').update({'ready': False})

        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 3)
        self.scheduler.check_existing_radios()
        self.assertEqual(self.scheduler.radio_state_manager.radio_states.find().count(), 2)


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

        radio_uuid2 = 'uuid2'
        r2 = Radio('mat radio 2', radio_uuid2)
        r2.ready = True
        self.yaapp_session.add(r2)

        radio_uuid3 = 'uuid3'
        r3 = Radio('mat radio 3', radio_uuid3)
        r3.ready = False
        self.yaapp_session.add(r3)

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
        self.assertEqual(self.manager.builder.playlist_count(), 3)

