import random
from unittest import TestCase
import settings
from models.yaapp_alchemy_models import Radio, Playlist, SongMetadata, SongInstance
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User, UserProfile, ApiKey
from radio_scheduler import RadioScheduler

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
