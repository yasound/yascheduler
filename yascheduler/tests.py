import random
from unittest import TestCase
import settings
from settings import yaapp_session_maker, yasound_session_maker
from models.yaapp_alchemy_models import Radio, Playlist, SongMetadata, SongInstance
from models.yasound_alchemy_models import YasoundSong
from models.account_alchemy_models import User, UserProfile

def clean_db(yaapp_session, yasound_session):
    yaapp_session.query(Radio).delete()
    yaapp_session.query(Playlist).delete()
    yaapp_session.query(SongInstance).delete()
    yaapp_session.query(SongMetadata).delete()

    yaapp_session.query(User).delete()
    yaapp_session.query(UserProfile).delete()

class TestSqlAlchemy(TestCase):

    def setUp(self):
        self.yaapp_session = yaapp_session_maker()
        self.yasound_session = yasound_session_maker()
        clean_db(self.yaapp_session, self.yasound_session)

    def test_yabase_models(self):
        count = self.yasound_session.query(YasoundSong).count()
        self.assertEqual(count, 0)

        count = self.yaapp_session.query(Radio).count()
        self.assertEqual(count, 0)

        # test object creation
        m = SongMetadata('The Beatles', 'Help', 'Ticket to Ride')
        self.yaapp_session.add(m)
        count = self.yaapp_session.query(SongMetadata).count()
        self.assertEqual(count, 1)

        clean_db(self.yaapp_session, self.yasound_session)

