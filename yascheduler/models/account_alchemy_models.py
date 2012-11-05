from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, BigInteger, SmallInteger
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref
from datetime import datetime

Base = declarative_base()


class User(Base):
    __tablename__ = 'auth_user'

    id = Column(Integer, primary_key=True)
    username = Column(String(30))
    first_name = Column(String(30))
    last_name = Column(String(30))
    email = Column(String(75))
    password = Column(String(128))
    is_staff = Column(Boolean)
    is_superuser = Column(Boolean)
    is_active = Column(Boolean)
    last_login = Column(DateTime)
    date_joined = Column(DateTime)

    def __init__(self, username):
        self.username = username
        self.first_name = ''
        self.last_name = ''
        self.email = ''
        self.password = ''
        self.is_staff = False
        self.is_superuser = False
        self.is_active = False
        self.last_login = datetime.now()
        self.date_joined = datetime.now()

    def __str__(self):
        return '(%d) %s' % (self.id, self.username)

class UserProfile(Base):
    HD = 2
    __tablename__ = 'account_userprofile'

    id = Column(Integer, primary_key=True)
    name = Column(String(60))
    permissions = Column(BigInteger)
    account_type = Column(String)
    twitter_token = Column(String)
    twitter_token_secret = Column(String)
    twitter_username = Column(String)
    twitter_email = Column(String)
    facebook_token = Column(String)
    facebook_username = Column(String)
    facebook_email = Column(String)
    facebook_expiration_date = Column(String)
    yasound_email = Column(String)
    privacy = Column(SmallInteger)
    gender = Column(String)
    city = Column(String)
    language = Column(String)
    email_confirmed = Column(Boolean)
    notifications_preferences = Column(Integer)
    friends_count = Column(Integer)
    followers_count = Column(Integer)

    user_id = Column(Integer, ForeignKey('auth_user.id'))
    user = relationship('User', uselist=False, backref=backref('userprofile', uselist=False))

    def __init__(self, name, user):
        self.name = name
        self.permissions = 0
        self.user_id = user.id
        self.account_type = ''
        self.twitter_token = ''
        self.twitter_token_secret = ''
        self.twitter_username = ''
        self.twitter_email = ''
        self.facebook_token = ''
        self.facebook_username = ''
        self.facebook_email = ''
        self.facebook_expiration_date = ''
        self.yasound_email = ''
        self.privacy = 0
        self.gender = ''
        self.city = ''
        self.language = ''
        self.email_confirmed = False
        self.notifications_preferences = 0
        self.friends_count = 0
        self.followers_count = 0

    def __str__(self):
        return '(%d) %s' % (self.id, self.name)

    @property
    def hd_enabled(self):
        hd = self.permissions & self.HD
        if hd == 0:
            return False
        return True

    def enable_hd(self, enabled):
        self.permissions |= self.HD


class ApiKey(Base):
    __tablename__ = 'tastypie_apikey'

    id = Column(Integer, primary_key=True)
    key = Column(String(255))
    created = Column(DateTime)

    user_id = Column(Integer, ForeignKey('auth_user.id'))
    user = relationship('User', uselist=False, backref=backref('api_key', uselist=False))

    def __init__(self, user, key):
        self.user_id = user.id
        self.key = key
        self.created = datetime.now()

    def __str__(self):
        return '%s - %s' % (self.user.username, self.key)


