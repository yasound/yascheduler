from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, BigInteger
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

    user_id = Column(Integer, ForeignKey('auth_user.id'))
    user = relationship('User', uselist=False, backref=backref('userprofile', uselist=False))

    def __init__(self, name, user):
        self.name = name
        self.permissions = 0
        self.user_id = user.id

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


