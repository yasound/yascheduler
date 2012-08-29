from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class User(Base):
    __tablename__ = 'auth_user'

    id = Column(Integer, primary_key=True)
    username = Column(String)

    def __init__(self, username):
        self.username = username

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


