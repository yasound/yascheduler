from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, SmallInteger, Text
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Radio(Base):
    __tablename__ = 'yabase_radio'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    uuid = Column(String)
    current_song_id = Column(Integer, ForeignKey('yabase_songinstance.id'))
    current_song = relationship('SongInstance')
    ready = Column(Boolean)
    deleted = Column(Boolean)
    origin = Column(SmallInteger)
    created = Column(DateTime)
    updated = Column(DateTime)
    description = Column(Text)
    genre = Column(String)
    theme = Column(String)
    anonymous_audience = Column(Integer)
    current_connections = Column(Integer)
    favorites = Column(Integer)
    leaderboard_favorites = Column(Integer)
    computing_next_songs = Column(Boolean)
    new_wall_messages_count = Column(Integer)
    popularity_score = Column(Integer)
    city = Column(String)

    def __init__(self, name, uuid=''):
        self.name = name
        self.uuid = uuid
        self.current_song_id = None
        self.ready = False
        self.origin = 0
        self.created = datetime.now()
        self.updated = datetime.now()
        self.deleted = False
        self.description = ''
        self.genre = 'style_all'
        self.theme = ''
        self.anonymous_audience = 0
        self.current_connections = 0
        self.favorites = 0
        self.leaderboard_favorites = 0
        self.computing_next_songs = False
        self.new_wall_messages_count = 0
        self.popularity_score = 0
        self.city = ''

    def __str__(self):
        return '(%d) %s - %s' % (self.id, self.name, self.uuid)


class Playlist(Base):
    __tablename__ = 'yabase_playlist'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    enabled = Column(Boolean)
    source = Column(String)
    sync_date = Column(DateTime)

    radio_id = Column(Integer, ForeignKey('yabase_radio.id'))
    radio = relationship('Radio')

    def __init__(self, name, radio):
        self.name = name
        self.enabled = True
        self.radio_id = radio.id
        self.source = ''
        self.sync_date = datetime.now()

    def __str__(self):
        return '(%d) %s - %s' % (self.id, self.name, self.radio.name)


class SongMetadata(Base):
    __tablename__ = 'yabase_songmetadata'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    artist_name = Column(String(255))
    album_name = Column(String(255))
    duration = Column(Float)
    yasound_song_id = Column(Integer)

    def __init__(self, name, artist_name, album_name, duration=20, yasound_song_id=0):
        self.name = name
        self.artist_name = artist_name
        self.album_name = album_name
        self.duration = duration
        self.yasound_song_id = yasound_song_id

    def __str__(self):
        return '(%d) %s - %s - %s' % (self.id, self.artist_name, self.album_name, self.name)


class SongInstance(Base):
    __tablename__ = 'yabase_songinstance'

    id = Column(Integer, primary_key=True)
    play_count = Column(Integer)
    last_play_time = Column(DateTime)
    order = Column(Integer)
    frequency = Column(Float)
    enabled = Column(Boolean)
    yasound_score = Column(Float)
    need_sync = Column(Boolean)
    likes = Column(Integer)
    dislikes = Column(Integer)

    metadata_id = Column(Integer, ForeignKey('yabase_songmetadata.id'))
    song_metadata = relationship('SongMetadata')

    playlist_id = Column(Integer, ForeignKey('yabase_playlist.id'))
    playlist = relationship('Playlist')

    def __init__(self, song_metadata):
        self.metadata_id = song_metadata.id
        self.play_count = 0
        self.last_play_time = None
        self.order = None
        self.frequency = 0.5
        self.enabled = True
        self.yasound_score = 0
        self.need_sync = False
        self.likes = 0
        self.dislikes = 0

    def __str__(self):
        return '(%d) %s - %s' % (self.id, self.song_metadata.name, self.playlist.name)
