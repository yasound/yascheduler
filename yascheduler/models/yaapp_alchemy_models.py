from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Radio(Base):
    __tablename__ = 'yabase_radio'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    uuid = Column(String)
    current_song_id = Column(Integer, ForeignKey('yabase_songinstance.id'))
    current_song = relationship('SongInstance')

    def __str__(self):
        return '(%d) %s - %s' % (self.id, self.name, self.uuid)


class Playlist(Base):
    __tablename__ = 'yabase_playlist'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    enabled = Column(Boolean)

    radio_id = Column(Integer, ForeignKey('yabase_radio.id'))
    radio = relationship('Radio')

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

    metadata_id = Column(Integer, ForeignKey('yabase_songmetadata.id'))
    song_metadata = relationship('SongMetadata')

    playlist_id = Column(Integer, ForeignKey('yabase_playlist.id'))
    playlist = relationship('Playlist')

    def __str__(self):
        return '(%d) %s - %s' % (self.id, self.song_metadata.name, self.playlist.name)
