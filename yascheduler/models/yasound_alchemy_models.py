from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()


class YasoundSong(Base):
    __tablename__ = 'yasound_song'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    artist_name = Column(String)
    album_name = Column(String)
    filename = Column(String)
    duration = Column(Integer)

    def __init__(self, name, artist_name, album_name, filename='', duration=20):
        self.name = name
        self.artist_name = artist_name
        self.album_name = album_name
        self.filename = filename
        self.duration = duration

    def __str__(self):
        return '(%d) %s - %s - %s' % (self.id, self.artist_name, self.album_name, self.name)
