import settings
from models.yaapp_alchemy_models import SongInstance, Playlist, Radio, SongMetadata
from models.account_alchemy_models import User
from models.yasound_alchemy_models import YasoundSong
from sqlalchemy import or_
from logger import Logger

logger = Logger().log

QUERY_TYPE_RADIO_EXISTS = 1
QUERY_TYPE_READY_RADIOS = 2
QUERY_TYPE_ENABLED_PLAYLISTS = 3
QUERY_TYPE_RADIO_PLAYLISTS = 4
QUERY_TYPE_RADIO_DEFAULT_PLAYLIST = 5
QUERY_TYPE_PLAYLIST = 6
QUERY_TYPE_SONG = 7
QUERY_TYPE_CURRENT_SONG = 8
QUERY_TYPE_NEXT_SONGS = 9
QUERY_TYPE_NEXT_SONGS_PART2 = 10
QUERY_TYPE_OLD_SONGS = 11
QUERY_TYPE_SONGS = 12
QUERY_TYPE_RANDOM_SONG = 13
QUERY_TYPE_YASOUND_SONG = 14
QUERY_TYPE_USER_BY_USERNAME = 15
QUERY_TYPE_USER_BY_ID = 16


def yaquery(query_type, *args):
    try:
        res = yaquery_internal(query_type, args)
    except Exception, err:
        logger.debug('query exception: %s' % err)
        settings.yaapp_alchemy_session.remove()
        settings.yasound_alchemy_session.remove()
        res = yaquery_internal(query_type, args)
        logger.debug('same query with a new session: OK')
    return res


def yaquery_internal(query_type, args):
    if query_type == QUERY_TYPE_RADIO_EXISTS:
        return query_radio_exists(args[0])
    elif query_type == QUERY_TYPE_READY_RADIOS:
        return query_ready_radios()
    elif query_type == QUERY_TYPE_ENABLED_PLAYLISTS:
        return query_enabled_playlists()
    elif query_type == QUERY_TYPE_RADIO_PLAYLISTS:
        return query_radio_playlists(args[0])
    elif query_type == QUERY_TYPE_RADIO_DEFAULT_PLAYLIST:
        return query_radio_default_playlist(args[0])
    elif query_type == QUERY_TYPE_PLAYLIST:
        return query_playlist(args[0])
    elif query_type == QUERY_TYPE_SONG:
        return query_song(args[0])
    elif query_type == QUERY_TYPE_CURRENT_SONG:
        return query_current_song(args[0])
    elif query_type == QUERY_TYPE_NEXT_SONGS:
        return query_next_ordered_songs(args[0], args[1])
    elif query_type == QUERY_TYPE_NEXT_SONGS_PART2:
        return query_next_ordered_songs_from_order0(args[0], args[1])
    elif query_type == QUERY_TYPE_OLD_SONGS:
        return query_old_songs(args[0], args[1])
    elif query_type == QUERY_TYPE_SONGS:
        return query_songs(args[0])
    elif query_type == QUERY_TYPE_RANDOM_SONG:
        return query_random_song(args[0])
    elif query_type == QUERY_TYPE_YASOUND_SONG:
        return query_yasound_song(args[0])
    elif query_type == QUERY_TYPE_USER_BY_USERNAME:
        return query_user_by_username(args[0])
    elif query_type == QUERY_TYPE_USER_BY_ID:
        return query_user_by_id(args[0])

    return None


#################################
# Radio
#


def query_radio_exists(radio_uuid):
    exists = settings.yaapp_alchemy_session.query(Radio).filter(Radio.uuid == radio_uuid).count() == 0
    return exists


def query_ready_radios():
    radios = settings.yaapp_alchemy_session.query(Radio).filter(Radio.ready == True, Radio.origin == 0, Radio.deleted == False).all()
    return radios


#################################
# Playlist
#


def query_enabled_playlists():
    playlists = settings.yaapp_alchemy_session.query(Playlist).filter(Playlist.enabled == True, Playlist.radio != None).all()
    return playlists


def query_radio_playlists(radio_uuid):
    playlists = settings.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.uuid == radio_uuid)
    return playlists


def query_radio_default_playlist(radio_uuid):
    playlist = settings.yaapp_alchemy_session.query(Playlist).join(Radio).filter(Radio.uuid == radio_uuid, Playlist.name == 'default').first()
    return playlist


def query_playlist(playlist_id):
    playlist = settings.yaapp_alchemy_session.query(Playlist).get(playlist_id)
    return playlist


#################################
# Song
#

def query_song(song_id):
    song = settings.yaapp_alchemy_session.query(SongInstance).get(song_id)
    return song


def query_current_song(playlist_id):
    song = settings.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id).order_by(SongInstance.last_play_time).first()
    return song


def query_next_ordered_songs(playlist_id, current_song_order):
    songs = settings.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id, SongInstance.order > current_song_order).order_by(SongInstance.order)
    return songs


def query_next_ordered_songs_from_order0(playlist_id, current_song_order):
    songs = settings.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id, SongInstance.order <= current_song_order).order_by(SongInstance.order)
    return songs


def query_old_songs(playlist_id, time_limit):
    songs = settings.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, or_(SongInstance.last_play_time < time_limit, SongInstance.last_play_time == None), SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
    return songs


def query_songs(playlist_id):
    songs = settings.yaapp_alchemy_session.query(SongInstance).join(SongMetadata).filter(SongInstance.playlist_id == playlist_id, SongInstance.enabled == True, SongMetadata.yasound_song_id > 0).order_by(SongInstance.last_play_time)
    return songs


def query_random_song(playlist_id):
    song = settings.yaapp_alchemy_session.query(SongInstance).filter(SongInstance.enabled == True, SongInstance.playlist_id == playlist_id).order_by(SongInstance.last_play_time).first()
    return song


#################################
# Yasound song
#


def query_yasound_song(yasound_song_id):
    yasound_song = settings.yasound_alchemy_session.query(YasoundSong).get(yasound_song_id)
    return yasound_song


#################################
# User
#


def query_user_by_username(username):
    user = settings.yaapp_alchemy_session.query(User).filter(User.username == username).first()
    return user


def query_user_by_id(user_id):
    user = settings.yaapp_alchemy_session.query(User).get(user_id)
    return user
