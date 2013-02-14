import json
import redis
import settings
from logger import Logger


class RedisPublisher():
    MESSAGE_TYPE_TEST = 'test'
    MESSAGE_TYPE_PLAY = 'play'
    MESSAGE_TYPE_RADIO_STARTED = 'radio_started'
    MESSAGE_TYPE_RADIO_STOPPED = 'radio_stopped'
    MESSAGE_TYPE_RADIO_UNKNOWN = 'radio_unknown'
    MESSAGE_TYPE_USER_AUTHENTICATION = 'user_authentication'
    MESSAGE_TYPE_PING = 'ping'

    def __init__(self, channel):
        self.redis_publish_channel = channel
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)
        self.logger = Logger().log

    def send_test_message(self, info, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_TEST,
                    'info': info
        }
        self.send_message(message, dest_streamer)
        return message

    def send_prepare_track_message(self, radio_uuid, track_filename, delay, offset, crossfade_duration, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_PLAY,
                    'radio_uuid': radio_uuid,
                    'filename': track_filename,
                    'delay': delay,
                    'offset': offset,
                    'crossfade_duration': crossfade_duration
        }
        self.send_message(message, dest_streamer)
        return message

    def send_radio_started_message(self, radio_uuid, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_RADIO_STARTED,
                    'radio_uuid': radio_uuid
        }
        self.send_message(message, dest_streamer)
        return message  # for test purpose

    def send_radio_stopped_message(self, radio_uuid, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_RADIO_STOPPED,
                    'radio_uuid': radio_uuid
        }
        self.send_message(message, dest_streamer)
        return message

    def send_radio_unknown_message(self, radio_uuid, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_RADIO_UNKNOWN,
                    'radio_uuid': radio_uuid
        }
        self.send_message(message, dest_streamer)
        return message

    def send_ping_message(self, dest_streamer):
        message = {
                    'type': self.MESSAGE_TYPE_PING
        }
        self.send_message(message, dest_streamer)

    def send_message(self, message, streamer=None):
        m = json.dumps(message)
        channel = self.redis_publish_channel
        if streamer is not None:
            channel += '.%s' % (streamer)
        self.redis.publish(channel, m)
        self.logger.debug('+++ %s +++ sent          dest_streamer = %s  data = %s' % (message.get('type', None), streamer, message))
