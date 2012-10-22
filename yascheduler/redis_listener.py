from threading import Thread
import settings
import redis
import time
import json
from logger import Logger

class RedisListener(Thread):
    WAIT_TIME = 0.020  # seconds

    TYPE_MESSAGE_TEST = 'test'
    TYPE_MESSAGE_PLAY_RADIO = 'play_radio'
    TYPE_MESSAGE_STOP_RADIO = 'stop_radio'
    TYPE_MESSAGE_USER_AUTHENTICATION = 'user_authentication'
    TYPE_MESSAGE_REGISTER_STREAMER = 'register_streamer'
    TYPE_MESSAGE_UNREGISTER_STREAMER = 'unregister_streamer'
    TYPE_MESSAGE_PONG = 'pong'
    TYPE_MESSAGE_REGISTER_LISTENER = 'register_listener'
    TYPE_MESSAGE_UNREGISTER_LISTENER = 'unregister_listener'

    REDIS_LISTEN_CHANNEL = 'yascheduler'

    def __init__(self, radio_scheduler):
        Thread.__init__(self)
        self.radio_scheduler = radio_scheduler
        self.logger = Logger().log

    def run(self):
        self.logger.debug('Redis listener run...')
        r = redis.StrictRedis(host=settings.REDIS_HOST, db=settings.REDIS_DB)
        r = r.pubsub()
        channel = self.REDIS_LISTEN_CHANNEL
        r.subscribe(channel)
        for message in r.listen():
            if message.get('type') != 'message':
                continue

            data_str = message.get('data')
            data = json.loads(data_str)

            message_type = data.get('type', None)
            self.logger.debug('redis message %s RECEIVED (%s)' % message_type)

            if message_type == self.TYPE_MESSAGE_TEST:
                self.radio_scheduler.receive_test_message(data)
            elif message_type == self.TYPE_MESSAGE_PLAY_RADIO:
                self.radio_scheduler.receive_play_radio_message(data)
            elif message_type == self.TYPE_MESSAGE_STOP_RADIO:
                self.radio_scheduler.receive_stop_radio_message(data)
            elif message_type == self.TYPE_MESSAGE_USER_AUTHENTICATION:
                self.radio_scheduler.receive_user_authentication_message(data)
            elif message_type == self.TYPE_MESSAGE_REGISTER_STREAMER:
                self.radio_scheduler.receive_register_streamer_message(data)
            elif message_type == self.TYPE_MESSAGE_UNREGISTER_STREAMER:
                self.radio_scheduler.receive_unregister_streamer_message(data)
            elif message_type == self.TYPE_MESSAGE_PONG:
                self.radio_scheduler.receive_pong_message(data)
            elif message_type == self.TYPE_MESSAGE_REGISTER_LISTENER:
                self.radio_scheduler.receive_register_listener_message(data)
            elif message_type == self.TYPE_MESSAGE_UNREGISTER_LISTENER:
                self.radio_scheduler.receive_unregister_listener_message(data)

            self.logger.debug('redis message %s HANDLED (%s)' % message_type)
