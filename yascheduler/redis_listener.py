from threading import Thread
import settings
import redis
import time
import json
from logger import Logger
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import gevent
from gevent import Greenlet

class RedisListener(Greenlet):
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
        Greenlet.__init__(self)
        self.radio_scheduler = radio_scheduler
        self.logger = Logger().log

        session_factory = sessionmaker(bind=settings.yaapp_alchemy_engine)
        self.yaapp_alchemy_session = scoped_session(session_factory)

        session_factory = sessionmaker(bind=settings.yasound_alchemy_engine)
        self.yasound_alchemy_session = scoped_session(session_factory)

    def _run(self):
        self.logger.debug('Redis listener run...')
        try:
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
                self.logger.debug('--- %s --- received        data = %s' % (message_type, data))

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

                self.logger.debug('--- %s --- handled' % message_type)
        except Exception, err:
            self.logger.info('RedisListener exception: %s' % str(err))
