from os.path import abspath, dirname
import os, sys

PROJECT_PATH = os.path.abspath(os.path.split(__file__)[0])

APP_MODE = os.environ.get('DJANGO_MODE', False)
PRODUCTION_MODE = (APP_MODE == 'production')
DEVELOPMENT_MODE = (APP_MODE == 'development')
LOCAL_MODE = not (PRODUCTION_MODE or DEVELOPMENT_MODE)
USE_MYSQL_IN_LOCAL_MODE = os.environ.get('USE_MYSQL', False)

TEST_MODE = False
for item in sys.argv:
    if 'test' in item:
        TEST_MODE = True
        break

if TEST_MODE:
    LOCAL_MODE = False
    DEVELOPMENT_MODE = False
    PRODUCTION_MODE = False
    USE_MYSQL_IN_LOCAL_MODE = False

HOST = '0.0.0.0'
PORT = 9000
CERT_FILE = '/etc/nginx/ssl/server.crt'
KEY_FILE = '/etc/nginx/ssl/server.key'

REDIS_HOST = 'localhost'
REDIS_DB = 0

if PRODUCTION_MODE:
    REDIS_HOST = 'yas-sql-01'
    REDIS_DB = 3

PROJECT_ROOT = abspath(dirname(__file__))
LOG_DIRECTORY = os.path.join(PROJECT_ROOT, 'logs/')
LOG_FILENAME = os.path.join(LOG_DIRECTORY, 'yascheduler.log')

DATA_DIRECTORY = os.path.join(PROJECT_ROOT, 'data/')
TIME_EVENTS_SAVE_FILENAME = os.path.join(DATA_DIRECTORY, 'time_events.save')

# sql alchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.url import URL

if LOCAL_MODE:
    YASOUND_SERVER = 'http://127.0.0.1:8000'
    if not USE_MYSQL_IN_LOCAL_MODE:
        yaapp_db_path = os.path.join(PROJECT_PATH, 'db.dat')
        yasound_db_path = os.path.join(PROJECT_PATH, 'yasound_db.dat')
        yaapp_alchemy_engine = create_engine('sqlite+pysqlite:////%s' % yaapp_db_path)
        yasound_alchemy_engine = create_engine('sqlite+pysqlite:////%s' % yasound_db_path)
    else:
        yaapp_alchemy_engine = create_engine('mysql+mysqldb://root:root@127.0.0.1:8889/yaapp')
        yasound_alchemy_engine = create_engine('mysql+mysqldb://root:root@127.0.0.1:8889/yasound')
elif DEVELOPMENT_MODE:
    YASOUND_SERVER = 'http://yasound:reagan@yas-dev-01.ig-1.net'
    yaapp_db = URL(drivername='mysql', host='localhost', database='yaapp', query=  { 'read_default_file' : '~/.my.cnf' } )
    yaapp_alchemy_engine = create_engine(name_or_url=yaapp_db)

    yasound_db = URL(drivername='mysql', host='localhost', database='yasound', query=  { 'read_default_file' : '~/.my.cnf.yasound' } )
    yasound_alchemy_engine = create_engine(name_or_url=yasound_db)

elif PRODUCTION_MODE:
    YASOUND_SERVER = 'https://yasound.com'

    yaapp_db = URL(drivername='mysql', host='yas-sql-01', database='yaapp', query=  { 'read_default_file' : '~/.my.cnf' } )
    yaapp_alchemy_engine = create_engine(name_or_url=yaapp_db, pool_recycle=1800, pool_size=10)

    yasound_db = URL(drivername='mysql', host='yas-sql-01', database='yasound', query=  { 'read_default_file' : '~/.my.cnf' } )
    yasound_alchemy_engine = create_engine(name_or_url=yasound_db, pool_recycle=1800, pool_size=10)

elif TEST_MODE:
    YASOUND_SERVER = 'http://127.0.0.1:8000'
    yaapp_db_path = os.path.join(PROJECT_PATH, 'db_test_yascheduler.dat')
    yasound_db_path = os.path.join(PROJECT_PATH, 'yasound_db_test_yascheduler.dat')
    yaapp_alchemy_engine = create_engine('sqlite+pysqlite:////%s' % yaapp_db_path)
    yasound_alchemy_engine = create_engine('sqlite+pysqlite:////%s' % yasound_db_path)

session_factory = sessionmaker(bind=yaapp_alchemy_engine)
yaapp_alchemy_session = scoped_session(session_factory)

session_factory = sessionmaker(bind=yasound_alchemy_engine)
yasound_alchemy_session = scoped_session(session_factory)

# mongodb
from pymongo.connection import Connection
if PRODUCTION_MODE:
    MONGO_DB = Connection('mongodb://yasound:yiNOAi6P8eQC14L@yas-sql-01,yas-sql-02/yasound').yasound
elif DEVELOPMENT_MODE:
    MONGO_DB = Connection('mongodb://yasound:yiNOAi6P8eQC14L@localhost/yasound').yasound
elif TEST_MODE:
    MONGO_DB = Connection().yasound_test
else:
    MONGO_DB = Connection().yasound

import socket
hostname = socket.gethostname()

if PRODUCTION_MODE:
    radio_limit = 4000
    if hostname == 'yas-web-08':
        scheduler_db = MONGO_DB.scheduler.scheduler1
        scheduler_name = 'scheduler1'
        default_radio_offset = 0
        default_radio_limit = radio_limit
    elif hostname == 'yas-web-09':
        scheduler_db = MONGO_DB.scheduler.scheduler2
        scheduler_name = 'scheduler2'
        default_radio_offset = radio_limit
        default_radio_limit = None
elif DEVELOPMENT_MODE:
    radio_limit = 10
    if hostname == 'yas-dev-01':
        scheduler_db = MONGO_DB.scheduler.scheduler1
        scheduler_name = 'scheduler1'
        default_radio_offset = 0
        default_radio_limit = radio_limit
    elif hostname == 'yas-dev-02':
        scheduler_db = MONGO_DB.scheduler.scheduler2
        scheduler_name = 'scheduler2'
        default_radio_offset = radio_limit
        default_radio_limit = None
else:
    scheduler_db = MONGO_DB.scheduler.scheduler1
    scheduler_name = 'scheduler1'
    default_radio_offset = None
    default_radio_limit = None

SCHEDULER_KEY = 'pibs9wn20fnq-1nfk8762ncuwecydgso'
