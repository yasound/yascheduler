import BaseHTTPServer
from logger import Logger
from datetime import datetime
from threading import Thread, Event
import requests
from pymongo import ASCENDING


logger = Logger().log


class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/status':
            self.handle_status()
        elif self.path == '/radios':
            self.handle_radios()
        elif self.path == '/streaming_radios':
            self.handle_streaming_radios()
        elif self.path == '/broken_radios':
            self.handle_broken_radios()
        elif self.path == '/streamers':
            self.handle_streamers()
        elif self.path == '/listeners':
            self.handle_listeners()
        else:
            self.send_response(404)

        self.wfile.close()

    def handle_status(self):
        scheduler = self.server.scheduler

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")

        if scheduler.quit:
            status = 'stopped.'
        else:
            status = 'running...'

        streamer_count = scheduler.streamers.count()
        listener_count = scheduler.listeners.count()

        radio_count = scheduler.radio_state_manager.radio_states.count()
        playing_radio_count = scheduler.radio_state_manager.radio_states.find({'master_streamer': {'$ne': None}}).count()
        broken_radio_count = scheduler.radio_state_manager.radio_states.find({'song_end_time': {'lt': datetime.now()}}).count()

        event_count = scheduler.event_manager.count()

        song_report_count = len(scheduler.current_song_manager.current_songs)

        self.wfile.write("<body>")
        self.wfile.write("<p>Yascheduler %s</p>" % status)

        self.wfile.write("<ul>")

        # streamers
        self.wfile.write("<li>%d streamers</li>" % streamer_count)

        # radios
        self.wfile.write("<li>%d radios</li>" % radio_count)

        self.wfile.write("<ul>")
        self.wfile.write("<li>%d streaming radios</li>" % playing_radio_count)
        self.wfile.write("<li>%d broken radios</li>" % broken_radio_count)
        self.wfile.write("</ul>")

        # listeners
        self.wfile.write("<li>%d listeners</li>" % listener_count)

        # events
        self.wfile.write("<li>%d events in queue</li>" % event_count)

        # song reports
        self.wfile.write("<li>%d songs to report</li>" % song_report_count)

        self.wfile.write("</ul>")

        self.wfile.write("</body>")

    def handle_radios(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")
        self.wfile.write("<body>")

        self.wfile.write("<p>Yascheduler radios:</p>")

        self.wfile.write('<table border="1">')

        self.wfile.write("<tr>")
        self.wfile.write("<th>radio</th>")
        self.wfile.write("<th>streaming</th>")
        self.wfile.write("<th>broken</th>")
        self.wfile.write("<th> master streamer</th>")
        self.wfile.write("<th>song</th>")
        self.wfile.write("<th>song play time</th>")
        self.wfile.write("<th>song end time</th>")
        self.wfile.write("</tr>")

        logger.debug('handle_radios 1')
        radios = self.server.scheduler.radio_state_manager.radio_states.find().sort([('master_streamer', ASCENDING), ('song_end_time', ASCENDING)])
        logger.debug('handle_radios 2')
        for r in radios:
            logger.debug('handle_radios 3 = > %s' % r['radio_uuid'])
            default = '???'
            uuid = r.get('radio_uuid', default)
            logger.debug('handle_radios 3 : %s' % uuid)
            master_streamer = r.get('master_streamer', default)
            logger.debug('handle_radios 3 : %s' % master_streamer)
            song = r.get('song_id', default)
            logger.debug('handle_radios 3 : %s' % song)
            song_time = r.get('play_time', default)
            logger.debug('handle_radios 3 : %s' % song_time)
            song_end_time = r.get('song_end_time', default)
            logger.debug('handle_radios 3 : %s' % song_end_time)
            streaming = master_streamer != None and master_streamer != default
            logger.debug('handle_radios 3 : %s' % streaming)
            broken = song_end_time == default or song_end_time == None or song_end_time < datetime.now()
            logger.debug('handle_radios 3 : %s' % broken)

            logger.debug('handle_radios 3 a')
            self.wfile.write("<tr>")
            logger.debug('handle_radios 3 b')
            self.wfile.write("<td>%s</td>" % uuid)
            logger.debug('handle_radios 3 c')
            self.wfile.write("<td>%s</td>" % streaming)
            logger.debug('handle_radios 3 d')
            self.wfile.write("<td>%s</td>" % broken)
            logger.debug('handle_radios 3 e')
            self.wfile.write("<td>%s</td>" % master_streamer)
            logger.debug('handle_radios 3 f')
            self.wfile.write("<td>%s</td>" % song)
            logger.debug('handle_radios 3 g')
            self.wfile.write("<td>%s</td>" % song_time)
            logger.debug('handle_radios 3 h')
            self.wfile.write("<td>%s</td>" % song_end_time)
            logger.debug('handle_radios 3 i')
            self.wfile.write("</tr>")
        logger.debug('handle_radios 4')

        self.wfile.write("</table>")
        self.wfile.write("</body>")
        logger.debug('handle_radios 5')

    def handle_streaming_radios(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")
        self.wfile.write("<body>")

        self.wfile.write("<p>Yascheduler streaming radios:</p>")

        self.wfile.write('<table border="1">')

        self.wfile.write("<tr>")
        self.wfile.write("<th>radio</th>")
        self.wfile.write("<th>broken</th>")
        self.wfile.write("<th>master streamer</th>")
        self.wfile.write("<th>song</th>")
        self.wfile.write("<th>song play time</th>")
        self.wfile.write("<th>song end time</th>")
        self.wfile.write("</tr>")

        radios = self.server.scheduler.radio_state_manager.radio_states.find({'master_streamer': {'$ne': None}}).sort([('master_streamer', ASCENDING), ('song_end_time', ASCENDING)])
        for r in radios:
            default = '???'
            uuid = r.get('radio_uuid', default)
            master_streamer = r.get('master_streamer', default)
            song = r.get('song_id', default)
            song_time = r.get('play_time', default)
            song_end_time = r.get('song_end_time', default)
            broken = song_end_time == default or song_end_time < datetime.now()

            self.wfile.write("<tr>")
            self.wfile.write("<td>%s</td>" % uuid)
            self.wfile.write("<td>%s</td>" % broken)
            self.wfile.write("<td>%s</td>" % master_streamer)
            self.wfile.write("<td>%s</td>" % song)
            self.wfile.write("<td>%s</td>" % song_time)
            self.wfile.write("<td>%s</td>" % song_end_time)
            self.wfile.write("</tr>")

        self.wfile.write("</table>")
        self.wfile.write("</body>")

    def handle_broken_radios(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")
        self.wfile.write("<body>")

        self.wfile.write("<p>Yascheduler broken radios:</p>")

        self.wfile.write('<table border="1">')

        self.wfile.write("<tr>")
        self.wfile.write("<th>radio</th>")
        self.wfile.write("<th>streaming</th>")
        self.wfile.write("<th>master streamer</th>")
        self.wfile.write("<th>song</th>")
        self.wfile.write("<th>song play time</th>")
        self.wfile.write("<th>song end time</th>")
        self.wfile.write("</tr>")

        radios = self.server.scheduler.radio_state_manager.radio_states.find({'song_end_time': {'$lt': datetime.now()}}).sort([('song_end_time', ASCENDING), ('master_streamer', ASCENDING)])
        for r in radios:
            default = '???'
            uuid = r.get('radio_uuid', default)
            master_streamer = r.get('master_streamer', default)
            song = r.get('song_id', default)
            song_time = r.get('play_time', default)
            song_end_time = r.get('song_end_time', default)
            streaming = master_streamer != None and master_streamer != default

            self.wfile.write("<tr>")
            self.wfile.write("<td>%s</td>" % uuid)
            self.wfile.write("<td>%s</td>" % streaming)
            self.wfile.write("<td>%s</td>" % master_streamer)
            self.wfile.write("<td>%s</td>" % song)
            self.wfile.write("<td>%s</td>" % song_time)
            self.wfile.write("<td>%s</td>" % song_end_time)
            self.wfile.write("</tr>")

        self.wfile.write("</table>")
        self.wfile.write("</body>")

    def handle_streamers(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")
        self.wfile.write("<body>")

        self.wfile.write("<p>Yascheduler streamers:</p>")

        self.wfile.write('<table border="1">')

        self.wfile.write("<tr>")
        self.wfile.write("<th>streamer</th>")
        self.wfile.write("<th>status</th>")
        self.wfile.write("</tr>")

        streamers = self.server.scheduler.streamers.find()
        for s in streamers:
            self.wfile.write("<tr>")
            self.wfile.write("<td>%s</td>" % s['name'])
            self.wfile.write("<td>%s</td>" % s['ping_status'])

        self.wfile.write("</table>")
        self.wfile.write("</body>")

    def handle_listeners(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write("<html><head><title>Yascheduler Monitoring</title></head>")
        self.wfile.write("<body>")

        self.wfile.write("<p>Yascheduler listeners:</p>")

        self.wfile.write('<table border="1">')

        self.wfile.write("<tr>")
        self.wfile.write("<th>radio</th>")
        self.wfile.write("<th>user</th>")
        self.wfile.write("<th>session</th>")
        self.wfile.write("<th>start date</th>")
        self.wfile.write("</tr>")

        listeners = self.server.scheduler.listeners.find().sort([('radio_uuid', ASCENDING), ('start_date', ASCENDING)])
        for l in listeners:
            self.wfile.write("<tr>")
            self.wfile.write("<td>%s</td>" % l['radio_uuid'])
            self.wfile.write("<td>%s</td>" % l['user_id'])
            self.wfile.write("<td>%s</td>" % l['session_id'])
            self.wfile.write("<td>%s</td>" % l['start_date'])

        self.wfile.write("</table>")
        self.wfile.write("</body>")


class MonitoringHttpServer(BaseHTTPServer.HTTPServer):

    def __init__(self, scheduler, port):
        self.scheduler = scheduler
        BaseHTTPServer.HTTPServer.__init__(self, ('0.0.0.0', port), HttpHandler)


class MonitoringManager(Thread):

    def __init__(self, scheduler, port=8001):
        Thread.__init__(self)
        self.quit = Event()
        self.port = port
        self.server = MonitoringHttpServer(scheduler, port)

    def run(self):
        logger.info('start Monitoring htp server thread')
        while not self.quit.is_set():
            self.server.handle_request()
        logger.info('Monitoring htp server thread is over')

    def join(self, timeout=None):
        self.quit.set()
        requests.get('http://0.0.0.0:%s/' % self.port)  # send request to unlock 'handle_request' loop
        super(MonitoringManager, self).join(timeout)
