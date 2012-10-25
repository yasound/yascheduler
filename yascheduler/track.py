class Track:
    def __init__(self, filename, duration, song_id=None, show_id=None):
        self.filename = filename
        self.duration = duration
        self.song_id = song_id
        self.show_id = show_id

    @property
    def is_song(self):
        return self.song_id is not None

    @property
    def is_from_show(self):
        return self.show_id is not None

    def __str__(self):
        if self.is_from_show:
            return '%s - %d seconds ***** show: "%s" song: "%s"' % (self.filename, self.duration, self.show_id, self.song_id)
        elif self.is_song:
            return '%s - %d seconds ***** song: "%s"' % (self.filename, self.duration, self.song_id)
        return '%s - %d seconds' % (self.filename, self.duration)
