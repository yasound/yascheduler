class Track:
    def __init__(self, filename, duration, song=None, show=None):
        self.filename = filename
        self.duration = duration
        self.song = song
        self.show = show

    @property
    def is_song(self):
        return self.song is not None

    @property
    def is_from_show(self):
        return self.show is not None

    def __str__(self):
        if self.is_from_show:
            return '%s - %d seconds ***** show: "%s" song: "%s"' % (self.filename, self.duration, self.show['name'], self.song)
        elif self.is_song:
            return '%s - %d seconds ***** song: "%s"' % (self.filename, self.duration, self.song)
        return '%s - %d seconds' % (self.filename, self.duration)
