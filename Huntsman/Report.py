from multiprocessing.dummy import Queue


class Report:
    def __init__(self, metrics_queue: Queue):
        self._metrics_queue = metrics_queue
        self._flush()

    def make_html(self):
        item = self._metrics_queue.get()
        metrics = f"<p>reaped subreddit: {item}</p>"
        self._flush()
        return metrics

    def _flush(self):
        self.comment_count = 0
        self.submission_count = 0
