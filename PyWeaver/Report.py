from multiprocessing.dummy import Queue


class Report:
    def __init__(self, metrics_queue: Queue):
        self._metrics_queue = metrics_queue
        self._flush()

    def make_html(self):
        self._count()
        metrics = f"<p>comment count: {self.comment_count}</p> <p>submission count: {self.submission_count}</p>"
        self._flush()
        return metrics

    def make_str(self):
        self._count()
        metrics = f"""
            METRICS REPORT:
                comments scraped: [{self.comment_count}]
                submissions scraped: [{self.submission_count}]
        """
        self._flush()
        return metrics

    def _count(self):
        for _ in range(self._metrics_queue.qsize()):
            item = self._metrics_queue.get()
            if item == 'comment':
                self.comment_count += 1
            elif item == 'submission':
                self.submission_count += 1

    def _flush(self):
        self.comment_count = 0
        self.submission_count = 0
