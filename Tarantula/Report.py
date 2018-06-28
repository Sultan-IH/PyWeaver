from multiprocessing.dummy import Queue


class Report:
    def __init__(self, metrics_queue: Queue):
        self._metrics_queue = metrics_queue
        self._flush()

    def make_html(self):
        self._count()
        metrics = f"<p>updates made: {self.update_count}</p> <p>update cycles done: {self.cycle_count}</p>"
        self._flush()
        return metrics

    def make_str(self):
        self._count()
        metrics = f"""
            METRICS REPORT:
                updates made: [{self.update_count}]
                update cycles done: [{self.cycle_count}]
        """
        self._flush()
        return metrics

    def _count(self):
        for _ in range(self._metrics_queue.qsize()):
            item = self._metrics_queue.get()
            if item == 'score':
                self.update_count += 1
            elif item == 'cycle':
                self.cycle_count += 1

    def _flush(self):
        self.update_count = 0
        self.cycle_count = 0
