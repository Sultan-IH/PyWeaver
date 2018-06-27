import time

import ctypes
import inspect
import logging
import multiprocessing.dummy as mp
import threading
from typing import List

logger = logging.getLogger(__name__)


def _async_raise(tid, exctype):
    """
    Raises an exception in the threads with id tid
    """
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid),
                                                     ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # "if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class InterruptableThread(mp.Process):

    def _get_my_tid(self):
        """
        determines this (self's) thread id

        CAREFUL : this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def interrupt(self, exctype):
        """Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc( ... )
            ...
            t.raiseExc( SomeException )
            while t.isAlive():
                time.sleep( 0.1 )
                t.raiseExc( SomeException )

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL : this function is executed in the context of the
        caller thread, to raise an excpetion in the context of the
        thread represented by this instance.
        """
        _async_raise(self._get_my_tid(), exctype)


class StopWorkerException(Exception):
    pass


def kill_thread_pool(stop_work_event: mp.Event, pool: List[InterruptableThread], exctype=StopWorkerException,
                     pause: int = 30, max_retry: int = 3):
    stop_work_event.set()
    time.sleep(3)  # give em some time to terminate
    num_try = 0
    while True:
        # check how many are alive and pop a cap
        alive = [thread for thread in pool if thread.is_alive()]
        if alive:
            if num_try < max_retry:
                num_try += 1
                time.sleep(pause)
            else:
                ids = str([thread._id for thread in alive])
                logger.info(f"live threads left {ids}, sending exceptions")
                for thread in alive:
                    thread.interrupt(exctype)
                time.sleep(0.1)
        else:
            logger.info("thread pool terminated")
            break

    stop_work_event.clear()
