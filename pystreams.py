import time
import random
import functools
import itertools
import logging
import threading
from queue import Empty, Full
from collections import defaultdict
import multiprocessing as mp

from worker import Worker, End, EmptyBuffers, NoKey
from util import *

mp.set_start_method('fork')
import resource
print("getrlimit before:", resource.getrlimit(resource.RLIMIT_NOFILE))
resource.setrlimit(resource.RLIMIT_NOFILE, (2**12, 2**62))

#logger = mp.log_to_stderr()
#logger.setLevel(logging.INFO)
#logger.setLevel(mp.SUBDEBUG)


class SequentialStream:
    def __init__(self, data=None):
        self.data = data

    def filter(self, f):
        self.data = filter(f, self.data)
        return self

    def map(self, f):
        self.data = map(f, self.data)
        return self

    def flatmap(self, f, lazy=True):
        if lazy:
            self.data = itertools.chain(map(f, self.data))
        else:
            self.data = sum(map(f, self.data), [])
        return self

    def sum(self):
        return sum(self.data)

    def count(self):
        return len(list(self.data))

    def foreach(self, f):
        for x in self.data:
            f(x)

    def __iter__(self):
        return iter(self.data)

# The advantage of EagerStream is probably in how well it pickles
class EagerStream:
    def __init__(self, data=None):
        self.data = data

    def filter(self, f):
        self.data = list(filter(f, self.data))
        return self

    def map(self, f):
        self.data = list(map(f, self.data))
        return self

    def flatmap(self, f):
        self.data = sum(map(list, map(f, self.data)), [])
        return self

    def sum(self):
        return sum(self.data)

    def count(self):
        return len(list(self.data))

    def foreach(self, f):
        for x in self.data:
            f(x)

    def __iter__(self):
        return iter(self.data)

class FunctionalStream:
    def __init__(self, data=None):
        self.data = data

    def filter(self, f):
        return FunctionalStream(filter(f, self.data))

    def map(self, f):
        return FunctionalStream(map(f, self.data))

    def flatmap(self, f):
        return FunctionalStream(itertools.chain(map(f, self.data)))

    def sum(self):
        return sum(self.data)

    def count(self):
        return len(list(self.data))

    def foreach(self, f):
        for x in self.data:
            f(x)

    def __iter__(self):
        return iter(self.data)

class Stream:
    def __init__(self, data=None, n=None):
        if n is None:
            n = mp.cpu_count()
        self.n = n
        # We want to limit the memory consumption, but we can only safely do it
        # at the feeding level

        self.feed_queue = mp.JoinableQueue(FEED_MAX_SIZE)
        #self.feed_queue = mp.JoinableQueue()
        self.work_queue = mp.JoinableQueue()
        self.layers = []
        self.pool = []
        self.signal_queues = []
        self.feed_thread = None
        self.started = False
        if data is not None:
            self._data(data)
        self.cleanup_thread = None

    def join(self):
        self.cleanup_thread.join()

    def _start(self):
        assert not self.started
        self.started = True
        logging.info(f'Starting {self.n} processes')
        self.signal_queues = [mp.JoinableQueue() for _ in range(self.n)]
        for i in range(self.n):
            w = Worker(i, self.feed_queue, self.work_queue, self.signal_queues, self.layers)
            w.start()
            self.pool.append(w)

    def _stop(self):
        assert self.started
        # It's safe to wait for the feeding thread, since the other processes
        # will make sure to empty its queue so it can finish.
        logging.info('waiting for done feeding')
        self.feed_thread.join()

        # We wait for the queues to settle before sending EmptyBuffers,
        # so the buffers have a chance to actually work.
        logging.info('waiting for feed queue join')
        self.feed_queue.join()
        logging.info('waiting for worker queue join')
        self.work_queue.join()

        logging.info('telling workers to empty buffers')
        for signals in self.signal_queues:
            signals.put(EmptyBuffers, timeout=LONG_TIMEOUT)
        logging.info('making sure the message is received')
        for signals in self.signal_queues:
            signals.join()
        logging.info('waiting for work_queue again')
        self.work_queue.join()

        logging.info('sending End')
        for signals in self.signal_queues:
            signals.put(End, timeout=LONG_TIMEOUT)
        logging.info('waiting for processes')
        for w in self.pool:
            w.join()
            w.terminate()

        logging.info('closing')
        for closable in self.signal_queues + self.pool + \
                [self.feed_queue, self.work_queue]:
            closable.close()
        for queue in self.signal_queues + [self.feed_queue, self.work_queue]:
            queue.join_thread()

        logging.info('stream closed')

    def _run(self):
        self._start()
        self._stop()

    def _data(self, it, chunksize=None):
        if chunksize is None:
            if hasattr(it, '__len__'):
                chunksize = max(len(it)//self.n, 1)
            else:
                chunksize = CHUNK_SIZE
        def inner():
            logging.debug('starting feeding thread')
            for i, chunk in enumerate(slice(it, chunksize)):
                #logging.info(f'feeding chunk {i}')
                self.feed_queue.put((0, chunk), timeout=LONG_TIMEOUT)
                #logging.info(f'fed chunk {i}')
        self.feed_thread = threading.Thread(target=inner)
        # We make sure all data is feed by calling
        # self.feed_thread_join() in _stop
        self.feed_thread.start()
        return self

    def to_list(self):
        collect(lambda chunk: list(chunk),
                lambda lists: sum(lists, []),
                lambda x: x)

    def collect(self, from_chunk, combine, finisher=None):
        # Should really repeat `from_objects` till number of chunks is small enough
        combine_many = functools.partial(functools.reduce, combine)
        res = combine_many(self.reduce_once(from_chunk).reduce_once(combine_many))
        if finisher is not None:
            return finisher(res)
        return res

    def chunk_by_key(self, key_function):
        def inner(chunk):
            # return zip(map(key_function, chunk), chunk)
            for x in chunk:
                yield (key_function(x), (x,))
        self.layers.append(inner)
        return self

    def map(self, f):
        def inner(chunk):
            yield (NoKey, map(f, chunk))
        self.layers.append(inner)
        return self

    def filter(self, f):
        def inner(chunk):
            yield (NoKey, filter(f, chunk))
        self.layers.append(inner)
        return self

    def shuffle(self, chunks=None):
        if chunks is None:
            chunks = SHUFFLE_CHUNKS
        self.chunk_by_key(lambda x: hash(x) % chunks)

        def shuffled(tup):
            l = list(tup)
            random.shuffle(l)
            return l
        return self.chunkmap(shuffled)

    def distinct(self, chunks=None):
        if chunks is None:
            chunks = SHUFFLE_CHUNKS
        return (self
            .chunk_by_key(lambda x: hash(x) % chunks)
            .reduce_once(lambda chunk: len(set(chunk)))
            .sum())

    def flatmap(self, f):
        def inner(chunk):
            for x in chunk:
                # TODO: What if the result of `f` is very large, or infinite?
                # In that case it would be nice to use the feeder thread and queue
                # to slowly ingest the data as space becomes available.

                # TODO: Can we be smarter if f(x) is itself a Stream?
                # In Java, if f(x) is a stream, it is changed to a sequential stream.
                # We currently don't have a way to convert however.
                # Also, the Java streams are lazy, so flatmapping to an infinite
                # stream works. Currently we can only flatmap to EagerStream.
                yield (NoKey, tuple(f(x)))
        self.layers.append(inner)
        return self

    def map_chunks(self, f):
        def inner(chunk):
            yield (NoKey, f(chunk))
        self.layers.append(inner)
        return self

    def reduce_once(self, f):
        def inner(chunk):
            yield (NoKey, (f(chunk),))
        self.layers.append(inner)
        return self

    def reduce(self, f, unit):
        def inner(chunk):
            yield (NoKey, (functools.reduce(f, chunk, unit),))
        self.layers.append(inner)
        return functools.reduce(f, self, unit)

    def foreach(self, f):
        # Note that foreach calls the functioin from different processes.
        def inner(chunk):
            for x in chunk:
                f(x)
            yield from ()
        self.layers.append(inner)
        self._run()

    def __iter__(self):
        """ Iterable implementation which merges all chunks in the main thread. """
        q = mp.Queue()
        def inner(chunk):
            q.put(chunk, timeout=LONG_TIMEOUT)
            yield from ()
        self.layers.append(inner)

        self._start()

        def stop_then_end():
            # _stop ensures that all the calls to inner() has been made.
            self._stop()
            q.put(End, timeout=LONG_TIMEOUT)
            time.sleep(.1) # Python bug https://bugs.python.org/issue35844
            q.close()
            q.join_thread()
        self.cleanup_thread = threading.Thread(target=stop_then_end)
        #self.cleanup_thread.setDaemon(True)
        self.cleanup_thread.start()

        def iterable():
            while True:
                chunk = q.get(timeout=LONG_TIMEOUT)
                if chunk is End:
                    break
                yield from chunk

        return iter(iterable())

    def any(self):
        q = mp.Queue()

        def inner(chunk):
            if any(chunk):
                q.put_nowait(True)
            yield from ()
        self.layers.append(inner)
        self._start()

        def stop_then_end():
            self._stop()  # Wait for everything to settle down
            try:
                q.put_nowait(False)  # Didn't find anything
            except AssertionError as e:
                assert q._closed
        self.cleanup_thread = threading.Thread(target=stop_then_end)
        #self.cleanup_thread.setDaemon(True)
        self.cleanup_thread.start()

        res = q.get(timeout=LONG_TIMEOUT)
        # Cleanup
        q.close()
        q.join_thread()
        # Hopefully the thread will kill itself.

        # Try to help workers finish faster
        # for w in self.pool:
        # w.terminate()
        # for q in self.signal_queues:
        # if not q._closed:
        # q.close()

        return res

    def all(self):
        return not self.map(lambda x: not bool(x)).any()

    def count(self):
        return sum(self.reduce_once(len))

    def sum(self):
        return sum(self.reduce_once(sum))

    def min(self):
        return min(self.reduce_once(min))

    def max(self):
        return max(self.reduce_once(max))
