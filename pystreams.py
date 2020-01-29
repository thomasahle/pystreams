import random
import select
import functools
import itertools
import logging
import threading
from queue import Empty
from collections import defaultdict
import multiprocessing as mp

# logging.basicConfig(level=logging.DEBUG)

FEED_MAX_SIZE = 100  # Maximum number of chunks in feeding queue
CHUNK_SIZE = 10000  # Chunk size when rechunking flatmap
SHUFFLE_CHUNKS = 1000  # Chunk size when rechunking flatmap

# Using objcets doesn't work, since they are made diffent between processes,
# but classes provide nice sentinels.

# Signal workers to empty buffers


class EmptyBuffers:
    pass

# Signal workers to shut down


class End:
    pass

# Signal workers to shut down


class NoKey:
    pass


def slice(it, chunksize=1):
    # More efficient slicing for lists and ranges
    if hasattr(it, '__getitem__') and hasattr(it, '__len__'):
        n = len(it)
        for i in range(0, n, chunksize):
            yield it[i: i + chunksize]
    else:
        it = iter(it)
        while True:
            xs = tuple(itertools.islice(it, chunksize))
            if not xs:
                return
            yield xs


def worker(name, feed_queue, work_queue, signals, layers):
    ending = False
    buffer = defaultdict(list)

    def empty_buffers():
        if buffer:
            logging.debug(f'worker {name} emptying buffer {buffer}')
            for (level, key), chunk in buffer.items():
                if chunk:
                    work_queue.put((level, chunk))
            buffer.clear()

    qs = {feed_queue._reader: feed_queue,
          work_queue._reader: work_queue,
          signals._reader: signals}
    while True:
        # Check if a value is immediately availabe
        (socks, [], []) = select.select(qs.keys(), [], [], 0)
        # Otherwise empty the buffers so we don't deadlock
        if not socks:
            empty_buffers()

        # Pick item from one of the queues
        while True:
            # We might or might not already have a socket from the previous check
            if not socks:
                (socks, [], []) = select.select(qs.keys(), [], [])
            q = qs[socks[0]]
            try:
                task = q.get(timeout=0.001)
            except Empty:
                logging.debug(f'Worker {name} saw value, but it was stolen.')
                del socks[:]  # kill socks, since otherwise we don't call select next time
                continue
            break

        logging.debug(f'worker {name} got {task}')

        if task is EmptyBuffers:
            assert not ending
            empty_buffers()
            ending = True
            q.task_done()
            continue

        if task is End:
            logging.debug(f'worker {name} exiting')
            if buffer:
                # We still need a way to clear things after End
                logging.error(f'worker {name} is exiting with non-empty buffer {buffer}')
            break

        level, chunk = task
        # Include buffered elements if we have any
        if buffer[level, NoKey]:
            chunk = list(chunk) + buffer[level, NoKey]
            del buffer[level, NoKey]
        try:
            if level >= len(layers):
                debug.error(f'Got level {level}, but only has layers {layers}')
            func = layers[level]
            for key, chunk_out in func(chunk):
                buf = buffer[level + 1, key]
                buf.extend(chunk_out)
                while len(buf) >= CHUNK_SIZE:
                    work_queue.put((level + 1, buf[-CHUNK_SIZE:]))
                    del buf[-CHUNK_SIZE:]
        except Exception as e:
            logging.warn(f'{name} got error executing func {func} {e}')

        # If we're shutting down, we don't want to end up with a non-empty buffer
        if ending:
            empty_buffers()

        q.task_done()


class Stream:
    def __init__(self, data=None, n=None):
        if n is None:
            n = mp.cpu_count()
        self.n = n
        # We want to limit the memory consumption, but we can only safely do it
        # at the feeding level
        self.feed_queue = mp.JoinableQueue(FEED_MAX_SIZE)
        self.work_queue = mp.JoinableQueue()
        self.layers = []
        self.pool = []
        self.signal_queues = []
        self.feed_thread = threading.Thread()
        self.started = False
        if data is not None:
            self._data(data)

    def _start(self):
        assert not self.started
        self.started = True
        logging.info(f'Starting {self.n} processes')
        for i in range(self.n):
            signals = mp.JoinableQueue()
            self.signal_queues.append(signals)
            w = mp.Process(target=worker, args=(
                i, self.feed_queue, self.work_queue, signals, self.layers))
            # TODO: By setting daemon = True we prevent tasks from making their
            # own processes. Is that what we want?
            #w.daemon = True
            w.start()
            self.pool.append(w)

    def _stop(self):
        assert self.started
        logging.debug('waiting for done feeding')
        self.feed_thread.join()

        logging.debug('telling workers to empty buffers')
        for signals in self.signal_queues:
            signals.put(EmptyBuffers)
        logging.debug('making sure the message is received')
        for signals in self.signal_queues:
            signals.join()

        logging.debug('waiting for queues join')
        self.feed_queue.join()
        self.work_queue.join()

        logging.debug('sending End')
        for signals in self.signal_queues:
            signals.put(End)
        logging.debug('waiting for processes')
        for w in self.pool:
            w.join()

        logging.debug('closing')
        for closable in self.signal_queues + self.pool + \
                [self.feed_queue, self.work_queue]:
            closable.close()
        for queue in self.signal_queues + [self.feed_queue, self.work_queue]:
            queue.join_thread()

        logging.debug('stream closed')

    def _run(self):
        self._start()
        self._stop()

    def _data(self, it, chunksize=None):
        if chunksize is None:
            chunksize = CHUNK_SIZE

        def inner():
            logging.debug('starting feeding thread')
            for chunk in slice(it, chunksize):
                self.feed_queue.put((0, chunk))
        self.feed_thread._target = inner
        self.feed_thread.start()
        return self

    def to_list(self):
        collect(lambda chunk: list(chunk),
                lambda lists: sum(lists, []),
                lambda x: x)

    def collect(self, from_chunk, from_objects, finisher):
        # Should really repeat `from_objects` till number of chunks is small enough
        return finisher(iter(self
            .reduce_once(from_chunk)
            .reduce_once(from_objects)))

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
                # TODO: Can we be smarter if f(x) is itself a Stream?
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
            q.put(chunk)
            yield from ()
        self.layers.append(inner)
        self._start()

        def stop_then_end():
            self._stop()
            q.put(End)
        threading.Thread(target=stop_then_end).start()

        def iterable():
            while True:
                chunk = q.get()
                if chunk is End:
                    break
                yield from chunk
        return iter(iterable())

    def any(self):
        q = mp.Queue()

        def inner(chunk):
            if any(chunk):
                q.put(True)
            yield from ()
        self.layers.append(inner)
        self._start()

        def stop_then_end():
            self._stop()  # Wait for everything to settle down
            q.put(False)  # Didn't find anything
        threading.Thread(target=stop_then_end).start()

        res = q.get()

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
