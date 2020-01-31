import select
import logging
from queue import Empty, Full
from collections import defaultdict
import multiprocessing as mp

from util import CHUNK_SIZE, LONG_TIMEOUT


# Using objcets doesn't work, since they are made diffent between processes,
# but classes provide nice sentinels.
class EmptyBuffers:
# Signal workers to empty buffers
    pass

class End:
# Signal workers to shut down
    pass

class NoKey:
    pass

def multi_get(qs, t1, t2=0, repeat=True):
    """ Get from multiple mp.Queues. """
    pipes = [q._reader for q in qs]
    while True:
        socks, _, _ = select.select(pipes, [], [], t1)
        # Run through queues in prioritized order
        for q in qs:
            if q._reader in socks:
                try:
                    return q, q.get(timeout=t2)
                except Empty:
                    logging.debug(f'Saw value, but it was stolen.')
        if not repeat:
            break

class Worker(mp.Process):
    def __init__(self, name, feed_queue, work_queue, queues, layers):
        super(Worker, self).__init__()
        self.name = f'worker {name}'
        self.feed_queue = feed_queue
        self.work_queue = work_queue
        self.my_queue = queues[name]
        self.queues = queues
        self.layers = layers

        self.ending = False
        self.buffer = defaultdict(list)
        self.task_buffer = []
        self.overflow = 0

        # TODO: By setting daemon = True we prevent tasks from making their
        # own processes. Is that what we want?
        # self.daemon = False

    def send(self, key, level, chunk):
        if key is not NoKey:
            self.queues[hash(key)%len(self.queues)].put((level, chunk), timeout=LONG_TIMEOUT)
        else:
            try:
                self.work_queue.put_nowait((level, chunk))
            except Full:
                # Register the queue fullness to increase chunksizes
                self.overflow = min(self.overflow+1, 5)
                #logging.debug(f'Overflow level in {self.name} increased to {self.overflow}')
                # Pull out as many tasks as possible into local memory
                while True:
                    try:
                        task = self.work_queue.get_nowait()
                        self.task_buffer.append((self.work_queue, task))
                    except Empty:
                        break
                # Then put again, this time waiting longer
                self.work_queue.put((level, chunk), timeout=LONG_TIMEOUT)

    def get_task(self):
        if self.task_buffer:
            return self.task_buffer.pop()

        prioritized = (self.my_queue, self.work_queue, self.feed_queue)
        q_task = multi_get(prioritized, t1=0, repeat=False)
        if q_task is not None:
            return q_task
        else:
            # Otherwise empty the buffers so we don't deadlock
            self.empty_buffers()
            # Register the queue emptyness to decrease chunksizes
            self.overflow = max(self.overflow-1, -5)
            #logging.debug(f'Overflow level in {self.name} decreased to {self.overflow}')

        return multi_get(prioritized, t1=LONG_TIMEOUT, t2=0.001)

    def empty_buffers(self):
        if self.buffer and any(self.buffer.values()):
            logging.debug(f'{self.name} emptying buffer {self.buffer}')
            # TODO: Would it be ok to combine some keys here? Does chunk_by_key guarantee
            # full exclusivity? Or just that values with the same key are together?
            for (level, key), chunk in self.buffer.items():
                if chunk:
                    self.send(key, level, chunk)
            self.buffer.clear()

    def run(self):
        while True:
            q, task = self.get_task()

            logging.debug(f'{self.name} got {task}')

            if task is EmptyBuffers:
                assert not self.ending
                self.empty_buffers()
                q.task_done()
                self.ending = True
                continue

            if task is End:
                logging.debug(f'{self.name} exiting')
                assert not (self.buffer and any(self.buffer.values()))
                q.task_done()
                return

            level, chunk = task
            func = self.layers[level]

            # Include buffered elements if we have any
            if self.buffer[level, NoKey]:
                chunk = list(chunk) + self.buffer[level, NoKey]
                del self.buffer[level, NoKey]
            try:
                for key, chunk_out in func(chunk):
                    buf = self.buffer[level + 1, key]
                    buf.extend(chunk_out)
                    # If queues are full, use larger chunks and vice versa.
                    chunksize = max(1, int(CHUNK_SIZE * 2**(self.overflow)))
                    while len(buf) >= chunksize:
                        self.send(key, level+1, buf[-chunksize:])
                        del buf[-chunksize:]
            except Exception as e:
                logging.warn(f'{self.name} got error executing func {func} {e}')

            # If we're shutting down, we don't want to end up with a non-empty buffer
            if self.ending:
                self.empty_buffers()

            q.task_done()


