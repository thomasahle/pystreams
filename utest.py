from pystreams import Stream, SequentialStream, EagerStream, FunctionalStream
import unittest
import collections
from queue import Empty
import multiprocessing as mp

import util
util.CHUNK_SIZE = 10  # Smaller sizes are more likly to find errors

# TODO: Test shuffle, map_chunks, min, max, distinct (broken)

def read_queue(q, n):
    for _ in range(n):
        yield q.get()

class TestStream(unittest.TestCase):

    def test_lines(self):
        with open('words') as file:
            chars = ''.join(file)
        self.assertTrue(Stream()
                        .lines(open('words'))
                        .flatmap(tuple)
                        .take_one() in chars)

        self.assertTrue(list(Stream()
                             .lines(open('words'))
                             .flatmap(tuple))[0] in chars)

        self.assertEqual(Stream()
                         .lines(open('words'))
                         .flatmap(tuple)
                         .count(), len(chars))

    def test_filter(self):
        N = 1000
        seq = sum(filter((lambda x: x % 10 == 0), range(N)))

        self.assertEqual(
            SequentialStream(range(N))
            .filter(lambda x: x % 10 == 0)
            .sum(), seq)

        self.assertEqual(
            FunctionalStream(range(N))
            .filter(lambda x: x % 10 == 0)
            .sum(), seq)

        self.assertEqual(
            Stream(range(N))
            .filter(lambda x: x % 10 == 0)
            .sum(), seq)

    def test_any(self):
        N = 1000
        self.assertTrue(Stream(range(N)).map(lambda x: x == 30).any())
        self.assertFalse(Stream(range(N)).map(lambda x: x == -30).any())
        self.assertFalse(Stream(range(N)).map(lambda x: x == 30).all())
        self.assertTrue(Stream(range(N)).map(lambda x: x <= N).all())

    def test_chunk(self):
        lorem = [
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
            "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."]

        seq = collections.Counter(w for sentence in lorem for w in sentence.split())
        distinct = len(seq)

        # (Broken) Count distinct words
        self.assertGreaterEqual(
            Stream(lorem)
            .flatmap(lambda sentence: sentence.split())
            .chunk_by_key(lambda x: hash(x) % 10)
            .reduce_once(lambda chunk: len(set(chunk)))
            .sum(), distinct)

        # Count distinct words (also broken)
        def add(s1, s2):
            s1.update(s2)
            return s1
        self.assertGreaterEqual(
            Stream(lorem)
            .flatmap(lambda sentence: sentence.split())
            .chunk_by_key(lambda w: hash(w) % 10)
            .collect(set, add, len),
            distinct)

        # Count distinct words
        self.assertEqual(
            Stream(lorem)
            .flatmap(lambda sentence: sentence.split())
            .chunk_by_key(lambda w: hash(w) % 10)
            .collect(collections.Counter, add),
            seq)

    def test_take(self):

        self.assertGreaterEqual(
            len(list(Stream(range(10**3))
                     .flatmap(lambda x: EagerStream(range(x, x + 1000)))
                     .take_stream(5))), 5)

        self.assertEqual(
            len(list(Stream(range(10**3))
                     .flatmap(lambda x: EagerStream(range(x, x + 1000)))
                     .take(5))), 5)

        self.assertLess(
            Stream(range(10**3))
            .flatmap(lambda x: EagerStream(range(x, x + 1000)))
            .take_one(), 10**3)

    def test_count(self):
        self.assertEqual(Stream(range(10**6)).count(), 10**6)
        self.assertEqual(SequentialStream(range(10**6)).count(), 10**6)

        self.assertEqual(Stream(range(10**3))
                         .flatmap(lambda x: SequentialStream(range(x, x + 1000)))
                         .count(), 10**6)
        self.assertEqual(Stream(range(10**3))
                         .flatmap(lambda x: EagerStream(range(x, x + 1000)))
                         .count(), 10**6)

    def test_foreach(self):
        N = 10**2
        q = mp.Queue()
        Stream(range(N)).foreach(lambda x: q.put(x))
        self.assertEqual(sorted(read_queue(q, N)), list(range(N)))

    def test_peek():
        N = 10**2
        q = mp.Queue()
        s = Stream(range(N)).peek(lambda x: q.put(x)).sum()
        self.assertEqual(s, sum(range(N)))
        self.assertEqual(sorted(read_queue(q, N)), list(range(N)))


if __name__ == '__main__':
    unittest.main()
