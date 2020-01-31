import time
import collections
import logging
from functools import partial, reduce

from pystreams import Stream, SequentialStream, FunctionalStream, EagerStream


# logging.basicConfig(level=logging.INFO)

def debug_handles():
    import psutil
    res = []
    for proc in psutil.process_iter():
        try:
            res.append((len(proc.open_files()), float('NaN'), proc))
        except psutil.AccessDenied:
            pass
    res.sort(reverse=True)
    for cnt, _, proc in res:
        if cnt > 10:
            print(cnt, proc)


def test():
    N = 1000

    print(sum(filter((lambda x: x % 10 == 0), range(N))))

    print(SequentialStream(range(N))
          .filter(lambda x: x % 10 == 0)
          .sum())

    print(FunctionalStream(range(N))
          .filter(lambda x: x % 10 == 0)
          .sum())

    print(Stream(range(N))
          .filter(lambda x: x % 10 == 0)
          .sum())

    print(Stream(range(N)).map(lambda x: x == 30).any())
    print(Stream(range(N)).map(lambda x: x == -30).any())
    print(Stream(range(N)).map(lambda x: x == 30).all())
    print(Stream(range(N)).map(lambda x: x <= N).all())

    lorem = [
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
        "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."]

    # (Broken) Count distinct words
    print(Stream(lorem)
          .flatmap(lambda sentence: sentence.split())
          .chunk_by_key(lambda x: hash(x) % 10)
          .reduce_once(lambda chunk: len(set(chunk)))
          .sum())

    # Count distinct words
    def add(s1, s2): s1.update(s2)
    return s1
    print(Stream(lorem)
          .flatmap(lambda sentence: sentence.split())
          .chunk_by_key(lambda w: hash(w) % 10)
          .collect(set, add, len))

    # Count distinct words
    print(Stream(lorem)
          .flatmap(lambda sentence: sentence.split())
          .chunk_by_key(lambda w: hash(w) % 10)
          .collect(collections.Counter, add))


def test2():
    t = time.time()
    print(repr(Stream().lines(open('words'))
               .flatmap(tuple)
               .take_one()))
    print('elapsed:', time.time() - t)

    t = time.time()
    print(repr(list(Stream(open('words'))
                    .flatmap(tuple))[0]))
    print('elapsed:', time.time() - t)

    # Count characters in a file
    t = time.time()
    print(Stream(open('words'))
          .flatmap(tuple)
          .count())
    print('elapsed:', time.time() - t)


def test3():
    print(list(Stream(range(10**3))
               .flatmap(lambda x: EagerStream(range(x, x + 1000)))
               .take_stream(5)))
    print(Stream(range(10**6)).count())
    print(SequentialStream(range(10**6)).count())
    print(Stream(range(10**3))
          .flatmap(lambda x: SequentialStream(range(x, x + 1000)))
          .count())
    print(Stream(range(10**3))
          .flatmap(lambda x: EagerStream(range(x, x + 1000)))
          .count())
    print(Stream(range(10**3))
          .flatmap(lambda x: EagerStream(range(x, x + 1000)))
          .take_one())


def benchmark():
    import time

    def isprime(n):
        return not any(n % p == 0 for p in range(2, n))
    N = 3 * 10**3

    t = time.time()
    print(Stream(range(2, N))
          .flatmap(range)
          .filter(isprime)
          .sum())
    print('sum', time.time() - t)

    t = time.time()
    print(Stream(range(2, N))
          .flatmap(lambda x: range(x))
          .filter(isprime)
          .reduce((lambda x, y: x + y), 0))
    print('reduce', time.time() - t)

    t = time.time()
    print(sum(x for a in range(N) for x in range(a) if isprime(x)))
    print('seq', time.time() - t)


def sestoft():
    """ Python versions of Peter Sestoft's stream examples from Java Precicely. """

    def queens(n):
        global wrap_q, safe, safe_

        class wrap_q:
            def __init__(self, todo, tail):
                self.todo = todo
                self.tail = tail

            def __call__(self, r):
                return queens_(self.todo.difference([r]), (r,) + self.tail, 1)

        def queens_(todo, tail, depth=0):
            if not todo:
                return [tail]
            return ((Stream(todo) if depth == 0 else SequentialStream(todo))
                    .filter(lambda r: safe(r, tail))
                    # .filter(partial(safe, tail=tail))
                    .flatmap(wrap_q(todo, tail)))

        def safe(r, tail): return safe_(r + 1, r - 1, tail)

        def safe_(d1, d2, tail):
            return not tail or d1 != tail[0] and d2 != tail[0] and safe_(
                d1 + 1, d2 - 1, tail[1:])

        return queens_(set(range(n)), ())

    def perms(n):
        global perms_, wrap_p

        class wrap_p:
            def __init__(self, todo, tail):
                self.todo = todo
                self.tail = tail

            def __call__(self, r):
                return perms_(self.todo.difference([r]), (r,) + self.tail, 1)

        def perms_(todo, tail, depth=0):
            if not todo:
                return [tail]
            return ((Stream(todo) if depth == 0 else FunctionalStream(todo))
                    .flatmap(wrap_p(todo, tail)))
            # .flatmap(lambda r: perms_(todo.difference([r]), (r,) + tail, 1)))
        return perms_(set(range(n)), ())

    # TODO: Can't go above 5 because each Stream has its own workers,
    # so we end up opening "too many files"
    print('Count n-queens solutions:')
    for n in range(1, 14):
        print(f'{n}: {queens(n).count()}')

    print('\nFind n-queens:')
    for n in range(1, 10):
        st = queens(n)
        it = iter(st)
        print(n, ':', next(it, None))
        st.join()

    print('\nPermutations:')
    print(perms(8).count(), '= 8!')
    perms(3).foreach(print)


if __name__ == '__main__':
    test()
