import time
from pystreams import Stream


def test():
    N = 1000

    print(sum(filter((lambda x: x % 10 == 0), range(N))))

    print(Stream(range(N))
          .filter(lambda x: x % 10 == 0)
          .sum())

    # We need some sleeping after any, since the stream may still be going,
    # and so we may not have enough file desriptors to start another one
    print(Stream(range(N)).map(lambda x: x == 30).any())
    time.sleep(.1)
    print(Stream(range(N)).map(lambda x: x == -30).any())
    time.sleep(.1)
    print(Stream(range(N)).map(lambda x: x == 30).all())
    time.sleep(.1)
    print(Stream(range(N)).map(lambda x: x <= N).all())
    time.sleep(.1)

    # Count distinct words
    print(Stream(["hej hej med dig", "hvad med dig?"])
          .flatmap(lambda sentence: sentence.split())
          .chunk_by_key(lambda x: hash(x) % 10)
          .reduce_once(lambda chunk: len(set(chunk)))
          .sum())

    # Count characters in a file
    print(Stream(open('words'))
          .flatmap(tuple)
          .count())


def benchmark():
    import time

    def isprime(n):
        return not any(n % p == 0 for p in range(2, n))
    N = 10**3

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
        def inner(todo, tail):
            if not todo:
                return Stream([tail])
            return Stream(todo) \
                .filter(lambda r: safe(r + 1, r - 1, tail)) \
                .flatmap(lambda r: inner(todo.difference([r]), (r,) + tail))

        def safe(d1, d2, tail):
            return not tail or d1 != tail[0] and d2 != tail[0] and safe(
                d1 + 1, d2 - 1, tail[1:])
        return inner(set(range(n)), ())

    def perms(n):
        def inner(todo, tail):
            if not todo:
                return Stream([tail])
            return Stream(todo).flatmap(lambda r: inner(
                todo.difference([r]), (r,) + tail))
        return inner(set(range(n)), ())

    # TODO: Can't go above 5 because each Stream has its own workers,
    # so we end up opening "too many files"
    print('Count n-queens solutions:')
    for n in range(1, 6):
        print(f'{n}: {queens(n).count()}')

    print('\nFind n-queens:')
    for n in range(1, 6):
        print(n, ':', next(iter(queens(n)), None))

    print('\nPermutations:')
    print(perms(5).count(), '= 5!')
    perms(3).foreach(print)


if __name__ == '__main__':
    test()
