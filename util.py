import itertools

# CHUNK_SIZE = 10000  # Chunk size when rechunking flatmap
CHUNK_SIZE = 1000  # Chunk size when rechunking flatmap
SHUFFLE_CHUNKS = 1000  # Chunk size when rechunking flatmap

LONG_TIMEOUT = 300


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
