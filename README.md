# PyStreams

PyStreams is a Python library for dealing with parallelising pipelines of tasks.

## Usage

```python
>>> from pystreams import Stream
>>> xs = ['a1', 'a2', 'b1', 'c1', 'c2']
>>> (Stream(xs)
...     .filter(lambda x: x.startswith('c'))
...     .map(lambda x: x.upper())
...     .shuffled()
...     .foreach(print))
C1
C2
```

Note pystreams support lambdas unlike the map function in `multiprocessinig`.

Another classical example is counting the number of unique words in a dataset:

```python
>>> sentences = ["a word is a word", "all words are words"]
>>> (Stream(sentences)
...           .flatmap(lambda sentence: sentence.split())
...           .chunk_by_key(lambda x: hash(x) % 10)
...           .reduce_once(lambda chunk: len(set(chunk)))
...           .sum())
6
```

Streams also easily works on files, even when the files themselves are too big to fit into memory.
The following example counts the number of characters in a (not that big) file:
```python
>>> print(Stream(open('words'))
...       .flatmap(tuple)
...       .count())
2493109
```

## Performance

PyStreams work in parallel over your CPUs.
This is done efficiently by buffering and keeping data in chunks, similar to Spark's RDDs.

```python
>>> from pystreams import Stream
>>> isprime = lambda n: not any(n % p == 0 for p in range(2, n))
>>>
>>> Stream(range(2, 10**5)).filter(isprime).sum()
454396537
```

Takes **11.33** seconds on an Intel Core i7 4, while the following sequential python code

```python
>>> print(sum(x for x in range(N) if isprime(x)))
454396537
```

takes nearly 4 times longer at **43.40s**.

## Why another Python Streams clone? 

There are a number of Streams clones on github:
 [streams](https://github.com/9seconds/streams), [python-stream](https://github.com/fm100/python-stream), [streampy](https://github.com/tolsac/streampy), [Streams](https://github.com/ashbob999/Streams), [python-streams](https://github.com/GideonBuckwalter/python-streams), [python-streaming](https://github.com/dalonsog/python-streaming) and [pyStream](https://github.com/PiotrOpielski/pyStream).
Unfortunately, neither of them has great parallelism support, and most don't support methods such as `flatmap` and `chunk_by_key` as used in the examples above.
That said, this library still has a long way to go to match Java Streams and Spark's RDD feature by feature.
For example, it is not possible to run workers on a cluster or anything else not supported by Python's `multiprocessing`.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[AGPL3](https://choosealicense.com/licenses/agpl-3.0/)


