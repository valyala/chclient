# chclient - fast http client for `ClickHouse`

## Features

* Optimized for reading large responses.
* May read more than 20M rows per second on a single CPU core thanks
  to [tsvreader](https://github.com/valyala/tsvreader).
* Easily reads responses with billion rows thanks to response streaming.
  There is no need to fit the whole response in memory.
* Works ideally with [chproxy](https://github.com/Vertamedia/chproxy).

## Documentation

See [these docs](https://godoc.org/github.com/valyala/chclient).
