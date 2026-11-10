# prometheus-trio

<a href="https://github.com/mahmoudhas/prometheus-trio/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-C06524" alt="License: Apache 2.0" /></a>

<!-- teaser-begin -->

This is a fork of [*prometheus-async*](https://github.com/hynek/prometheus-async) that adds support for [Trio](https://trio.readthedocs.io/) to the official [Python client](https://github.com/prometheus/client_python) for the [Prometheus](https://prometheus.io/) metrics and monitoring system.
The Python module is still called `prometheus_async`.

Currently Trio and [Twisted](https://twisted.org) are supported.

It works by wrapping the metrics from the official client:

```python
import trio

from prometheus_client import Histogram
from prometheus_async.trio import time

REQ_TIME = Histogram("req_time_seconds", "time spent in requests")


@time(REQ_TIME)
async def req():
    await trio.sleep(1)
```


Even for *synchronous* applications, the metrics server can be useful since it is more powerful than the one shipped with the official client.
For that, a helper function has been added that runs it in a separate thread.

To install it, use GitHub rather than PyPI -- the *prometheus-async* there is the original and doesn't support Trio:

```console
$ python -Im pip install "prometheus-async[trio] @ git+https://github.com/mahmoudhas/prometheus-trio"
```

The source code and the documentation are hosted on [GitHub](https://github.com/mahmoudhas/prometheus-trio).


## Credits

*prometheus-async* is written and maintained by [Hynek Schlawack](https://hynek.me/), and this fork builds on his work.

The Trio support is maintained by [mahmoudhas](https://github.com/mahmoudhas).
