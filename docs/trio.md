(trio-api)=

# Trio Support

```{eval-rst}
.. currentmodule:: prometheus_async.trio
```

The Trio-related APIs can be found within the `prometheus_async.trio` package.

They are the counterparts of the [asyncio](asyncio-api) ones, but they work *with* Trio instead of merely on top of it: they follow Trio's clock, they understand cancellation and exception groups, and the web server is a regular Trio task that you can supervise like any other.

The decorators only need [Trio](https://trio.readthedocs.io/).
The web server and the service discovery are based on [h11](https://h11.readthedocs.io/) (aiohttp can't run on Trio), so you need to install that, too.
See {doc}`installation`.


## Decorator Wrappers

All of these functions take a *prometheus_client* metrics object and can either be applied as a decorator to async functions and methods, or they can be passed an {term}`awaitable` -- for example, a coroutine object -- for a second argument.

```{eval-rst}
.. autofunction:: time
```

The most common use case is using it as a decorator:

```python
import trio

from prometheus_client import Histogram
from prometheus_async.trio import time

REQ_TIME = Histogram("req_time_seconds", "time spent in requests")


@time(REQ_TIME)
async def req():
    await trio.sleep(1)
```

Since the time is measured using Trio's clock, you can test your instrumented code without waiting for real:

```python
import pytest

from prometheus_client import REGISTRY


@pytest.mark.trio
async def test_req(autojump_clock):  # both from pytest-trio
    await req()

    assert 1 == REGISTRY.get_sample_value("req_time_seconds_sum")
```

```{eval-rst}
.. autofunction:: count_exceptions
```

Whenever you have a nursery in your code, the errors of its tasks are raised as exception groups.
That's why *count_exceptions* looks into them to find what you asked it to count:

```python
from prometheus_client import Counter
from prometheus_async.trio import count_exceptions

ERRORS = Counter("errors_total", "failed requests")


@count_exceptions(ERRORS, exc=ValueError)
async def req():
    async with trio.open_nursery() as nursery:
        nursery.start_soon(task_that_raises_a_value_error)
```

This counts one error, although what is raised is an {class}`ExceptionGroup`.

```{eval-rst}
.. autofunction:: track_inprogress
```


(trio-web)=

## Metric Exposure

```{eval-rst}
.. currentmodule:: prometheus_async.trio.web
```

*prometheus-async* offers a metrics server for Trio under `prometheus_async.trio.web`.
Its API follows Trio's conventions for servers -- compare {func}`trio.serve_tcp` --  instead of the one of {ref}`the aiohttp-based server <asyncio-web>`.

```{eval-rst}
.. autofunction:: serve
```

Run it using {meth}`trio.Nursery.start` to find out where it's listening:

```python
from functools import partial

import trio

from prometheus_async.trio import web


async def main():
    async with trio.open_nursery() as nursery:
        server = await nursery.start(partial(web.serve, port=9090))
        print(f"Serving metrics on {server.url}")

        await run_my_app()

        # Stops the server along with everything else in the nursery.
        nursery.cancel_scope.cancel()


trio.run(main)
```

The server generates the metrics in your Trio thread, so a collector of yours can look at Trio's state -- for example with {func}`trio.lowlevel.current_statistics` -- but it's also on you to keep your collectors fast.
Rendering a really big registry can take hundreds of milliseconds, during which nothing else runs in that thread.
If that is a problem for you, use {func}`start_http_server_in_thread` that does the work in a thread of its own.

Connections that don't do anything for 30 seconds are closed, and so are connections of clients that don't read what we send them.

:::{note}
If you want to expose metrics from an ASGI application that runs on Trio -- for instance using [Hypercorn](https://hypercorn.readthedocs.io/) -- you don't need any of this.
Mount [`prometheus_client.make_asgi_app()`](https://prometheus.github.io/client_python/exporting/http/asgi/) in your application instead.
:::

```{eval-rst}
.. autofunction:: start_http_server_in_thread
```

:::{important}
In contrast to {func}`serve`, this does not need Trio in the rest of your application.
It runs Trio in a thread of its own, so the same caveats as for {ref}`the asyncio version <asyncio-web>` apply.
:::

```{eval-rst}
.. autoclass:: MetricsHTTPServer

.. autoclass:: ThreadedMetricsHTTPServer
   :members: close
```


(trio-sd)=

## Service Discovery

```{eval-rst}
.. currentmodule:: prometheus_async.trio.sd
```

Web exposure is much more useful if it comes with an easy way to integrate it with service discovery.

Like for asyncio, *prometheus-async* only ships integration with a local Consul agent.
We do **not** plan add more.

```{eval-rst}
.. autoclass:: ConsulAgent
```

If your agent isn't running, starting the server raises an {class}`OSError`.

When the server is stopped, it deregisters itself -- even though it's being cancelled at that time -- but only for ten seconds, so a broken service discovery can't keep your application from shutting down.
Failing to deregister is logged, but not raised.


### Custom Service Discovery

Adding own service discovery methods is simple:
All you need is to provide an instance with a coroutine `register(self, metrics_server)` that registers the passed `metrics_server` with the service of your choice and returns another coroutine function that is called for de-registration when the metrics server is shut down.
It may also return `None` if it didn't register anything.

Have a look at [our implementation](https://github.com/mahmoudhas/prometheus-trio/blob/main/src/prometheus_async/trio/sd.py) if you need inspiration or check out the `TrioServiceDiscovery` {class}`typing.Protocol` in the [`types` module](https://github.com/mahmoudhas/prometheus-trio/blob/main/src/prometheus_async/types.py)
