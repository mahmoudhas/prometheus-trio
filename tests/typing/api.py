"""
Some examples of prometheus-async typing integration.
"""

from asyncio import Future
from functools import partial

import trio

from prometheus_client.metrics import Counter, Gauge, Summary
from twisted.internet.defer import Deferred

from prometheus_async import aio, tx
from prometheus_async import trio as ptrio


REQ_DURATION = Summary("REQ_DUR", "Request duration")
IN_PROG = Gauge("IN_PROG", "In progress")
ERRORS = Counter("ERRORS", "Errors")


@aio.time(REQ_DURATION)
async def func(i: int) -> str:
    return str(i)


@aio.track_inprogress(IN_PROG)
async def func2(i: int) -> str:
    return str(i)


class C:
    @aio.time(REQ_DURATION)
    async def method(self, i: int) -> str:
        return str(i)


@aio.time(REQ_DURATION)
def future_func(i: int) -> Future[str]:
    return Future()


# `time` can also be applied to futures directly.
future = Future[str]()
aio.time(REQ_DURATION, future)


async def coro() -> None:
    pass


# `time` can also be applied to coroutines
aio.time(REQ_DURATION, coro())

# `time` errors on coroutine fns
aio.time(REQ_DURATION, coro)  # type: ignore[call-overload]
# `time` errors on non-futures
aio.time(REQ_DURATION, int)  # type: ignore[call-overload]


@aio.time(REQ_DURATION)  # type: ignore[type-var]
def should_be_async_func(i: int) -> str:
    return str(i)


#
# Twisted
#
tx.time(REQ_DURATION, Deferred())


@tx.time(REQ_DURATION)
def returns_deferred(param: int) -> Deferred:
    return Deferred()


returns_deferred(1)

# Invalid, takes an int.
returns_deferred("str")  # type: ignore[arg-type]


#
# Trio
#
@ptrio.time(REQ_DURATION)
async def trio_func(i: int) -> str:
    return str(i)


@ptrio.track_inprogress(IN_PROG)
async def trio_func2(i: int) -> str:
    return str(i)


@ptrio.count_exceptions(ERRORS, exc=ValueError)
async def trio_func3(i: int) -> str:
    return str(i)


class TrioC:
    @ptrio.time(REQ_DURATION)
    async def method(self, i: int) -> str:
        return str(i)


async def trio_main() -> None:
    # Decorators keep the signature.
    result: str = await trio_func(1)
    await trio_func("1")  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

    # Everything can also be applied to awaitables directly.
    ptrio.time(REQ_DURATION, coro())
    ptrio.track_inprogress(IN_PROG, coro())
    ptrio.count_exceptions(ERRORS, coro(), exc=ValueError)

    # They error on coroutine fns...
    ptrio.time(REQ_DURATION, coro)  # type: ignore[call-overload]  # ty: ignore[invalid-argument-type]
    # ...and on non-awaitables.
    ptrio.time(REQ_DURATION, int)  # type: ignore[call-overload]  # ty: ignore[invalid-argument-type]

    print(result)


@ptrio.time(REQ_DURATION)  # type: ignore[type-var]  # ty: ignore[invalid-argument-type]
def should_be_async_trio_func(i: int) -> str:
    return str(i)


async def trio_web(nursery: trio.Nursery) -> None:
    server: ptrio.web.MetricsHTTPServer = await nursery.start(
        partial(
            ptrio.web.serve,
            addr="127.0.0.1",
            service_discovery=ptrio.sd.ConsulAgent(name="app", tags=("x",)),
        )
    )
    url: str = server.url
    addr, port = server.socket
    print(url, addr + str(port), server.https, server.is_registered)


def trio_thread() -> None:
    threaded = ptrio.web.start_http_server_in_thread(
        port=8000, service_discovery=ptrio.sd.ConsulAgent()
    )
    print(threaded.url, threaded.socket.port)
    threaded.close()
