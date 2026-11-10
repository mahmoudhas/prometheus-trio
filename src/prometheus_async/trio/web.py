# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Trio-native metrics exposure.

aiohttp can't run on Trio, so this ships a small HTTP/1.1 server based on
h11.
"""

from __future__ import annotations

import logging
import queue
import threading

from contextlib import suppress
from email.utils import formatdate
from functools import partial
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn
from urllib.parse import urlsplit

import h11
import trio

from prometheus_client import REGISTRY

from .._exposition import LANDING_PAGE, choose_generator
from ._http import MAX_RECV, next_event, send, serialize


if TYPE_CHECKING:
    import ssl

    from ..types import Deregisterer, TrioServiceDiscovery


_logger = logging.getLogger(__name__)

# Requests to a metrics endpoint have no business carrying a body.  We throw
# away this many bytes of it and give up on the connection beyond that.
_MAX_BODY = 64 * 1024

# When we're done with a connection, the client gets this long (in seconds) to
# close its side before we close ours.
_LINGER = 2.0

# How long (in seconds) shutting down -- most notably deregistering from
# service discovery -- may take.
_CLEANUP_TIMEOUT = 10.0

_TEXT = "text/plain; charset=utf-8"


def _route(request: h11.Request) -> tuple[int, str, bytes, list]:
    """
    Compute (status code, content type, body, extra headers) for *request*.
    """
    path = urlsplit(request.target.decode("latin-1")).path

    if path not in ("/", "/metrics"):
        return 404, _TEXT, b"404: Not Found", []

    if request.method not in (b"GET", b"HEAD"):
        return (
            405,
            _TEXT,
            b"405: Method Not Allowed",
            [(b"Allow", b"GET, HEAD")],
        )

    if path == "/":
        return 200, "text/html; charset=utf-8", LANDING_PAGE.encode(), []

    accept = next(
        (v.decode("latin-1") for k, v in request.headers if k == b"accept"),
        None,
    )
    generate, content_type = choose_generator(accept)

    return 200, content_type, generate(REGISTRY), []


async def _discard_body(conn: h11.Connection, stream: trio.abc.Stream) -> bool:
    """
    Read and throw away the body of the current request, if there is one.

    Return False if it's too big.
    """
    size = 0
    while True:
        event = await next_event(conn, stream)
        if not isinstance(event, h11.Data):
            # h11.EndOfMessage; there's nothing else that can happen here.
            return True

        size += len(event.data)
        if size > _MAX_BODY:
            return False


async def _serve_one_request(
    conn: h11.Connection, stream: trio.abc.Stream
) -> bool:
    """
    Read one request from *stream* and respond to it.

    Return whether the connection can be used for another request.
    """
    request = await next_event(conn, stream)
    if not isinstance(request, h11.Request):
        # h11.ConnectionClosed: The client is done with us.
        return False

    if not await _discard_body(conn, stream):
        return False

    try:
        status, content_type, body, headers = _route(request)
    except Exception:
        # A broken collector must not kill the connection.
        _logger.exception("Generating the metrics failed.")
        status, content_type, headers = 500, _TEXT, []
        body = b"500: Internal Server Error"

    events: list[h11.Event] = [
        h11.Response(
            status_code=status,
            headers=[
                (b"Content-Type", content_type.encode()),
                (b"Content-Length", str(len(body)).encode()),
                (b"Date", formatdate(usegmt=True).encode()),
                *headers,
            ],
        )
    ]
    # HEAD responses have the headers of a GET response, but no body.
    if request.method != b"HEAD":
        events.append(h11.Data(data=body))
    events.append(h11.EndOfMessage())

    await send(stream, serialize(conn, *events))

    return conn.our_state is h11.DONE and conn.their_state is h11.DONE


async def _serve(stream: trio.abc.Stream) -> None:
    """
    Serve requests on *stream* until the connection ends.
    """
    conn = h11.Connection(h11.SERVER)
    try:
        while await _serve_one_request(conn, stream):
            conn.start_next_cycle()
    except h11.RemoteProtocolError as e:
        # The client sent us garbage.  We aren't in the middle of a response
        # here: we always read a whole request before we start responding.
        body = f"{e.error_status_hint}: Bad Request".encode()
        await send(
            stream,
            serialize(
                conn,
                h11.Response(
                    status_code=e.error_status_hint,
                    headers=[
                        (b"Content-Type", _TEXT.encode()),
                        (b"Content-Length", str(len(body)).encode()),
                        (b"Connection", b"close"),
                    ],
                ),
                h11.Data(data=body),
                h11.EndOfMessage(),
            ),
        )


async def _shutdown(stream: trio.abc.Stream) -> None:
    """
    Close the sending side of *stream* and give the client a moment to close
    its own.

    If we just closed the connection, unread data from the client could cause
    a TCP reset that destroys our last response before the client has read it.
    """
    if not isinstance(stream, trio.abc.HalfCloseableStream):
        # TLS. Can't do that, but Trio's closing it properly.
        return

    with (
        trio.move_on_after(_LINGER),
        suppress(trio.BrokenResourceError, trio.ClosedResourceError),
    ):
        await stream.send_eof()
        while await stream.receive_some(MAX_RECV):
            pass


async def _handle_connection(stream: trio.abc.Stream) -> None:
    """
    Handle a connection to our server.

    This is run by Trio's server machinery that, for good reasons, takes down
    the entire server if we raise.  So, other than cancellation, we mustn't.
    """
    try:
        await _serve(stream)
    except (
        trio.TooSlowError,
        trio.BrokenResourceError,
        trio.ClosedResourceError,
    ):
        # The client has vanished (or its TLS handshake failed), or went
        # silent.  Nothing to see here.
        _logger.debug("Lost a client.", exc_info=True)
    except Exception:
        _logger.exception("Serving a request failed unexpectedly.")
    finally:
        await _shutdown(stream)


class Socket(NamedTuple):
    addr: str
    port: int


class MetricsHTTPServer:
    """
    A running metrics HTTP server.

    Handed to you by :meth:`trio.Nursery.start` when you start :func:`serve`.
    Do *not* instantiate it yourself.

    :ivar socket: Socket the server is listening on.  namedtuple of
        ``Socket(addr, port)``.
    :ivar bool https: Whether the server uses SSL/TLS.
    :ivar str url: A valid URL to the metrics endpoint.
    :ivar bool is_registered: Is the web endpoint registered with a
        service discovery system?

    .. versionadded:: 26.2.0
    """

    socket: Socket
    https: bool
    _deregister: Deregisterer | None

    def __init__(self, socket: Socket, https: bool) -> None:
        self.socket = socket
        self.https = https
        self._deregister = None

    @property
    def is_registered(self) -> bool:
        """
        Is the web endpoint registered with a service discovery system?
        """
        return self._deregister is not None

    @property
    def url(self) -> str:
        addr = self.socket.addr
        return "http{s}://{host}:{port}/".format(
            s="s" if self.https else "",
            host=addr if ":" not in addr else f"[{addr}]",
            port=self.socket.port,
        )


async def _open_listeners(addr: str, port: int) -> list[trio.SocketListener]:
    listeners = await trio.open_tcp_listeners(port, host=addr or None)

    if port == 0:
        # If the system picks the port, it picks a different one for each
        # address family.  Settle for the first one such that there's exactly
        # one port to tell people about.
        for listener in listeners[1:]:
            await listener.aclose()
        listeners = listeners[:1]

    return listeners


async def serve(
    *,
    addr: str = "",
    port: int = 0,
    ssl_ctx: ssl.SSLContext | None = None,
    service_discovery: TrioServiceDiscovery | None = None,
    task_status: trio.TaskStatus[MetricsHTTPServer] = trio.TASK_STATUS_IGNORED,
) -> NoReturn:
    """
    Serve your metrics over HTTP(S) on *addr*:*port* until cancelled.

    Like :func:`trio.serve_tcp`, this never returns by itself.
    Run it in a task of your own using :meth:`trio.Nursery.start` to get a
    :class:`MetricsHTTPServer` as soon as the server is listening (and
    registered, if you've asked for it).
    Cancel the nursery -- or a :class:`trio.CancelScope` around the task -- to
    stop it.
    On cancellation, it also deregisters from service discovery, even though
    it's being cancelled.

    If *ssl_ctx* is set, use TLS.

    :param str addr: Interface to listen on.  Leaving empty will listen on all
        interfaces.
    :param int port: Port to listen on.  If it's 0, the system picks a free
        one and the server listens on one address family only.
    :param ssl.SSLContext ssl_ctx: TLS settings
    :param service_discovery: see :ref:`trio-sd`

    The *task_status* argument is for :meth:`trio.Nursery.start` and not meant
    to be passed by you.

    :rtype: NoReturn

    .. versionadded:: 26.2.0
    """
    listeners: list[Any] = await _open_listeners(addr, port)
    server = MetricsHTTPServer(
        socket=Socket(*listeners[0].socket.getsockname()[:2]),
        https=ssl_ctx is not None,
    )
    if ssl_ctx is not None:
        listeners = [
            trio.SSLListener(listener, ssl_ctx, https_compatible=True)
            for listener in listeners
        ]

    try:
        if service_discovery is not None:
            server._deregister = await service_discovery.register(server)

        task_status.started(server)

        await trio.serve_listeners(_handle_connection, listeners)
    finally:
        # We're likely being cancelled, but we still want to clean up.
        with trio.move_on_after(_CLEANUP_TIMEOUT) as scope:
            scope.shield = True

            # They are closed already if we got to serve them, so this is
            # about the case when registering failed.
            for listener in listeners:
                await listener.aclose()

            if server._deregister is not None:
                try:
                    await server._deregister()
                except Exception:
                    _logger.exception("Deregistering the server failed.")


class ThreadedMetricsHTTPServer:
    """
    A stoppable metrics HTTP server that runs in a separate thread.

    Returned by :func:`start_http_server_in_thread`.  Do *not* instantiate it
    yourself.

    :ivar socket: Socket the server is listening on.  namedtuple of
        ``Socket(addr, port)``.
    :ivar bool https: Whether the server uses SSL/TLS.
    :ivar str url: A valid URL to the metrics endpoint.
    :ivar bool is_registered: Is the web endpoint registered with a
        service discovery system?

    .. versionadded:: 26.2.0
    """

    def __init__(
        self,
        http_server: MetricsHTTPServer,
        thread: threading.Thread,
        token: trio.lowlevel.TrioToken,
        scope: trio.CancelScope,
    ) -> None:
        self._http_server = http_server
        self._thread = thread
        self._token = token
        self._scope = scope

    def close(self) -> None:
        """
        Stop the server, deregister it, and join the thread.
        """
        # The run is over if the server has crashed or if we're closed already.
        with suppress(trio.RunFinishedError):
            self._token.run_sync_soon(self._scope.cancel)

        self._thread.join()

    @property
    def https(self) -> bool:
        return self._http_server.https

    @property
    def socket(self) -> Socket:
        return self._http_server.socket

    @property
    def url(self) -> str:
        return self._http_server.url

    @property
    def is_registered(self) -> bool:
        return self._http_server.is_registered


def start_http_server_in_thread(
    *,
    port: int = 0,
    addr: str = "",
    ssl_ctx: ssl.SSLContext | None = None,
    service_discovery: TrioServiceDiscovery | None = None,
) -> ThreadedMetricsHTTPServer:
    """
    Start a Trio HTTP(S) server in a new thread with an own :func:`trio.run`.

    Ideal to expose your metrics in applications that don't use Trio, without
    having to install aiohttp.

    For arguments see :func:`serve`.

    :rtype: ThreadedMetricsHTTPServer

    .. versionadded:: 26.2.0
    """
    q: queue.Queue[
        tuple[MetricsHTTPServer, trio.lowlevel.TrioToken, trio.CancelScope]
        | Exception
    ] = queue.Queue()

    async def main() -> None:
        async with trio.open_nursery() as nursery:
            try:
                http = await nursery.start(
                    partial(
                        serve,
                        addr=addr,
                        port=port,
                        ssl_ctx=ssl_ctx,
                        service_discovery=service_discovery,
                    )
                )
            except Exception as e:  # noqa: BLE001 -- all of them go to the caller
                # Starting failed. Let the caller know instead of letting the
                # nursery bury the error in an exception group.
                q.put(e)
                return

            q.put(
                (
                    http,
                    trio.lowlevel.current_trio_token(),
                    nursery.cancel_scope,
                )
            )

    def run() -> None:
        try:
            trio.run(main)
        finally:
            # If main() had something to say, it has said it by now and this
            # goes unread.  If it didn't -- because something nastier than an
            # Exception killed the thread -- we don't leave the caller waiting.
            q.put(RuntimeError("The server thread died before it was up."))

    t = threading.Thread(
        target=run, name="PrometheusAsyncTrioWebEndpoint", daemon=True
    )
    t.start()

    result = q.get()
    if isinstance(result, Exception):
        t.join()
        raise result

    return ThreadedMetricsHTTPServer(result[0], t, result[1], result[2])
