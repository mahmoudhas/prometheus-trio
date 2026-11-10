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

import http.client
import json
import logging
import socket
import ssl

from functools import partial
from typing import NamedTuple

import h11
import pytest
import trio
import trio.testing
import trustme

from prometheus_client import CONTENT_TYPE_LATEST, Counter
from prometheus_client.openmetrics import exposition as openmetrics
from prometheus_client.registry import REGISTRY

from prometheus_async import trio as ptrio
from prometheus_async._exposition import LANDING_PAGE
from prometheus_async.trio import _http, sd, web


OPENMETRICS_ACCEPT = (
    "application/openmetrics-text; version=0.0.1,"
    "text/plain;version=0.0.4;q=0.5,*/*;q=0.1"
)


class Reply(NamedTuple):
    status: int
    headers: dict[str, str]
    body: bytes


class Client:
    """
    A minimal HTTP client on top of h11.

    It's deliberately independent of the code we're testing.
    """

    def __init__(self, stream):
        self.stream = stream
        self.conn = h11.Connection(h11.CLIENT)

    async def send(self, *events):
        for event in events:
            await self.stream.send_all(self.conn.send(event))

    async def request(
        self, method="GET", target="/metrics", headers=(), body=b""
    ):
        headers = [("Host", "test"), *headers]
        if body:
            headers.append(("Content-Length", str(len(body))))
        events = [h11.Request(method=method, target=target, headers=headers)]
        if body:
            events.append(h11.Data(data=body))
        await self.send(*events, h11.EndOfMessage())

        return await self.reply()

    async def reply(self):
        status = None
        headers = {}
        body = bytearray()
        while True:
            event = self.conn.next_event()
            if event is h11.NEED_DATA:
                self.conn.receive_data(await self.stream.receive_some(65536))
            elif isinstance(event, h11.Response):
                status = event.status_code
                headers = {k.decode(): v.decode() for k, v in event.headers}
            elif isinstance(event, h11.Data):
                body += event.data
            else:
                assert isinstance(event, h11.EndOfMessage)
                break

        if (
            self.conn.our_state is h11.DONE
            and self.conn.their_state is h11.DONE
        ):
            self.conn.start_next_cycle()

        return Reply(status, headers, bytes(body))

    async def is_closed(self):
        """
        Has the server closed the connection?
        """
        with trio.move_on_after(5):
            return b"" == await self.stream.receive_some(65536)

        return False


async def fetch(server, *args, **kw):
    """
    Open a connection to *server*, do one request, and return the reply.
    """
    async with await trio.open_tcp_stream(*server.socket) as stream:
        return await Client(stream).request(*args, **kw)


async def start(nursery, **kw):
    kw.setdefault("addr", "127.0.0.1")

    return await nursery.start(partial(web.serve, **kw))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))

        return s.getsockname()[1]


class FakeSD:
    """
    Fake Service Discovery.
    """

    def __init__(self):
        self.registered_ms = None
        self.deregistered = False

    async def register(self, metrics_server):
        self.registered_ms = metrics_server

        async def deregister():
            # Would be cancelled right away if it wasn't shielded.
            await trio.lowlevel.checkpoint()
            self.deregistered = True

        return deregister


class BrokenCollector:
    def describe(self):
        # Without this, registering would collect and thus fail right away.
        return []

    def collect(self):
        raise ValueError("I'm broken.")


class TestMetricsHTTPServer:
    @pytest.mark.parametrize(
        ("addr", "https", "url"),
        [
            ("127.0.0.1", False, "http://127.0.0.1:42/"),
            ("127.0.0.1", True, "https://127.0.0.1:42/"),
            ("::1", False, "http://[::1]:42/"),
            ("::", True, "https://[::]:42/"),
        ],
    )
    def test_url(self, addr, https, url):
        """
        The URL of a MetricsHTTPServer is correctly computed.
        """
        assert url == web.MetricsHTTPServer(web.Socket(addr, 42), https).url


@pytest.mark.trio
class TestServe:
    async def test_metrics_old(self, nursery):
        """
        Returns a response with the current stats in the old format.
        """
        Counter("test_server_stats_total", "cnt").inc()
        server = await start(nursery)

        rv = await fetch(server)

        assert 200 == rv.status
        assert CONTENT_TYPE_LATEST == rv.headers["content-type"]
        assert str(len(rv.body)) == rv.headers["content-length"]
        assert "date" in rv.headers
        assert (
            "# HELP test_server_stats_total cnt\n"
            "# TYPE test_server_stats_total counter\n"
            "test_server_stats_total 1.0\n"
        ) in rv.body.decode()

    async def test_metrics_openmetrics(self, nursery):
        """
        Returns a response with the current stats in the open metrics format
        if the client asks for it.
        """
        Counter("test_server_stats_total", "cnt").inc()
        server = await start(nursery)

        rv = await fetch(server, headers=[("Accept", OPENMETRICS_ACCEPT)])

        body = rv.body.decode()

        assert 200 == rv.status
        assert openmetrics.CONTENT_TYPE_LATEST == rv.headers["content-type"]
        assert (
            "# HELP test_server_stats cnt\n"
            "# TYPE test_server_stats counter\n"
            "test_server_stats_total 1.0\n"
        ) in body
        assert body.endswith("EOF\n")

    async def test_landing_page(self, nursery):
        """
        / links to the metrics.
        """
        server = await start(nursery)

        rv = await fetch(server, target="/")

        assert 200 == rv.status
        assert "text/html; charset=utf-8" == rv.headers["content-type"]
        assert LANDING_PAGE.encode() == rv.body

    async def test_head(self, nursery):
        """
        HEAD has the headers of GET, but no body.
        """
        Counter("test_head_total", "cnt").inc()
        server = await start(nursery)

        head = await fetch(server, method="HEAD")
        get = await fetch(server)

        assert 200 == head.status
        assert b"" == head.body
        assert get.headers["content-length"] == head.headers["content-length"]
        assert get.headers["content-type"] == head.headers["content-type"]

    async def test_query_is_ignored(self, nursery):
        """
        Query strings don't matter.
        """
        server = await start(nursery)

        rv = await fetch(server, target="/metrics?name[]=foo")

        assert 200 == rv.status

    @pytest.mark.parametrize(
        "target", ["/nope", "/metrics/", "/metric", "*", "//metrics"]
    )
    async def test_not_found(self, nursery, target):
        """
        Unknown paths are a 404.
        """
        server = await start(nursery)

        rv = await fetch(server, target=target)

        assert 404 == rv.status
        assert b"404: Not Found" == rv.body

    @pytest.mark.parametrize("target", ["/", "/metrics"])
    async def test_method_not_allowed(self, nursery, target):
        """
        Only GET and HEAD are allowed.
        """
        server = await start(nursery)

        rv = await fetch(server, method="DELETE", target=target)

        assert 405 == rv.status
        assert "GET, HEAD" == rv.headers["allow"]

    async def test_keep_alive(self, nursery):
        """
        Connections are reused.
        """
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            client = Client(stream)

            assert 200 == (await client.request()).status
            assert 200 == (await client.request(target="/")).status
            assert 404 == (await client.request(target="/nope")).status

    async def test_connection_close(self, nursery):
        """
        The server closes the connection if the client asks for it.
        """
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            client = Client(stream)

            rv = await client.request(headers=[("Connection", "close")])

            assert 200 == rv.status
            assert "close" == rv.headers["connection"]
            assert await client.is_closed()

    async def test_pipelining(self, nursery):
        """
        Requests that arrive together are answered one after the other.
        """
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            # h11 can't pipeline as a client, so we do it by hand.
            await stream.send_all(
                b"GET / HTTP/1.1\r\nHost: test\r\n\r\n"
                b"GET /nope HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
            )
            data = b""
            while chunk := await stream.receive_some():
                data += chunk

        first, second = data.split(b"HTTP/1.1 ")[1:]

        assert first.startswith(b"200")
        assert second.startswith(b"404")

    async def test_body_is_discarded(self, nursery):
        """
        If a client sends us a body, we read it so the connection stays
        usable.
        """
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            client = Client(stream)

            rv = await client.request(method="POST", body=b"x" * 1000)

            assert 405 == rv.status
            assert 200 == (await client.request()).status

    async def test_body_too_big(self, nursery, monkeypatch):
        """
        If the body is too big, the connection is closed.
        """
        monkeypatch.setattr(web, "_MAX_BODY", 100)
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            client = Client(stream)
            await client.send(
                h11.Request(
                    method="POST",
                    target="/metrics",
                    headers=[("Host", "test"), ("Content-Length", "10000")],
                ),
                h11.Data(data=b"x" * 200),
            )

            assert await client.is_closed()

        # The server is just fine.
        assert 200 == (await fetch(server)).status

    @pytest.mark.parametrize(
        ("raw", "status"),
        [
            (b"NONSENSE\r\n\r\n", 400),
            (b"GET /metrics HTTP/1.1\r\n\r\n", 400),  # missing Host
            # h11 limits the size of incomplete requests only, so this has to
            # be bigger than whatever we read in one go.
            (
                b"GET /metrics HTTP/1.1\r\nHost: test\r\nX: "
                + b"x" * 100_000
                + b"\r\n\r\n",
                431,
            ),
        ],
        ids=["garbage", "no-host", "too-big"],
    )
    async def test_bad_request(self, nursery, raw, status):
        """
        Broken requests are answered with an error, and the connection is
        closed.  The server survives.
        """
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            await stream.send_all(raw)
            client = Client(stream)
            rv = await client.reply()

            assert status == rv.status
            assert "close" == rv.headers["connection"]
            assert f"{status}: Bad Request".encode() == rv.body
            assert await client.is_closed()

        assert 200 == (await fetch(server)).status

    async def test_client_vanishes(self, nursery, caplog):
        """
        Clients that disappear don't bother us.
        """
        caplog.set_level(logging.WARNING)
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            await stream.send_all(b"GET /metrics HTTP/1.1\r\nHost: t")

        assert 200 == (await fetch(server)).status
        assert [] == caplog.records

    async def test_broken_collector(self, nursery, caplog):
        """
        If generating the metrics fails, it's a 500, and the server and the
        connection survive.
        """
        REGISTRY.register(BrokenCollector())
        server = await start(nursery)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            client = Client(stream)
            rv = await client.request()

            assert 500 == rv.status
            assert b"500: Internal Server Error" == rv.body
            assert 200 == (await client.request(target="/")).status

        assert "Generating the metrics failed." == caplog.records[0].message

    async def test_socket_and_url(self, nursery):
        """
        The server knows where it listens.
        """
        server = await start(nursery)

        assert isinstance(server, web.MetricsHTTPServer)
        assert "127.0.0.1" == server.socket.addr
        assert 0 != server.socket.port
        assert False is server.https
        assert False is server.is_registered
        assert f"http://127.0.0.1:{server.socket.port}/" == server.url

    async def test_all_interfaces(self, nursery):
        """
        If we don't say where to listen, we listen everywhere.
        """
        server = await nursery.start(web.serve)

        assert server.socket.addr in ("0.0.0.0", "::")

        host = {"0.0.0.0": "127.0.0.1", "::": "::1"}[server.socket.addr]
        async with await trio.open_tcp_stream(host, server.socket.port) as s:
            assert 200 == (await Client(s).request()).status

    async def test_explicit_port(self, nursery):
        """
        We listen on the port we're told to.
        """
        port = free_port()

        server = await start(nursery, port=port)

        assert port == server.socket.port
        assert 200 == (await fetch(server)).status

    async def test_system_port_single_listener(self, nursery, monkeypatch):
        """
        If the system picks the port, we listen on one address family only,
        because the system would pick a different port for each.
        """
        opened = []
        real = trio.open_tcp_listeners

        async def fake(port, *, host=None):
            # What we get on a system with IPv4 and IPv6.
            listeners = [
                *await real(0, host="127.0.0.1"),
                *await real(0, host="127.0.0.1"),
            ]
            opened.extend(listeners)

            return listeners

        monkeypatch.setattr(trio, "open_tcp_listeners", fake)

        server = await start(nursery)

        assert opened[0].socket.getsockname()[1] == server.socket.port
        assert -1 != opened[0].socket.fileno()
        assert -1 == opened[1].socket.fileno()  # closed

    async def test_is_stopped_by_cancellation(self):
        """
        Cancelling stops the server and closes its port.
        """
        async with trio.open_nursery() as nursery:
            server = await start(nursery)
            assert 200 == (await fetch(server)).status

            nursery.cancel_scope.cancel()

        with pytest.raises(OSError):
            await trio.open_tcp_stream(*server.socket)

    async def test_direct(self):
        """
        You can also just await it.
        """
        with trio.move_on_after(0.1) as scope:
            await web.serve(addr="127.0.0.1")

        assert scope.cancelled_caught

    async def test_port_in_use(self, nursery):
        """
        If it can't listen, you get to know.
        """
        server = await start(nursery)

        with pytest.raises(OSError):
            await start(nursery, port=server.socket.port)


@pytest.mark.trio
class TestServiceDiscovery:
    async def test_register_and_deregister(self):
        """
        Starting registers the server, stopping deregisters it -- even though
        that happens while we're being cancelled.
        """
        sd = FakeSD()

        async with trio.open_nursery() as nursery:
            server = await start(nursery, service_discovery=sd)

            assert server.is_registered
            assert sd.registered_ms is server
            assert not sd.deregistered

            nursery.cancel_scope.cancel()

        assert sd.deregistered

    async def test_register_fails(self, nursery):
        """
        If registering fails, we get the error and nothing keeps running.
        """
        ports = []

        class Failing:
            async def register(self, metrics_server):
                ports.append(metrics_server.socket.port)
                raise RuntimeError("no registering for you")

        with pytest.raises(RuntimeError, match="no registering for you"):
            await start(nursery, service_discovery=Failing())

        with pytest.raises(OSError):
            await trio.open_tcp_stream("127.0.0.1", ports[0])

    async def test_no_deregister_if_not_registered(self):
        """
        If service discovery says it didn't register us, we don't deregister.
        """

        class Declining:
            async def register(self, metrics_server):
                return None

        async with trio.open_nursery() as nursery:
            server = await start(nursery, service_discovery=Declining())

            assert not server.is_registered

            nursery.cancel_scope.cancel()

    async def test_deregister_fails(self, caplog):
        """
        If deregistering fails, we log it and shut down anyway.
        """

        class Failing:
            async def register(self, metrics_server):
                async def deregister():
                    raise RuntimeError("can't deregister")

                return deregister

        async with trio.open_nursery() as nursery:
            server = await start(nursery, service_discovery=Failing())

            nursery.cancel_scope.cancel()

        assert "Deregistering the server failed." == caplog.records[0].message
        with pytest.raises(OSError):
            await trio.open_tcp_stream(*server.socket)

    async def test_deregister_is_bounded(self, autojump_clock, monkeypatch):
        """
        Deregistering can't keep us from shutting down forever.
        """

        class Hanging:
            async def register(self, metrics_server):
                return trio.sleep_forever

        async with trio.open_nursery() as nursery:
            await start(nursery, service_discovery=Hanging())

            t0 = trio.current_time()
            nursery.cancel_scope.cancel()

        assert web._CLEANUP_TIMEOUT == trio.current_time() - t0


@pytest.mark.trio
class TestHandleConnection:
    """
    Tests that need to control time and thus work with in-memory streams.
    """

    async def test_idle(self, nursery, autojump_clock):
        """
        Connections that are idle for too long are closed.
        """
        client, server = trio.testing.memory_stream_pair()
        nursery.start_soon(web._handle_connection, server)

        t0 = trio.current_time()

        assert b"" == await client.receive_some()
        assert _http.TIMEOUT == trio.current_time() - t0

    async def test_slow_request(self, nursery, autojump_clock):
        """
        Clients that don't finish their request in time are dropped.
        """
        client, server = trio.testing.memory_stream_pair()
        nursery.start_soon(web._handle_connection, server)

        await client.send_all(b"GET /metrics HTTP/1.1\r\nHost: t")

        assert b"" == await client.receive_some()

    async def test_slow_reader(self, nursery, autojump_clock, caplog):
        """
        Clients that don't read our response are dropped.
        """
        caplog.set_level(logging.WARNING)
        client, server = trio.testing.lockstep_stream_pair()
        done = trio.Event()

        async def run():
            await web._handle_connection(server)
            done.set()

        nursery.start_soon(run)

        await client.send_all(b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n")
        # We don't read the response.

        await done.wait()
        assert [] == caplog.records

    async def test_linger(self, nursery, autojump_clock):
        """
        After we're done, the client has a moment to close the connection.
        """
        client, server = trio.testing.memory_stream_pair()
        done = trio.Event()

        async def run():
            await web._handle_connection(server)
            done.set()

        nursery.start_soon(run)

        t0 = trio.current_time()
        await client.send_all(
            b"GET /nope HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
        )
        await done.wait()

        assert web._LINGER == trio.current_time() - t0

    async def test_unexpected_error(self, monkeypatch, caplog, autojump_clock):
        """
        Unexpected errors are logged, but never escape: they would take down
        the entire server.
        """

        async def boom(stream):
            raise RuntimeError("what?")

        monkeypatch.setattr(web, "_serve", boom)
        _, server = trio.testing.memory_stream_pair()

        await web._handle_connection(server)

        assert "Serving a request failed unexpectedly." == (
            caplog.records[0].message
        )
        assert RuntimeError is caplog.records[0].exc_info[0]


@pytest.fixture(name="certs", scope="module")
def _certs():
    """
    A TLS server context and a client context that trusts it.
    """
    ca = trustme.CA()
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ca.issue_cert("127.0.0.1").configure_cert(server_ctx)
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)

    return server_ctx, client_ctx


@pytest.mark.trio
class TestTLS:
    async def test_https(self, nursery, certs):
        """
        With a TLS context, we serve HTTPS.
        """
        Counter("test_https_total", "cnt").inc()
        server_ctx, client_ctx = certs
        server = await start(nursery, ssl_ctx=server_ctx)

        assert True is server.https
        assert f"https://127.0.0.1:{server.socket.port}/" == server.url

        async with await trio.open_ssl_over_tcp_stream(
            *server.socket, ssl_context=client_ctx, https_compatible=True
        ) as stream:
            client = Client(stream)
            rv = await client.request()

            assert 200 == rv.status
            assert b"test_https_total 1.0" in rv.body
            # It's kept alive, too.
            assert 200 == (await client.request(target="/")).status

    async def test_plain_http_client(self, nursery, certs, caplog):
        """
        Clients that don't speak TLS don't bother us.
        """
        caplog.set_level(logging.WARNING)
        server_ctx, client_ctx = certs
        server = await start(nursery, ssl_ctx=server_ctx)

        async with await trio.open_tcp_stream(*server.socket) as stream:
            await stream.send_all(
                b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n"
            )

            assert b"" == await stream.receive_some()

        async with await trio.open_ssl_over_tcp_stream(
            *server.socket, ssl_context=client_ctx, https_compatible=True
        ) as stream:
            assert 200 == (await Client(stream).request()).status

        assert [] == caplog.records


class TestThreaded:
    @pytest.mark.parametrize("use_sd", [False, True])
    def test_start_in_thread(self, use_sd):
        """
        Threaded version starts and exits properly, passes on service
        discovery.
        """
        fake_sd = FakeSD() if use_sd else None
        Counter("test_start_http_server_in_thread_total", "cnt").inc()

        t = web.start_http_server_in_thread(
            addr="127.0.0.1", service_discovery=fake_sd
        )

        assert isinstance(t, web.ThreadedMetricsHTTPServer)
        assert "PrometheusAsyncTrioWebEndpoint" == t._thread.name
        assert t.url.startswith("http")
        assert False is t.https
        assert t.is_registered is use_sd
        if use_sd:
            assert fake_sd.registered_ms is t._http_server

        s = t.socket
        h = http.client.HTTPConnection(s.addr, port=s[1])
        h.request("GET", "/metrics")
        rsp = h.getresponse()
        body = rsp.read().decode()
        rsp.close()
        h.close()

        assert "HELP test_start_http_server_in_thread_total cnt" in body

        t.close()

        assert False is t._thread.is_alive()
        if use_sd:
            assert fake_sd.deregistered

    def test_close_twice(self):
        """
        Closing a closed server is fine.
        """
        t = web.start_http_server_in_thread(addr="127.0.0.1")

        t.close()
        t.close()

        assert False is t._thread.is_alive()

    def test_start_fails(self):
        """
        If starting the server fails, the caller gets the original error.
        """
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            s.listen()

            with pytest.raises(OSError):
                web.start_http_server_in_thread(
                    addr="127.0.0.1", port=s.getsockname()[1]
                )

    def test_register_fails(self):
        """
        If registering fails, the caller gets the original error.
        """

        class Failing:
            async def register(self, metrics_server):
                raise RuntimeError("no registering for you")

        with pytest.raises(RuntimeError, match="no registering for you"):
            web.start_http_server_in_thread(
                addr="127.0.0.1", service_discovery=Failing()
            )

    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_thread_dies(self):
        """
        If the thread dies from something that isn't an Exception, the caller
        isn't left waiting forever.
        """

        class Dying:
            async def register(self, metrics_server):
                raise SystemExit

        with pytest.raises(RuntimeError, match="died before it was up"):
            web.start_http_server_in_thread(
                addr="127.0.0.1", service_discovery=Dying()
            )

    @pytest.mark.filterwarnings(
        "ignore::pytest.PytestUnhandledThreadExceptionWarning"
    )
    def test_close_after_crash(self, monkeypatch):
        """
        If the server has crashed, closing it still works.
        """

        async def crash(*args, **kw):
            raise RuntimeError("crash")

        monkeypatch.setattr(trio, "serve_listeners", crash)

        t = web.start_http_server_in_thread(addr="127.0.0.1")
        t._thread.join()

        t.close()


class FakeConsul:
    """
    A Consul agent that records the requests it gets.
    """

    def __init__(self, status=200, interim=False, body=b"{}"):
        self.status = status
        self.interim = interim
        self.body = body
        self.requests = []

    async def __call__(self, stream):
        conn = h11.Connection(h11.SERVER)
        request = None
        body = bytearray()
        while True:
            event = conn.next_event()
            if event is h11.NEED_DATA:
                conn.receive_data(await stream.receive_some(65536))
            elif isinstance(event, h11.Request):
                request = event
            elif isinstance(event, h11.Data):
                body += event.data
            else:
                break

        self.requests.append(
            SimpleRequest(
                request.method.decode(),
                request.target.decode(),
                {k.decode(): v.decode() for k, v in request.headers},
                bytes(body),
            )
        )

        events = []
        if self.interim:
            events.append(
                h11.InformationalResponse(status_code=102, headers=[])
            )
        events += [
            h11.Response(
                status_code=self.status,
                headers=[("Content-Length", str(len(self.body)))],
            ),
            h11.Data(data=self.body),
            h11.EndOfMessage(),
        ]
        await stream.send_all(_http.serialize(conn, *events))


class SimpleRequest(NamedTuple):
    method: str
    target: str
    headers: dict[str, str]
    body: bytes


async def start_consul(nursery, **kw):
    """
    Start a fake Consul agent and return it along with a client that talks to
    it.
    """
    consul = FakeConsul(**kw)
    listeners = await nursery.start(
        partial(trio.serve_tcp, consul, 0, host="127.0.0.1")
    )
    client = sd._LocalConsulAgentClient(
        token=None, port=listeners[0].socket.getsockname()[1]
    )

    return consul, client


@pytest.mark.trio
class TestConsulAgent:
    @pytest.mark.parametrize("deregister", [True, False])
    async def test_integration(self, nursery, deregister):
        """
        Start a service, register it, close it, verify it's deregistered (if
        we're supposed to).
        """
        consul, client = await start_consul(nursery)
        ca = ptrio.sd.ConsulAgent(
            name="test-metrics",
            service_id="the-id",
            tags=("foo", "bar"),
            deregister=deregister,
        )
        ca.consul = client

        async with trio.open_nursery() as server_nursery:
            server = await start(server_nursery, service_discovery=ca)

            assert server.is_registered

            [register] = consul.requests

            assert "PUT" == register.method
            assert "/v1/agent/service/register" == register.target
            assert "application/json" == register.headers["content-type"]
            assert "x-consul-token" not in register.headers
            assert {
                "Name": "test-metrics",
                "ID": "the-id",
                "Tags": ["foo", "bar"],
                "Address": "127.0.0.1",
                "Port": server.socket.port,
                "Check": {"HTTP": server.url, "Interval": "10s"},
            } == json.loads(register.body)

            server_nursery.cancel_scope.cancel()

        if deregister:
            assert [
                ("PUT", "/v1/agent/service/register"),
                ("PUT", "/v1/agent/service/deregister/the-id"),
            ] == [(r.method, r.target) for r in consul.requests]
            assert b"" == consul.requests[1].body
            assert "0" == consul.requests[1].headers["content-length"]
        else:
            assert 1 == len(consul.requests)

    async def test_no_tags(self, nursery):
        """
        Without tags, none are sent.
        """
        consul, client = await start_consul(nursery)
        ca = ptrio.sd.ConsulAgent()
        ca.consul = client

        async with trio.open_nursery() as server_nursery:
            await start(server_nursery, service_discovery=ca)

            assert None is json.loads(consul.requests[0].body)["Tags"]
            assert "app-metrics" == json.loads(consul.requests[0].body)["ID"]

            server_nursery.cancel_scope.cancel()

    async def test_none_if_register_fails(self, nursery):
        """
        If register fails, return None.
        """
        _, client = await start_consul(nursery, status=400)
        ca = ptrio.sd.ConsulAgent()
        ca.consul = client

        class Server:
            socket = web.Socket("127.0.0.1", 12345)
            url = "http://127.0.0.1:12345/metrics"

        assert None is (await ca.register(Server()))

    async def test_service_id_is_quoted(self, nursery):
        """
        Service IDs can't mess with the path.
        """
        consul, client = await start_consul(nursery)

        assert 200 == await client.deregister_service("a/b c?d")

        assert "/v1/agent/service/deregister/a%2Fb%20c%3Fd" == (
            consul.requests[0].target
        )

    async def test_token(self, nursery):
        """
        If a token is passed, "X-Consul-Token" is sent.
        """
        consul, plain = await start_consul(nursery)
        client = sd._LocalConsulAgentClient(token="token42", port=plain.port)

        await client.get_services()

        assert "token42" == consul.requests[0].headers["x-consul-token"]

    async def test_get_services(self, nursery):
        """
        Services are returned as a dict.
        """
        _, client = await start_consul(
            nursery, body=b'{"the-id": {"Service": "test-metrics"}}'
        )

        assert {"the-id": {"Service": "test-metrics"}} == (
            await client.get_services()
        )

    async def test_interim_responses(self, nursery):
        """
        Interim responses are skipped.
        """
        _, client = await start_consul(nursery, interim=True)

        assert {} == await client.get_services()

    async def test_agent_is_missing(self):
        """
        If there's no agent, you get an OSError.
        """
        ca = ptrio.sd.ConsulAgent()
        ca.consul = sd._LocalConsulAgentClient(token=None, port=free_port())

        with pytest.raises(OSError):
            await ca.register(
                web.MetricsHTTPServer(web.Socket("127.0.0.1", 1), False)
            )

    async def test_agent_is_silent(self, nursery, monkeypatch):
        """
        If the agent doesn't answer, we give up eventually.
        """
        monkeypatch.setattr(_http, "TIMEOUT", 0.1)
        monkeypatch.setattr(sd, "TIMEOUT", 0.1)

        async def silent(stream):
            await trio.sleep_forever()

        listeners = await nursery.start(
            partial(trio.serve_tcp, silent, 0, host="127.0.0.1")
        )
        client = sd._LocalConsulAgentClient(
            token=None, port=listeners[0].socket.getsockname()[1]
        )

        with pytest.raises(trio.TooSlowError):
            await client.get_services()


class TestLocalConsulAgentClient:
    def test_sets_headers(self):
        """
        If a token is passed, "X-Consul-Token" header is set.
        """
        con = sd._LocalConsulAgentClient(token="token42")

        assert (b"X-Consul-Token", b"token42") in con.headers
        assert (b"Host", b"127.0.0.1:8500") in con.headers

    def test_no_token(self):
        """
        If there's no token, there's no header.
        """
        con = sd._LocalConsulAgentClient(token=None)

        assert [(b"Host", b"127.0.0.1:8500")] == con.headers
