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
Service discovery for Trio's web exposure.
"""

from __future__ import annotations

import json

from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import h11
import trio

from ._http import TIMEOUT, next_event, send, serialize


if TYPE_CHECKING:
    from ..types import Deregisterer
    from .web import MetricsHTTPServer

__all__ = ["ConsulAgent"]


class ConsulAgent:
    """
    Service discovery via a local Consul agent.

    Pass as ``service_discovery`` into
    :func:`prometheus_async.trio.web.serve`/
    :func:`prometheus_async.trio.web.start_http_server_in_thread`.

    :param str name: Application name that is used for the name and the service
        ID if not set.
    :param str service_id: Consul Service ID.  If not set, *name* is used.
    :param tuple tags: Tags to use in Consul registration.
    :param str token: A consul access token.
    :param bool deregister: Whether to deregister when the HTTP server is
        closed.

    .. versionadded:: 26.2.0
    """

    def __init__(
        self,
        *,
        name: str = "app-metrics",
        service_id: str | None = None,
        tags: tuple[str, ...] = (),
        token: str | None = None,
        deregister: bool = True,
    ):
        self.name = name
        self.service_id = service_id or name
        self.tags = tags
        self.token = token
        self.deregister = deregister
        self.consul = _LocalConsulAgentClient(token=token)

    async def register(
        self, metrics_server: MetricsHTTPServer
    ) -> Deregisterer | None:
        """
        :return: A coroutine callable to deregister or ``None``.
        """
        registered = await self.consul.register_service(
            name=self.name,
            service_id=self.service_id,
            tags=list(self.tags) or None,
            metrics_server=metrics_server,
        )
        if not registered:
            return None

        async def deregister() -> None:
            if self.deregister is True:
                await self.consul.deregister_service(self.service_id)

        return deregister


class _LocalConsulAgentClient:
    """
    Minimal client to speak to a Consul agent on localhost:8500.
    """

    def __init__(
        self,
        token: str | None,
        *,
        host: str = "127.0.0.1",
        port: int = 8500,
    ) -> None:
        self.host = host
        self.port = port
        self.headers = [(b"Host", f"{host}:{port}".encode())]

        if token:
            self.headers.append((b"X-Consul-Token", token.encode()))

    async def _request(
        self, method: str, path: str, body: bytes = b""
    ) -> tuple[int, bytes]:
        """
        Do a request on a connection of its own and return the status code
        and the body of the response.
        """
        headers = [
            *self.headers,
            (b"Connection", b"close"),
            (b"Content-Length", str(len(body)).encode()),
        ]
        if body:
            headers.append((b"Content-Type", b"application/json"))

        events: list[h11.Event] = [
            h11.Request(method=method, target=path, headers=headers)
        ]
        if body:
            events.append(h11.Data(data=body))
        events.append(h11.EndOfMessage())

        conn = h11.Connection(h11.CLIENT)
        status = 0
        response = bytearray()

        # Everything we do has a timeout of its own, but the whole thing has to
        # be over in time, too, so a trickling Consul can't keep us here.
        with trio.fail_after(TIMEOUT):
            async with await trio.open_tcp_stream(self.host, self.port) as s:
                await send(s, serialize(conn, *events))

                while True:
                    event = await next_event(conn, s)
                    if isinstance(event, h11.Response):
                        status = event.status_code
                    elif isinstance(event, h11.Data):
                        response += event.data
                    elif isinstance(event, h11.EndOfMessage):
                        break

        return status, bytes(response)

    async def get_services(self) -> dict[str, Any]:
        _, body = await self._request("GET", "/v1/agent/services")

        return json.loads(body)

    async def register_service(
        self,
        name: str,
        service_id: str,
        tags: list[str] | None,
        metrics_server: MetricsHTTPServer,
    ) -> bool:
        """
        :return: Whether Consul has accepted the service.
        """
        status, _ = await self._request(
            "PUT",
            "/v1/agent/service/register",
            json.dumps(
                {
                    "Name": name,
                    "ID": service_id,
                    "Tags": tags,
                    "Address": metrics_server.socket.addr,
                    "Port": metrics_server.socket.port,
                    "Check": {"HTTP": metrics_server.url, "Interval": "10s"},
                }
            ).encode(),
        )

        return status == 200

    async def deregister_service(self, service_id: str) -> int:
        """
        :return: The HTTP status code of Consul's response.
        """
        status, _ = await self._request(
            "PUT", f"/v1/agent/service/deregister/{quote(service_id, safe='')}"
        )

        return status
