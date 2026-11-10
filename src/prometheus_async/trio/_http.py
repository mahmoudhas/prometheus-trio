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
The little bit of h11 plumbing that the web server and the Consul client share.

h11 is a sans-I/O HTTP/1.1 implementation: it only turns bytes into events and
events into bytes.  Moving them to and from a Trio stream is up to us.
"""

from __future__ import annotations

from typing import cast

import h11
import trio


# The most we read from a stream in one go.
MAX_RECV = 64 * 1024

# How long we wait for our peer to read from or write to us, in seconds.
TIMEOUT = 30.0


async def next_event(
    conn: h11.Connection, stream: trio.abc.ReceiveStream
) -> h11.Event | type[h11.PAUSED]:
    """
    Return the next event of *conn*, feeding it from *stream* as needed.

    :raises trio.TooSlowError: if the peer is silent for more than
        :data:`TIMEOUT`.
    """
    while True:
        event = conn.next_event()
        if event is not h11.NEED_DATA:
            # Type checkers can't narrow sentinel classes.
            return cast("h11.Event | type[h11.PAUSED]", event)

        with trio.fail_after(TIMEOUT):
            data = await stream.receive_some(MAX_RECV)

        # An empty bytes object signals EOF to h11.
        conn.receive_data(bytes(data))


def serialize(conn: h11.Connection, *events: h11.Event) -> bytes:
    """
    Turn *events* into one chunk of bytes, so we can send them in one go.
    """
    # Only ConnectionClosed produces None and we never send that.
    return b"".join(conn.send(event) or b"" for event in events)


async def send(stream: trio.abc.SendStream, data: bytes) -> None:
    """
    Send *data* on *stream*.

    :raises trio.TooSlowError: if the peer doesn't read it for more than
        :data:`TIMEOUT`.
    """
    with trio.fail_after(TIMEOUT):
        await stream.send_all(data)
