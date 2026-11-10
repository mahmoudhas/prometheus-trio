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
Framework-independent helpers for exposing metrics.

Shared between the asyncio and the Trio web endpoints.  Must *not* import any
optional dependencies like aiohttp or Trio.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.openmetrics import exposition as openmetrics


if TYPE_CHECKING:
    from collections.abc import Callable


# What we serve on / -- useful for cheap health checks.
LANDING_PAGE = '<html><body><a href="/metrics">Metrics</a></body></html>'


def choose_generator(accept_header: str | None) -> tuple[Callable, str]:
    """
    Return the correct generate function according to *accept_header*.

    Default to the old style.
    """
    accept_header = accept_header or ""
    for accepted in accept_header.split(","):
        if accepted.split(";")[0].strip() == "application/openmetrics-text":
            return (
                openmetrics.generate_latest,
                openmetrics.CONTENT_TYPE_LATEST,
            )

    return generate_latest, CONTENT_TYPE_LATEST
