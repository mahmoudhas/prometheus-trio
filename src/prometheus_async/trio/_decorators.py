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
Decorators for Trio.
"""

from __future__ import annotations

import sys

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, overload

import trio

from wrapt import decorator


if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    from prometheus_client import Gauge

    from ..types import Incrementer, Observer, P, R, T


def _matches(
    error: BaseException,
    exc: type[BaseException] | tuple[type[BaseException], ...],
) -> bool:
    """
    Is *error* an instance of *exc* or does it contain one?

    Trio nurseries wrap errors of their tasks into exception groups, so we
    have to look inside of them.
    """
    if isinstance(error, BaseExceptionGroup):
        # Matches the group itself, as well as anything nested in it.
        return error.subgroup(exc) is not None

    return isinstance(error, exc)


@overload
def time(metric: Observer) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


@overload
def time(metric: Observer, awaitable: Awaitable[T]) -> Awaitable[T]: ...


def time(
    metric: Observer, awaitable: Awaitable[T] | None = None
) -> Awaitable[T] | Callable[[Callable[P, R]], Callable[P, R]]:
    r"""
    Call ``metric.observe(time)`` with the runtime in seconds.

    Works as a decorator on async functions and methods, as well as on
    awaitables like coroutine objects.

    The runtime is measured using :func:`trio.current_time`.
    Therefore, it follows a :class:`trio.testing.MockClock` in your tests.
    The measurement starts once the function is called or the awaitable is
    awaited, respectively.

    :returns: coroutine function (if decorator) or coroutine.

    .. versionadded:: 26.2.0
    """

    def observe(start_time: float) -> None:
        metric.observe(trio.current_time() - start_time)

    if awaitable is None:

        @decorator  # type: ignore[arg-type]
        async def time_decorator(
            wrapped: Callable[P, R],
            instance: Any,
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> R:
            start_time = trio.current_time()
            try:
                return await wrapped(*args, **kwargs)
            finally:
                observe(start_time)

        return time_decorator

    a = awaitable

    async def measure() -> T:
        start_time = trio.current_time()
        try:
            return await a
        finally:
            observe(start_time)

    return measure()


@overload
def count_exceptions(
    metric: Incrementer, *, exc: type[BaseException] = BaseException
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


@overload
def count_exceptions(
    metric: Incrementer,
    awaitable: Awaitable[T],
    *,
    exc: type[BaseException] = BaseException,
) -> Awaitable[T]: ...


def count_exceptions(
    metric: Incrementer,
    awaitable: Awaitable[T] | None = None,
    *,
    exc: type[BaseException] = BaseException,
) -> Callable[[Callable[P, R]], Callable[P, R]] | Awaitable[T]:
    r"""
    Call ``metric.inc()`` whenever *exc* is caught.

    Works as a decorator on async functions and methods, as well as on
    awaitables like coroutine objects.

    If an exception group is raised -- for example, because a task in a
    nursery of the instrumented code failed --, it is counted once if it
    either is an instance of *exc* itself, or contains at least one exception
    that is.

    .. note::

       :class:`trio.Cancelled` is a :class:`BaseException`, so the default of
       *exc* counts cancellations, too.
       Pass ``exc=Exception`` to count only actual errors.

    :returns: coroutine function (if decorator) or coroutine.

    .. versionadded:: 26.2.0
    """
    if awaitable is None:

        @decorator  # type: ignore[arg-type]
        async def count_decorator(
            wrapped: Callable[P, R],
            instance: Any,
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> R:
            try:
                return await wrapped(*args, **kwargs)
            except BaseException as e:
                if _matches(e, exc):
                    metric.inc()
                raise

        return count_decorator

    a = awaitable

    async def count_awaitable() -> T:
        try:
            return await a
        except BaseException as e:
            if _matches(e, exc):
                metric.inc()
            raise

    return count_awaitable()


@overload
def track_inprogress(
    metric: Gauge,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


@overload
def track_inprogress(
    metric: Gauge, awaitable: Awaitable[T]
) -> Awaitable[T]: ...


def track_inprogress(
    metric: Gauge, awaitable: Awaitable[T] | None = None
) -> Callable[[Callable[P, R]], Callable[P, R]] | Awaitable[T]:
    r"""
    Call ``metrics.inc()`` on entry and ``metric.dec()`` on exit.

    Works as a decorator on async functions and methods, as well as on
    awaitables like coroutine objects.

    In the latter case, the metric is increased once the awaitable is awaited
    -- not when this function is called -- so it can't leak if it never is.

    :returns: coroutine function (if decorator) or coroutine.

    .. versionadded:: 26.2.0
    """
    if awaitable is None:

        @decorator  # type: ignore[arg-type]
        async def track_decorator(
            wrapped: Callable[P, R],
            instance: Any,
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> R:
            metric.inc()
            try:
                return await wrapped(*args, **kwargs)
            finally:
                metric.dec()

        return track_decorator

    a = awaitable

    async def track_awaitable() -> T:
        metric.inc()
        try:
            return await a
        finally:
            metric.dec()

    return track_awaitable()
