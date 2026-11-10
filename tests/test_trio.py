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

import importlib
import inspect
import sys

import pytest
import trio
import trio.lowlevel
import wrapt

import prometheus_async

from prometheus_async import trio as ptrio


if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup, ExceptionGroup


ve = ValueError("foo")


async def coro():
    await trio.sleep(1)
    return 42


async def raiser():
    await trio.sleep(1)
    raise ve


class C:
    async def coro(self):
        await trio.sleep(1)
        return 42

    async def raiser(self):
        await trio.sleep(1)
        raise ve


async def fail_in_nursery(*errors):
    """
    Raise *errors* from tasks in a nursery, like real-world code does.  They
    end up in an exception group.
    """

    async def fail(error):
        # No checkpoint before raising!  Trio runs tasks in random order and
        # the first failure cancels the nursery.  Siblings that get to a
        # checkpoint after that are cancelled and their errors never happen.
        raise error

    # Before Trio 0.25, a single error was not wrapped by default.
    async with trio.open_nursery(strict_exception_groups=True) as nursery:
        for error in errors:
            nursery.start_soon(fail, error)


@pytest.mark.trio
class TestTime:
    @pytest.mark.parametrize(
        "coro", [coro, C().coro], ids=["function", "method"]
    )
    async def test_still_coroutine_function(
        self, fake_observer, coro, autojump_clock
    ):
        """
        It's ensured that a decorated function still passes as a coroutine
        function.
        """
        func = ptrio.time(fake_observer)(coro)
        new_coro = func()

        assert inspect.iscoroutine(new_coro)
        assert inspect.iscoroutinefunction(func)

        await new_coro

    async def test_decorator_sync(self, fake_observer, autojump_clock):
        """
        time works with functions that don't checkpoint.
        """

        @ptrio.time(fake_observer)
        async def func():
            return 42

        assert 42 == await func()
        assert [0] == fake_observer._observed

    @pytest.mark.parametrize(
        "coro", [coro, C().coro], ids=["function", "method"]
    )
    async def test_decorator(self, fake_observer, coro, autojump_clock):
        """
        time observes the time as it passes according to Trio's clock.
        """
        func = ptrio.time(fake_observer)(coro)

        rv = func()

        assert inspect.iscoroutine(rv)
        assert [] == fake_observer._observed

        rv = await rv

        assert [1] == fake_observer._observed
        assert 42 == rv

    @pytest.mark.parametrize(
        "coro", [raiser, C().raiser], ids=["function", "method"]
    )
    async def test_decorator_exc(self, fake_observer, coro, autojump_clock):
        """
        Does not swallow exceptions.
        """
        func = ptrio.time(fake_observer)(coro)

        with pytest.raises(ValueError) as e:
            await func()

        assert ve is e.value
        assert [1] == fake_observer._observed

    async def test_decorator_cancelled(self, fake_observer, autojump_clock):
        """
        The time is observed up to the point of a cancellation.
        """
        func = ptrio.time(fake_observer)(trio.sleep_forever)

        with trio.move_on_after(3) as scope:
            await func()

        assert scope.cancelled_caught
        assert [3] == fake_observer._observed

    async def test_decorator_start_soon(self, fake_observer, autojump_clock):
        """
        Trio accepts the decorated function as an async function.
        """
        func = ptrio.time(fake_observer)(coro)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(func)

        assert [1] == fake_observer._observed

    async def test_awaitable(self, fake_observer, autojump_clock):
        """
        time works with awaitables like coroutine objects.
        """
        c = ptrio.time(fake_observer, coro())

        assert [] == fake_observer._observed

        assert 42 == await c
        assert [1] == fake_observer._observed

    async def test_awaitable_starts_when_awaited(
        self, fake_observer, autojump_clock
    ):
        """
        Awaitables are not running until we await them, so we don't count the
        time before that.
        """
        c = ptrio.time(fake_observer, coro())

        await trio.sleep(10)
        await c

        assert [1] == fake_observer._observed

    async def test_awaitable_exc(self, fake_observer, autojump_clock):
        """
        Does not swallow exceptions.
        """
        c = ptrio.time(fake_observer, raiser())

        with pytest.raises(ValueError) as e:
            await c

        assert ve is e.value
        assert [1] == fake_observer._observed

    async def test_decorator_wrapt(self, fake_observer, autojump_clock):
        """
        Our decorator doesn't break wrapt-based decorators further down.

        A naive decorator using functools.wraps would add `self` to args and
        zero out `instance`. Potentially breaking signatures.
        """
        before_sig = before_instance = before_kw = before_args = None
        after_sig = after_instance = after_kw = after_args = None

        @wrapt.decorator
        def before(wrapped, instance, args, kw):
            assert instance is not None
            nonlocal before_args, before_kw, before_instance, before_sig

            before_args = args
            before_kw = kw
            before_instance = instance
            before_sig = inspect.signature(wrapped)

            return wrapped(*args, **kw)

        @wrapt.decorator
        def after(wrapped, instance, args, kw):
            assert instance is not None
            nonlocal after_args, after_kw, after_instance, after_sig

            after_args = args
            after_kw = kw
            after_instance = instance
            after_sig = inspect.signature(wrapped)

            return wrapped(*args, **kw)

        class C:
            @after
            @ptrio.time(fake_observer)
            @before
            async def coro(self, x):
                await trio.sleep(1)
                return str(x)

        i1 = C()
        i2 = C()

        assert "5" == await i1.coro(5)
        assert "42" == await i2.coro(42)

        assert after_instance is before_instance
        assert after_instance is not None
        assert after_args == before_args is not None
        assert after_kw == before_kw is not None
        assert before_sig == after_sig
        assert [1, 1] == fake_observer._observed


@pytest.mark.trio
class TestCountExceptions:
    async def test_decorator_no_exc(self, fake_counter):
        """
        If no exception is raised, the counter does not change.
        """

        @ptrio.count_exceptions(fake_counter)
        async def func():
            await trio.lowlevel.checkpoint()
            return 42

        assert 42 == await func()
        assert 0 == fake_counter._val

    async def test_decorator_wrong_exc(self, fake_counter):
        """
        If a wrong exception is raised, the counter does not change.
        """

        @ptrio.count_exceptions(fake_counter, exc=TypeError)
        async def func():
            await trio.lowlevel.checkpoint()
            raise ValueError

        with pytest.raises(ValueError):
            await func()

        assert 0 == fake_counter._val

    async def test_decorator_exc(self, fake_counter):
        """
        If the correct exception is raised, count it.
        """

        @ptrio.count_exceptions(fake_counter, exc=ValueError)
        async def func():
            await trio.lowlevel.checkpoint()
            raise ValueError

        with pytest.raises(ValueError):
            await func()

        assert 1 == fake_counter._val

    async def test_decorator_default_counts_all(self, fake_counter):
        """
        By default, everything is counted.
        """

        @ptrio.count_exceptions(fake_counter)
        async def func():
            await trio.lowlevel.checkpoint()
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            await func()

        assert 1 == fake_counter._val

    async def test_decorator_counts_cancellation(self, fake_counter):
        """
        Cancelled is a BaseException, so it's counted by default.
        """

        @ptrio.count_exceptions(fake_counter)
        async def func():
            await trio.sleep_forever()

        with trio.move_on_after(0) as scope:
            await func()

        assert scope.cancelled_caught
        assert 1 == fake_counter._val

    async def test_decorator_ignores_cancellation(self, fake_counter):
        """
        If asked to count only Exceptions, cancellations are ignored.
        """

        @ptrio.count_exceptions(fake_counter, exc=Exception)
        async def func():
            await trio.sleep_forever()

        with trio.move_on_after(0) as scope:
            await func()

        assert scope.cancelled_caught
        assert 0 == fake_counter._val

    async def test_decorator_group(self, fake_counter):
        """
        Errors from nurseries are wrapped into exception groups.  They are
        looked into and counted once, even if there are more of them.
        """

        @ptrio.count_exceptions(fake_counter, exc=ValueError)
        async def func():
            await fail_in_nursery(ValueError(), ValueError(), TypeError())

        with pytest.raises(ExceptionGroup):
            await func()

        assert 1 == fake_counter._val

    async def test_decorator_nested_group(self, fake_counter):
        """
        Exceptions are found, no matter how deep they are nested.
        """

        @ptrio.count_exceptions(fake_counter, exc=ValueError)
        async def func():
            async with trio.open_nursery(strict_exception_groups=True) as n:
                n.start_soon(fail_in_nursery, ValueError())

        with pytest.raises(ExceptionGroup):
            await func()

        assert 1 == fake_counter._val

    async def test_decorator_group_wrong_exc(self, fake_counter):
        """
        If there's nothing to count in a group, the counter does not change.
        """

        @ptrio.count_exceptions(fake_counter, exc=ValueError)
        async def func():
            await fail_in_nursery(TypeError(), KeyError())

        with pytest.raises(ExceptionGroup):
            await func()

        assert 0 == fake_counter._val

    async def test_decorator_group_default(self, fake_counter):
        """
        Groups are exceptions, too.
        """

        @ptrio.count_exceptions(fake_counter)
        async def func():
            await fail_in_nursery(TypeError())

        with pytest.raises(ExceptionGroup):
            await func()

        assert 1 == fake_counter._val

    async def test_decorator_group_exception(self, fake_counter):
        """
        A group that contains something that isn't an Exception isn't one
        either.  But if there's an actual error in it, it's counted anyway.
        """

        @ptrio.count_exceptions(fake_counter, exc=Exception)
        async def func():
            raise BaseExceptionGroup("mixed", [KeyboardInterrupt(), ve])

        with pytest.raises(BaseExceptionGroup):
            await func()

        assert 1 == fake_counter._val

    async def test_awaitable_no_exc(self, fake_counter, autojump_clock):
        """
        If no exception is raised, the counter does not change.
        """
        assert 42 == await ptrio.count_exceptions(fake_counter, coro())
        assert 0 == fake_counter._val

    async def test_awaitable_wrong_exc(self, fake_counter, autojump_clock):
        """
        If a wrong exception is raised, the counter does not change.
        """
        c = ptrio.count_exceptions(fake_counter, raiser(), exc=TypeError)

        with pytest.raises(ValueError):
            await c

        assert 0 == fake_counter._val

    async def test_awaitable_exc(self, fake_counter, autojump_clock):
        """
        If the correct exception is raised, count it.
        """
        c = ptrio.count_exceptions(fake_counter, raiser(), exc=ValueError)

        with pytest.raises(ValueError):
            await c

        assert 1 == fake_counter._val

    async def test_awaitable_group(self, fake_counter):
        """
        Groups are looked into for awaitables, too.
        """
        c = ptrio.count_exceptions(
            fake_counter, fail_in_nursery(ValueError()), exc=ValueError
        )

        with pytest.raises(ExceptionGroup):
            await c

        assert 1 == fake_counter._val


@pytest.mark.trio
class TestTrackInprogress:
    async def test_async_decorator(self, fake_gauge):
        """
        Works as a decorator of async functions.
        """

        @ptrio.track_inprogress(fake_gauge)
        async def f():
            assert 1 == fake_gauge._val
            await trio.lowlevel.checkpoint()

        await f()

        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_coroutine(self, fake_gauge, autojump_clock):
        """
        Incs and decs.
        """
        f = ptrio.track_inprogress(fake_gauge)(coro)

        await f()

        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_decorator_exc(self, fake_gauge, autojump_clock):
        """
        Decs, even if the function fails.
        """
        f = ptrio.track_inprogress(fake_gauge)(raiser)

        with pytest.raises(ValueError):
            await f()

        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_decorator_cancelled(self, fake_gauge):
        """
        Decs, even if the function is cancelled.
        """
        f = ptrio.track_inprogress(fake_gauge)(trio.sleep_forever)

        with trio.move_on_after(0) as scope:
            await f()

        assert scope.cancelled_caught
        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_concurrent(self, fake_gauge, autojump_clock):
        """
        The gauge counts concurrent calls.
        """
        f = ptrio.track_inprogress(fake_gauge)(coro)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(f)
            nursery.start_soon(f)
            await trio.sleep(0.5)

            assert 2 == fake_gauge._val

        assert 0 == fake_gauge._val

    async def test_awaitable(self, fake_gauge, autojump_clock):
        """
        Incs and decs.
        """
        wrapped = ptrio.track_inprogress(fake_gauge, coro())

        # Nothing is in progress until we await it.
        assert 0 == fake_gauge._val

        assert 42 == await wrapped

        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_awaitable_exc(self, fake_gauge, autojump_clock):
        """
        Decs, even if the awaitable fails.
        """
        wrapped = ptrio.track_inprogress(fake_gauge, raiser())

        with pytest.raises(ValueError):
            await wrapped

        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls

    async def test_awaitable_never_awaited(self, fake_gauge):
        """
        The gauge does not leak if the awaitable is never awaited.
        """
        inner = coro()
        wrapped = ptrio.track_inprogress(fake_gauge, inner)

        wrapped.close()
        inner.close()

        assert 0 == fake_gauge._val
        assert 0 == fake_gauge._calls


class TestImport:
    def test_exports(self):
        """
        The package exports the same decorators as its siblings.
        """
        from prometheus_async import aio

        assert {"count_exceptions", "time", "track_inprogress"} <= set(
            ptrio.__all__
        )
        assert {"count_exceptions", "time", "track_inprogress"} <= set(
            aio.__all__
        )

    def test_without_h11(self, monkeypatch):
        """
        The decorators only need Trio.  Without h11, there's no web endpoint
        and service discovery, but the package is still importable.
        """
        # Forget everything about the package, such that it's imported anew.
        for name in list(sys.modules):
            if name.startswith("prometheus_async.trio"):
                monkeypatch.delitem(sys.modules, name)
        # Importing sets this, so make sure it's put back afterwards.
        monkeypatch.setattr(prometheus_async, "trio", ptrio)
        # None makes the import fail.
        monkeypatch.setitem(sys.modules, "h11", None)

        fresh = importlib.import_module("prometheus_async.trio")

        assert fresh is not ptrio
        assert [
            "count_exceptions",
            "time",
            "track_inprogress",
        ] == fresh.__all__
        assert not hasattr(fresh, "web")
        assert not hasattr(fresh, "sd")
