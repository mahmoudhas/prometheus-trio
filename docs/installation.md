# Installation and Requirements

This fork is installed from GitHub, not from PyPI:
The *prometheus-async* package there is the original and doesn't support Trio.

If you just want to instrument an *asyncio*-based application:

```console
$ python -Im pip install "prometheus-async @ git+https://github.com/mahmoudhas/prometheus-trio"
```

If you want to expose metrics using *aiohttp*, you also have to install it along with it.

```console
$ python -Im pip install "prometheus-async @ git+https://github.com/mahmoudhas/prometheus-trio" aiohttp
```

If you want to instrument a *Trio*-based application, you have to install it along with *prometheus-async*, and, if you want to expose metrics, *h11*.
The *trio* extra takes care of both:

```console
$ python -Im pip install "prometheus-async[trio] @ git+https://github.com/mahmoudhas/prometheus-trio"
```

*prometheus-async* does not require anything extra to instrument Twisted applications.
