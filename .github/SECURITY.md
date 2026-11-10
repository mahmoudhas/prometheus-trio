# Security Policy

## Supported Versions

We are following [Calendar Versioning](https://calver.org) with generous backwards-compatibility guarantees.
Therefore we only support the latest version.

Put simply, you shouldn't ever be afraid to upgrade as long as you're only using our public APIs.
Whenever there is a need to break compatibility, it is announced in the changelog, and raises a `DeprecationWarning` for a year (if possible) before it's finally really broken.


## Reporting a Vulnerability

To report a security vulnerability in this fork, please use GitHub's [private vulnerability reporting](https://github.com/mahmoudhas/prometheus-trio/security/advisories/new).
Please don't open a public issue or pull request for it.

If the vulnerability is in code that this fork shares with the original *prometheus-async*, you can also report it there using the [Tidelift security contact](https://tidelift.com/security).
Tidelift will coordinate the fix and disclosure.
