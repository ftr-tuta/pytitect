# Compatibility

Pytitect requires CPython 3.12 or newer. CI covers Python 3.12 and 3.14. The initial adapter contract
supports Django `>=5.2.1,<5.3`, DRF `>=3.16,<4`, and drf-spectacular `>=0.28,<1`.

Optional dependencies are isolated. Core imports succeed when none is installed. There is no async
adapter in 0.9; `pytitect.aio` only reserves the namespace.
