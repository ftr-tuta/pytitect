# Compatibility

This table is generated from `pyproject.toml` and checked by `tool/docs_quality.py`.

| Surface | Declared support | CI evidence |
| --- | --- | --- |
| CPython | `>=3.12` | 3.12, 3.13, and 3.14 unit matrix |
| Django | `>=5.2.1,<6.2` | 5.2, 6.0, and 6.1 jobs plus PostgreSQL consumers |
| Django REST Framework | `>=3.16,<4` | minimum/latest adapter jobs |
| drf-spectacular | `>=0.28,<1` | contracts smoke and schema tests |

Optional dependencies remain isolated: the core imports with none installed. `pytitect.aio` is a
Preview namespace with explicit bounded runtimes and separate async ports. Framework, database, and
transport adapters are Low-level and never load through the package root. The exact package wheel
used by extras smokes is also installed unchanged in framework and infrastructure canaries.
