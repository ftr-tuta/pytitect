# Compatibility

This table is generated from `pyproject.toml` and checked by `tool/docs_quality.py`.

| Surface | Declared support | CI evidence |
| --- | --- | --- |
| CPython | `>=3.12` | 3.12, 3.13, and 3.14 unit matrix |
| Django | `>=5.2.1,<5.3` | 5.2 minimum/latest jobs and PostgreSQL consumers |
| Django REST Framework | `>=3.16,<4` | minimum/latest adapter jobs |
| drf-spectacular | `>=0.28,<1` | contracts smoke and schema tests |

Optional dependencies remain isolated: the core imports with none installed. `pytitect.aio` is a
reserved namespace; 1.0 has no FastAPI, ASGI, or async-store implementation. The package wheel used
by the extras smokes is also installed unchanged in the Django canary and reference project.
