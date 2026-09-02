# Django protocol matrix provenance

The `django_protocol_matrix/` snapshot is generated with Cookiecutter `2.6.0` from
Cookiecutter Django tag `2025.10.01`, commit
`6cfcbb7fa5c354e6f863e5219e9460e33b66d956`. The exact non-interactive inputs are committed in
`django_protocol_matrix.cookiecutter.json`.

The generation host is Linux; the target runtime is Python 3.13, Django 5.2.7, Django REST
Framework, Docker, PostgreSQL 16, and GitHub Actions. Cloud integrations, a frontend pipeline,
async mode, Celery, Sentry, and Heroku are disabled.

Post-generation changes are limited to pinning Python and Django, adding the synthetic `legacy`,
`mobile_v2`, and `erp_v2` protocol apps, installing a caller-supplied Pytitect wheel, and adding
protocol/concurrency tests. Python files were normalized with the repository formatter after these
additions. The generated `psycopg[c]` pin is changed to the same-version binary distribution so CI
does not depend on host `pg_config`. This canary is a consumer: its concrete models, URLs, migrations,
credentials, and protocol bindings are deliberately outside `src/` and excluded from packages.
