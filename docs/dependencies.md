# Dependency and license inventory

The core has no dependencies. Optional runtime dependencies are listed below; verify the exact
transitive inventory from `uv.lock` before every release.

| Extra | Direct dependency | Purpose | Upstream license |
| --- | --- | --- | --- |
| django | Django | Transactions and model mixins | BSD-3-Clause |
| drf | djangorestframework | Strict HTTP serializers | BSD-3-Clause |
| contracts | Django | Supported framework constraint for schema helpers | BSD-3-Clause |
| contracts | djangorestframework | Supported DRF constraint for schema helpers | BSD-3-Clause |
| contracts | drf-spectacular | OpenAPI decorators | BSD-3-Clause |
| contracts | PyYAML | Local YAML contracts | MIT |
| canonical-json | rfc8785 | JSON Canonicalization Scheme | Apache-2.0 |
| dpop | PyJWT with cryptography | JOSE ES256 verification | MIT / dual Apache-BSD |
| signed-http | http-message-signatures | RFC 9421 verification | MIT |
| sync | rfc8785 | RFC 8785 cursor headers and interoperable fingerprints | Apache-2.0 |
| sync | cryptography >=46,<51 | AES-256-GCM cursor authentication | Apache-2.0 / BSD-3-Clause |

This table is an engineering inventory, not legal advice.
