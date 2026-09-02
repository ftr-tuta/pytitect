# Repository instructions

- Keep all public code, documentation, tests, release metadata, and messages in English.
- Preserve neutrality. Do not introduce consumer names, domain rules, private contracts, migrations,
  schemas, URLs, middleware, signals, workers, or application-owned bindings.
- `import pytitect` must remain dependency-free and side-effect free: no settings, database, network,
  logging, registry, environment, or optional-adapter access.
- Consumers own persistence, transactions, routing, authorization, protocol selection, and external
  effects. Reference stores must be finite and must state their process-local limitations.
- Never add a global runtime, singleton, protocol fallback, or automatic binding selection.
- Work through short-lived branches and pull requests after repository bootstrap. Protect `main` and
  `v*` tags; do not force-push or bypass required checks.
