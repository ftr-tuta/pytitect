# Versioning

Pytitect follows Semantic Versioning. Before 1.0, minor releases may intentionally revise public APIs;
patch releases preserve them unless correcting an unsafe behavior. Alpha, beta, and release-candidate
versions use PEP 440 identifiers. The version has one source in `src/pytitect/__about__.py`.

Public symbols are recorded in `tool/public-api.txt`. Removing or changing one requires a deliberate
version decision and changelog entry.
