# Versioning

Pytitect follows Semantic Versioning. Before 1.0, minor releases may intentionally revise public APIs;
patch releases preserve them unless correcting an unsafe behavior. Alpha, beta, and release-candidate
versions use PEP 440 identifiers. The version has one source in `src/pytitect/__about__.py`.

Public symbols are recorded in `tool/public-api.txt`. Removing or changing one requires a deliberate
version decision and changelog entry.

The source candidate is `1.0.0rc1`, whose derivable repository tag is `v1.0.0-rc.1`. Candidate
preparation does not imply materialization: until that protected tag, GitHub Release, and PyPI
artifact exist, `v0.9.0a1` remains the latest public distribution.
