# Versioning

Pytitect follows Semantic Versioning. Before 1.0, minor releases may intentionally revise public APIs;
patch releases preserve them unless correcting an unsafe behavior. Alpha, beta, and release-candidate
versions use PEP 440 identifiers. The version has one source in `src/pytitect/__about__.py`.

Public symbols are recorded in `tool/public-api.txt`. Removing or changing one requires a deliberate
version decision and changelog entry.

A source version `X.Y.ZrcN` maps to the protected repository tag `vX.Y.Z-rc.N`; a stable source
version `X.Y.Z` maps to `vX.Y.Z`. A matching tag and GitHub Release are the authoritative record of
publication. The repository does not duplicate that external state in a checked-in boolean. PyPI
and TestPyPI publication are prohibited until separately and explicitly authorized.
