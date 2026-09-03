# Dart interoperability probe

The canonical fixture is copied byte-for-byte from `titect-message/1`. A Dart implementation should
decode it, validate the closed profile, and re-encode the same canonical UTF-8 bytes. The bundle
manifest, rather than this convenience copy, remains authoritative.
