# Security protocols

RFC 8785 canonical JSON uses the specialized `rfc8785` library after strict I-JSON validation. DPoP
accepts only ES256 and validates a closed JOSE header and claim set, method, normalized target URI,
issuance time, UUID-v4 JTI, access-token hash, optional nonce, and atomic replay reservation.

Content-Digest accepts canonical SHA-256 and compares in constant time under a body-size limit. HTTP
Message Signatures require an allowlisted algorithm, tag, components, creation/expiry window, nonce,
digest when configured, a consumer-owned key resolver, a specialized verification backend, and replay
reservation.

A valid proof authenticates only that protocol statement. Session, connector, database, tenant, and
authorization policy always remain in the application.

W3C Trace Context is untrusted correlation input, not a security proof. The bounded parser rejects
malformed identifiers and state, but consumers still own trust-boundary restarts, sampling limits,
cross-origin exposure, and any decision to propagate vendor state. Trace and tracestate values must
not contain credentials or personal information.
