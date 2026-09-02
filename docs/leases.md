# Leases and fencing

A lease provides temporary ownership; its monotonically increasing fencing token provides authority.
Expiry and takeover issue a greater token. Renew and release require the current owner and token.

Checking a token before opening a transaction is unsafe. Lock the consumer-owned authority row,
compare the token, and perform the protected mutation under the same lock and commit. The Django
factory accepts callbacks for this exact pattern and owns no schema.
