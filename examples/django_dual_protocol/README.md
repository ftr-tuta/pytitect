# Synthetic dual-protocol binding

Define separate Django views and URL patterns for each descriptor. Do not add a negotiation fallback.

```python
from pytitect.contracts import ExactVersionPolicy, ProtocolDescriptor

legacy = ExactVersionPolicy(ProtocolDescriptor("orders", "1"))
current = ExactVersionPolicy(
    ProtocolDescriptor("orders", "2", frozenset({"receipts", "idempotency"}))
)

# The consumer binds legacy and current to distinct view/serializer/authentication stacks.
```
