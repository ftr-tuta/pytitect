# Synthetic legacy binding

This example demonstrates inert installation. The application keeps its existing serializer, URL,
model, and transaction choices and imports only the Pytitect primitive it needs.

```python
from pytitect.http import ProblemRenderer, static_titles

legacy_problems = ProblemRenderer(
    "https://example.invalid/problems/legacy/",
    static_titles({"invalid-request": "Invalid request"}),
)
```

Nothing is registered by importing this file.
