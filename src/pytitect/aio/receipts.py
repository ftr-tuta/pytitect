"""Preview async ports and finite process-local reference stores."""

from __future__ import annotations

from typing import Protocol

from pytitect.core import OpaqueId
from pytitect.receipts import (
    InMemoryReceiptStore,
    Receipt,
)


class AsyncReceiptStore[ResultT](Protocol):
    """Persistence port; resolving uncertainty requires the explicit CAS method."""

    async def get(self, receipt_id: OpaqueId[object]) -> Receipt[ResultT] | None: ...

    async def add(self, receipt: Receipt[ResultT]) -> bool: ...

    async def transition(self, receipt: Receipt[ResultT], target: Receipt[ResultT]) -> bool: ...

    async def reconcile_uncertain(
        self,
        receipt: Receipt[ResultT],
        target: Receipt[ResultT],
    ) -> bool: ...


class InMemoryAsyncReceiptStore[ResultT]:
    """Finite process-local reference; no cross-process coordination or durability."""

    def __init__(self, *, capacity: int = 10_000) -> None:

        self._store = InMemoryReceiptStore[ResultT](capacity=capacity)

    async def get(self, receipt_id: OpaqueId[object]) -> Receipt[ResultT] | None:

        return self._store.get(receipt_id)

    async def add(self, receipt: Receipt[ResultT]) -> bool:

        return self._store.add(receipt)

    async def transition(self, receipt: Receipt[ResultT], target: Receipt[ResultT]) -> bool:

        return self._store.transition(receipt, target)

    async def reconcile_uncertain(
        self, receipt: Receipt[ResultT], target: Receipt[ResultT]
    ) -> bool:

        return self._store.reconcile_uncertain(receipt, target)
