"""Synthetic Django composition values, imported only after Django setup."""

from pytitect.django import DjangoAsyncBridge, DjangoTransactionRunner


def transaction_components(*, using: str) -> tuple[DjangoTransactionRunner, DjangoAsyncBridge]:
    return DjangoTransactionRunner(using), DjangoAsyncBridge(concurrency=4)
