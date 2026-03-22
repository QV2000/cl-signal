"""Signal module for composite signal computation."""

from signal_engine.composer import (
    SignalEngine,
    CompositeSignal,
    generate_composite_signal,
    should_alert,
)

__all__ = [
    'SignalEngine',
    'CompositeSignal',
    'generate_composite_signal',
    'should_alert',
]