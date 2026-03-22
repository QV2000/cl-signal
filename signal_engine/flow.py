"""
High-Frequency Flow Signal

Computes real-time directional flow from scored wallet activity.
Updates on every WebSocket trade. State lives entirely in memory.
"""

import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Callable, Optional

import logging

logger = logging.getLogger(__name__)


class FlowSignal:
    """
    Computes a real-time directional flow signal from scored wallet activity.
    Maintains rolling windows of recent fills, weighted by wallet score and
    taker/maker status. Output is a normalized signal from -1 (bearish) to +1 (bullish).
    """

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        self.fills = deque()  # (timestamp_s, weighted_contribution)

    def on_fill(self, ts_ms: int, buyer: str, seller: str, taker_side: str,
                px: float, sz: float, wallet_scores: Dict[str, float]):
        """
        Called on every WebSocket trade message.
        ts_ms: trade timestamp in milliseconds
        taker_side: "B" (buyer was aggressor) or "S" (seller was aggressor)
        """
        ts_s = ts_ms / 1000.0
        notional = px * sz

        buyer_score = wallet_scores.get(buyer, 0.0)
        seller_score = wallet_scores.get(seller, 0.0)

        # Taker fills are more informative than maker fills.
        # Crossing the spread signals urgency and conviction.
        # Maker fills might just be passive limit orders getting hit.

        if buyer_score != 0.0:
            taker_weight = 1.0 if taker_side == "B" else 0.4
            contribution = buyer_score * notional * taker_weight
            self.fills.append((ts_s, contribution))

        if seller_score != 0.0:
            taker_weight = 1.0 if taker_side == "S" else 0.4
            contribution = -seller_score * notional * taker_weight
            self.fills.append((ts_s, contribution))

        # Trim expired fills
        cutoff = ts_s - self.window_seconds
        while self.fills and self.fills[0][0] < cutoff:
            self.fills.popleft()

    def get_signal(self) -> float:
        """Returns normalized signal from -1 to +1. Returns 0 if no data."""
        if not self.fills:
            return 0.0
        weighted_sum = sum(f[1] for f in self.fills)
        abs_sum = sum(abs(f[1]) for f in self.fills)
        if abs_sum == 0:
            return 0.0
        return weighted_sum / abs_sum

    def get_fill_count(self) -> int:
        """Number of scored fills currently in the window."""
        return len(self.fills)


class FlowAlertManager:
    """Manages flow alerts with cooldown and outcome tracking."""

    def __init__(self, cooldown_seconds: int = 120):
        self.cooldown_seconds = cooldown_seconds
        self.last_alert_time = 0
        self.last_alert_direction: Optional[str] = None

    def check_alert(self, flow_30s: FlowSignal, flow_2m: FlowSignal,
                    flow_5m: FlowSignal, current_px: float,
                    threshold_30s: float = 0.4, threshold_2m: float = 0.4,
                    threshold_5m: float = 0.3) -> Optional[Dict]:
        """
        Check if alert conditions are met.
        Returns alert dict if should fire, None otherwise.
        """
        now = time.time()
        if now - self.last_alert_time < self.cooldown_seconds:
            return None

        s30 = flow_30s.get_signal()
        s2m = flow_2m.get_signal()
        s5m = flow_5m.get_signal()

        # All three must agree on direction
        all_bullish = s30 > threshold_30s and s2m > threshold_2m and s5m > threshold_5m
        all_bearish = s30 < -threshold_30s and s2m < -threshold_2m and s5m < -threshold_5m

        if not all_bullish and not all_bearish:
            return None

        direction = "LONG" if all_bullish else "SHORT"

        self.last_alert_time = now
        self.last_alert_direction = direction

        return {
            "direction": direction,
            "signal_30s": s30,
            "signal_2m": s2m,
            "signal_5m": s5m,
            "fills_30s": flow_30s.get_fill_count(),
            "fills_2m": flow_2m.get_fill_count(),
            "fills_5m": flow_5m.get_fill_count(),
            "mark_px": current_px,
            "timestamp": datetime.now(timezone.utc),
        }

    def seconds_since_last_alert(self) -> float:
        """Returns seconds since last alert, or infinity if never alerted."""
        if self.last_alert_time == 0:
            return float('inf')
        return time.time() - self.last_alert_time

    def format_alert_message(self, alert: Dict) -> str:
        """Format alert for Telegram."""
        direction = alert["direction"]
        icon = "+" if direction == "LONG" else "-"

        return (
            f"CL FLOW ALERT: {direction}\n\n"
            f"30s: {alert['signal_30s']:+.3f} ({alert['fills_30s']} fills)\n"
            f"2m:  {alert['signal_2m']:+.3f} ({alert['fills_2m']} fills)\n"
            f"5m:  {alert['signal_5m']:+.3f} ({alert['fills_5m']} fills)\n\n"
            f"Mark: ${alert['mark_px']:.2f}\n"
            f"Time: {alert['timestamp'].strftime('%H:%M:%S UTC')}"
        )
