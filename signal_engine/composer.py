"""
Signal Composer Module

Computes the composite positioning signal from wallet positions and scores.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, List

import duckdb

from config import CL_COIN

logger = logging.getLogger(__name__)


@dataclass
class CompositeSignal:
    """The computed composite signal with metadata."""
    timestamp: datetime
    composite_signal: float  # -1 to +1
    direction: str  # 'LONG', 'SHORT', 'FLAT'
    confidence: float  # 0 to 1
    weighted_long: float
    weighted_short: float
    scored_wallets: int
    total_wallets: int
    hhi: float  # Herfindahl-Hirschman Index for concentration
    is_nymex_hours: bool

    # Deltas (rate of change)
    delta_1h: Optional[float] = None
    delta_4h: Optional[float] = None
    delta_24h: Optional[float] = None


def get_current_positions(conn: duckdb.DuckDBPyConnection) -> Dict[str, float]:
    """
    Get current position for each wallet from the latest snapshot.
    Returns {wallet: signed_position} where positive = long.
    """
    result = conn.execute("""
        WITH latest AS (
            SELECT wallet, position_size, side,
                   ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY ts DESC) as rn
            FROM position_snapshots
            WHERE coin = ?
        )
        SELECT wallet, position_size, side
        FROM latest
        WHERE rn = 1 AND position_size > 0
    """, [CL_COIN]).fetchall()

    positions = {}
    for wallet, size, side in result:
        # Convert to signed position
        signed = float(size) if side == 'B' else -float(size)
        positions[wallet] = signed

    return positions


def get_wallet_scores(conn: duckdb.DuckDBPyConnection) -> Dict[str, float]:
    """Get wallet scores from database."""
    result = conn.execute("""
        SELECT wallet, composite_score, archetype_weight
        FROM wallet_scores
        WHERE is_scoreable = TRUE
    """).fetchall()

    # Combine composite score with archetype weight
    scores = {}
    for wallet, score, weight in result:
        scores[wallet] = float(score) * float(weight)

    return scores


def compute_hhi(positions: Dict[str, float]) -> float:
    """
    Compute Herfindahl-Hirschman Index to measure concentration.
    HHI > 0.25 suggests concentrated positioning (few big players).
    """
    if not positions:
        return 0.0

    total = sum(abs(p) for p in positions.values())
    if total == 0:
        return 0.0

    shares = [(abs(p) / total) ** 2 for p in positions.values()]
    return sum(shares)


def is_nymex_hours() -> bool:
    """
    Check if we're in NYMEX/CME trading hours.
    CME CL trades Sunday 5pm CT to Friday 4pm CT with a 1hr daily break.
    Simplified: weekday 11:00-22:00 UTC.
    """
    now = datetime.utcnow()
    weekday = now.weekday()
    hour = now.hour

    # Sunday evening to Friday afternoon
    if weekday == 6:  # Sunday
        return hour >= 23
    elif weekday == 5:  # Saturday
        return False
    elif weekday == 4:  # Friday
        return hour < 22
    else:  # Mon-Thu
        return True


def compute_signal(wallet_positions: Dict[str, float],
                   wallet_scores: Dict[str, float]) -> Dict:
    """
    Compute the composite positioning signal.

    The score encodes direction of information:
    - Positive score + long = bullish
    - Negative score + long = bearish (fade the loser)
    - Positive score + short = bearish
    - Negative score + short = bullish (fade the loser)
    """
    weighted_long = 0.0
    weighted_short = 0.0
    total_abs_weight = 0.0
    scored_count = 0

    for wallet, position in wallet_positions.items():
        score = wallet_scores.get(wallet, 0.0)
        if score == 0.0:
            continue

        scored_count += 1
        weighted_contribution = score * position
        abs_weight = abs(score) * abs(position)

        if weighted_contribution > 0:
            weighted_long += weighted_contribution
        else:
            weighted_short += abs(weighted_contribution)

        total_abs_weight += abs_weight

    if total_abs_weight == 0:
        return {
            'composite_signal': 0.0,
            'direction': 'FLAT',
            'confidence': 0.0,
            'weighted_long': 0.0,
            'weighted_short': 0.0,
            'scored_wallets': 0,
        }

    # Normalize to [-1, +1]
    composite = (weighted_long - weighted_short) / total_abs_weight

    # Direction thresholds
    if composite > 0.3:
        direction = 'LONG'
    elif composite < -0.3:
        direction = 'SHORT'
    else:
        direction = 'FLAT'

    # Confidence scales with participation and signal strength
    confidence = min(scored_count / 50, 1.0) * abs(composite)

    return {
        'composite_signal': composite,
        'direction': direction,
        'confidence': confidence,
        'weighted_long': weighted_long,
        'weighted_short': weighted_short,
        'scored_wallets': scored_count,
    }


def get_signal_deltas(conn: duckdb.DuckDBPyConnection) -> Dict[str, Optional[float]]:
    """
    Compute rate-of-change in signal over 1h, 4h, 24h windows.
    Assumes 15-minute signal intervals.
    """
    result = conn.execute("""
        WITH ordered AS (
            SELECT
                composite_signal,
                LAG(composite_signal, 4) OVER (ORDER BY signal_ts) as lag_1h,
                LAG(composite_signal, 16) OVER (ORDER BY signal_ts) as lag_4h,
                LAG(composite_signal, 96) OVER (ORDER BY signal_ts) as lag_24h
            FROM signals
            ORDER BY signal_ts DESC
            LIMIT 1
        )
        SELECT
            composite_signal - lag_1h as delta_1h,
            composite_signal - lag_4h as delta_4h,
            composite_signal - lag_24h as delta_24h
        FROM ordered
    """).fetchone()

    if result:
        return {
            'delta_1h': result[0],
            'delta_4h': result[1],
            'delta_24h': result[2],
        }
    return {'delta_1h': None, 'delta_4h': None, 'delta_24h': None}


def generate_composite_signal(conn: duckdb.DuckDBPyConnection) -> CompositeSignal:
    """Generate the full composite signal with all metadata."""
    # Get current data
    positions = get_current_positions(conn)
    scores = get_wallet_scores(conn)

    # Compute signal
    signal_data = compute_signal(positions, scores)

    # Get deltas
    deltas = get_signal_deltas(conn)

    # Compute HHI
    hhi = compute_hhi(positions)

    signal = CompositeSignal(
        timestamp=datetime.utcnow(),
        composite_signal=signal_data['composite_signal'],
        direction=signal_data['direction'],
        confidence=signal_data['confidence'],
        weighted_long=signal_data['weighted_long'],
        weighted_short=signal_data['weighted_short'],
        scored_wallets=signal_data['scored_wallets'],
        total_wallets=len(positions),
        hhi=hhi,
        is_nymex_hours=is_nymex_hours(),
        delta_1h=deltas.get('delta_1h'),
        delta_4h=deltas.get('delta_4h'),
        delta_24h=deltas.get('delta_24h'),
    )

    return signal


def persist_signal(conn: duckdb.DuckDBPyConnection, signal: CompositeSignal):
    """Save signal to database."""
    conn.execute("""
        INSERT INTO signals
        (signal_ts, composite_signal, direction, confidence,
         scored_wallets, total_wallets, hhi, is_nymex_hours,
         delta_1h, delta_4h, delta_24h)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        signal.timestamp,
        signal.composite_signal,
        signal.direction,
        signal.confidence,
        signal.scored_wallets,
        signal.total_wallets,
        signal.hhi,
        signal.is_nymex_hours,
        signal.delta_1h,
        signal.delta_4h,
        signal.delta_24h,
    ])


def should_alert(signal: CompositeSignal, prev_signal: Optional[CompositeSignal]) -> Dict:
    """
    Determine if this signal warrants an alert.

    Alert conditions:
    - Direction change (FLAT->LONG, LONG->SHORT, etc.)
    - Signal crosses threshold (|signal| > 0.5)
    - Rapid delta (|delta_1h| > 0.3)
    - High concentration (HHI > 0.25) warning
    """
    alerts = []

    # Direction change
    if prev_signal and signal.direction != prev_signal.direction:
        alerts.append({
            'type': 'direction_change',
            'message': f"Direction changed: {prev_signal.direction} -> {signal.direction}",
            'severity': 'high' if signal.direction != 'FLAT' else 'medium',
        })

    # Strong signal
    if abs(signal.composite_signal) > 0.5:
        alerts.append({
            'type': 'strong_signal',
            'message': f"Strong {signal.direction} signal: {signal.composite_signal:.2f}",
            'severity': 'high',
        })

    # Rapid change
    if signal.delta_1h and abs(signal.delta_1h) > 0.3:
        direction = 'bullish' if signal.delta_1h > 0 else 'bearish'
        alerts.append({
            'type': 'rapid_change',
            'message': f"Rapid {direction} shift: {signal.delta_1h:+.2f} in 1h",
            'severity': 'high',
        })

    # Concentration warning
    if signal.hhi > 0.25:
        alerts.append({
            'type': 'concentration',
            'message': f"High position concentration (HHI={signal.hhi:.2f})",
            'severity': 'warning',
        })

    return {
        'should_alert': len(alerts) > 0,
        'alerts': alerts,
    }


class SignalEngine:
    """Manages signal generation and alerting."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.last_signal: Optional[CompositeSignal] = None

    def generate_and_persist(self) -> CompositeSignal:
        """Generate signal, persist it, and check for alerts."""
        signal = generate_composite_signal(self.conn)
        persist_signal(self.conn, signal)

        # Check alerts
        alert_info = should_alert(signal, self.last_signal)
        if alert_info['should_alert']:
            for alert in alert_info['alerts']:
                logger.info(f"ALERT [{alert['severity']}]: {alert['message']}")

        self.last_signal = signal
        return signal

    def get_latest_signal(self) -> Optional[Dict]:
        """Get the most recent signal from database."""
        result = self.conn.execute("""
            SELECT signal_ts, composite_signal, direction, confidence,
                   scored_wallets, total_wallets, hhi, is_nymex_hours,
                   delta_1h, delta_4h, delta_24h
            FROM signals
            ORDER BY signal_ts DESC
            LIMIT 1
        """).fetchone()

        if not result:
            return None

        return {
            'timestamp': result[0],
            'signal': result[1],
            'direction': result[2],
            'confidence': result[3],
            'scored_wallets': result[4],
            'total_wallets': result[5],
            'hhi': result[6],
            'is_nymex': result[7],
            'delta_1h': result[8],
            'delta_4h': result[9],
            'delta_24h': result[10],
        }

    def get_signal_history(self, hours: int = 24) -> List[Dict]:
        """Get signal history for the past N hours."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        results = self.conn.execute("""
            SELECT signal_ts, composite_signal, direction, confidence
            FROM signals
            WHERE signal_ts >= ?
            ORDER BY signal_ts ASC
        """, [cutoff]).fetchall()

        return [
            {
                'timestamp': row[0],
                'signal': row[1],
                'direction': row[2],
                'confidence': row[3],
            }
            for row in results
        ]