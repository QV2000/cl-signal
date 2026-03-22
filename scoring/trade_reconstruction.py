"""
Trade Reconstruction Module

Reconstructs round-trip trades from fill data for each wallet.
A round trip is a sequence from flat -> position -> flat (or position reversal).
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime, timedelta

import duckdb

from config import CL_COIN

logger = logging.getLogger(__name__)


@dataclass
class RoundTrip:
    """Represents a completed round-trip trade."""
    wallet: str
    direction: str  # 'long' or 'short'
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    size: float
    realized_pnl: float
    hold_duration_hours: float
    entry_fills: int
    exit_fills: int
    alpha_1h: Optional[float] = None
    alpha_4h: Optional[float] = None
    alpha_24h: Optional[float] = None


@dataclass
class WalletFill:
    """A single fill for a wallet."""
    ts: datetime
    side: str
    size: float
    price: float


def get_wallet_fills(conn: duckdb.DuckDBPyConnection, wallet: str,
                     lookback_days: int = 60) -> List[WalletFill]:
    """Get all fills for a wallet within lookback period."""
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    result = conn.execute("""
        SELECT ts, side, size, price
        FROM fills
        WHERE wallet = ? AND coin = ? AND ts >= ?
        ORDER BY ts ASC
    """, [wallet, CL_COIN, cutoff]).fetchall()
    return [WalletFill(ts=row[0], side=row[1], size=float(row[2]), price=float(row[3]))
            for row in result]


def reconstruct_round_trips(fills: List[WalletFill], wallet: str) -> List[RoundTrip]:
    """Reconstruct round trips from a sequence of fills."""
    if not fills:
        return []

    round_trips = []
    position = 0.0
    entry_fills_data: List[Tuple[float, float]] = []
    entry_time: Optional[datetime] = None
    entry_direction: Optional[str] = None

    for fill in fills:
        delta = fill.size if fill.side == 'B' else -fill.size
        old_position = position
        position += delta

        old_dir = 'long' if old_position > 0 else ('short' if old_position < 0 else 'flat')
        new_dir = 'long' if position > 0 else ('short' if position < 0 else 'flat')

        # Opening new position from flat
        if old_dir == 'flat' and new_dir != 'flat':
            entry_fills_data = [(abs(delta), fill.price)]
            entry_time = fill.ts
            entry_direction = new_dir

        # Adding to position
        elif old_dir == new_dir and new_dir != 'flat':
            entry_fills_data.append((abs(delta), fill.price))

        # Full close
        elif new_dir == 'flat' and old_dir != 'flat':
            if entry_time and entry_fills_data:
                total_entry_size = sum(s for s, p in entry_fills_data)
                entry_price = sum(s * p for s, p in entry_fills_data) / total_entry_size if total_entry_size > 0 else 0
                exit_price = fill.price

                if entry_direction == 'long':
                    pnl = (exit_price - entry_price) * total_entry_size
                else:
                    pnl = (entry_price - exit_price) * total_entry_size

                hold_duration = (fill.ts - entry_time).total_seconds() / 3600

                round_trips.append(RoundTrip(
                    wallet=wallet,
                    direction=entry_direction,
                    entry_time=entry_time,
                    exit_time=fill.ts,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    size=total_entry_size,
                    realized_pnl=pnl,
                    hold_duration_hours=hold_duration,
                    entry_fills=len(entry_fills_data),
                    exit_fills=1
                ))

            entry_fills_data = []
            entry_time = None
            entry_direction = None

        # Position reversal
        elif old_dir != new_dir and old_dir != 'flat' and new_dir != 'flat':
            if entry_time and entry_fills_data:
                total_entry_size = sum(s for s, p in entry_fills_data)
                entry_price = sum(s * p for s, p in entry_fills_data) / total_entry_size if total_entry_size > 0 else 0
                exit_price = fill.price
                closed_size = abs(old_position)

                if entry_direction == 'long':
                    pnl = (exit_price - entry_price) * closed_size
                else:
                    pnl = (entry_price - exit_price) * closed_size

                hold_duration = (fill.ts - entry_time).total_seconds() / 3600

                round_trips.append(RoundTrip(
                    wallet=wallet,
                    direction=entry_direction,
                    entry_time=entry_time,
                    exit_time=fill.ts,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    size=closed_size,
                    realized_pnl=pnl,
                    hold_duration_hours=hold_duration,
                    entry_fills=len(entry_fills_data),
                    exit_fills=1
                ))

            new_size = abs(position)
            entry_fills_data = [(new_size, fill.price)]
            entry_time = fill.ts
            entry_direction = new_dir

    return round_trips


def get_price_at_time(conn: duckdb.DuckDBPyConnection, target_time: datetime,
                      tolerance_minutes: int = 10) -> Optional[float]:
    """Get price closest to target_time."""
    result = conn.execute("""
        SELECT price FROM orderbook_snapshots
        WHERE coin = ? AND side = 'B' AND level = 0
        AND ts BETWEEN ? AND ?
        ORDER BY ABS(EXTRACT(EPOCH FROM (ts - ?)))
        LIMIT 1
    """, [CL_COIN,
          target_time - timedelta(minutes=tolerance_minutes),
          target_time + timedelta(minutes=tolerance_minutes),
          target_time]).fetchone()

    if result:
        return float(result[0])

    result = conn.execute("""
        SELECT price FROM fills
        WHERE coin = ?
        AND ts BETWEEN ? AND ?
        ORDER BY ABS(EXTRACT(EPOCH FROM (ts - ?)))
        LIMIT 1
    """, [CL_COIN,
          target_time - timedelta(minutes=tolerance_minutes),
          target_time + timedelta(minutes=tolerance_minutes),
          target_time]).fetchone()

    return float(result[0]) if result else None


def compute_post_entry_alpha(conn: duckdb.DuckDBPyConnection,
                              round_trips: List[RoundTrip]) -> List[RoundTrip]:
    """Compute price move in wallet's direction at 1h, 4h, 24h after entry."""
    for rt in round_trips:
        try:
            for hours, attr in [(1, 'alpha_1h'), (4, 'alpha_4h'), (24, 'alpha_24h')]:
                price = get_price_at_time(conn, rt.entry_time + timedelta(hours=hours))
                if price and rt.entry_price:
                    move = (price - rt.entry_price) / rt.entry_price
                    setattr(rt, attr, move if rt.direction == 'long' else -move)
        except Exception as e:
            logger.debug(f"Could not compute alpha: {e}")
    return round_trips


def get_all_active_wallets(conn: duckdb.DuckDBPyConnection,
                           min_fills: int = 5,
                           lookback_days: int = 60) -> List[str]:
    """Get wallets with at least min_fills in lookback period."""
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    result = conn.execute("""
        SELECT wallet, COUNT(*) as fill_count
        FROM fills
        WHERE coin = ? AND ts >= ?
        GROUP BY wallet
        HAVING COUNT(*) >= ?
        ORDER BY fill_count DESC
    """, [CL_COIN, cutoff, min_fills]).fetchall()
    return [row[0] for row in result]


def reconstruct_all_wallets(conn: duckdb.DuckDBPyConnection,
                            min_fills: int = 5,
                            lookback_days: int = 60) -> dict[str, List[RoundTrip]]:
    """Reconstruct round trips for all active wallets."""
    wallets = get_all_active_wallets(conn, min_fills, lookback_days)
    logger.info(f"Reconstructing trades for {len(wallets)} wallets")

    all_round_trips = {}
    for wallet in wallets:
        fills = get_wallet_fills(conn, wallet, lookback_days)
        round_trips = reconstruct_round_trips(fills, wallet)
        round_trips = compute_post_entry_alpha(conn, round_trips)
        if round_trips:
            all_round_trips[wallet] = round_trips

    total_trips = sum(len(trips) for trips in all_round_trips.values())
    logger.info(f"Reconstructed {total_trips} round trips across {len(all_round_trips)} wallets")
    return all_round_trips