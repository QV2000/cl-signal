"""
Reusable DuckDB query functions.
"""
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
import duckdb

from db.schema import get_connection


def get_wallet_fills(
    conn: duckdb.DuckDBPyConnection,
    wallet: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[Tuple]:
    """
    Get all fills for a wallet, optionally filtered by date range.

    Returns list of (ts, buyer, seller, taker_side, px, sz, notional_usd) tuples.
    """
    query = """
        SELECT ts, buyer, seller, taker_side, px, sz, notional_usd
        FROM fills
        WHERE buyer = ? OR seller = ?
    """
    params = [wallet, wallet]

    if start_date:
        query += " AND ts >= ?"
        params.append(start_date)

    if end_date:
        query += " AND ts <= ?"
        params.append(end_date)

    query += " ORDER BY ts"

    return conn.execute(query, params).fetchall()


def get_active_wallets(
    conn: duckdb.DuckDBPyConnection,
    days: int = 7,
) -> List[str]:
    """Get wallets that have traded in the last N days."""
    cutoff = datetime.utcnow() - timedelta(days=days)

    result = conn.execute(
        """
        SELECT DISTINCT wallet
        FROM wallet_registry
        WHERE last_seen >= ?
        """,
        [cutoff]
    ).fetchall()

    return [row[0] for row in result]


def get_latest_position_snapshot(
    conn: duckdb.DuckDBPyConnection,
    wallet: str,
) -> Optional[Tuple]:
    """Get the most recent position snapshot for a wallet."""
    return conn.execute(
        """
        SELECT *
        FROM position_snapshots
        WHERE wallet = ?
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """,
        [wallet]
    ).fetchone()


def get_latest_scores(
    conn: duckdb.DuckDBPyConnection,
    min_score: Optional[float] = None,
    max_score: Optional[float] = None,
    scoreable_only: bool = True,
) -> List[Tuple]:
    """
    Get the latest wallet scores.

    Args:
        min_score: Minimum overall_score filter.
        max_score: Maximum overall_score filter.
        scoreable_only: Only include wallets with is_scoreable=True.

    Returns list of (wallet, overall_score, trade_count_window, win_rate) tuples.
    """
    query = """
        SELECT wallet, overall_score, trade_count_window, win_rate
        FROM wallet_scores
        WHERE score_date = (SELECT MAX(score_date) FROM wallet_scores)
    """

    if scoreable_only:
        query += " AND is_scoreable = TRUE"

    if min_score is not None:
        query += f" AND overall_score >= {min_score}"

    if max_score is not None:
        query += f" AND overall_score <= {max_score}"

    query += " ORDER BY overall_score DESC"

    return conn.execute(query).fetchall()


def get_signal_deltas(
    conn: duckdb.DuckDBPyConnection,
) -> Optional[dict]:
    """
    Get the latest signal with computed deltas.

    Returns dict with composite_signal, delta_1h, delta_4h, delta_24h.
    """
    result = conn.execute("""
        SELECT
            signal_ts,
            composite_signal,
            composite_signal - LAG(composite_signal, 4) OVER (ORDER BY signal_ts) as delta_1h,
            composite_signal - LAG(composite_signal, 16) OVER (ORDER BY signal_ts) as delta_4h,
            composite_signal - LAG(composite_signal, 96) OVER (ORDER BY signal_ts) as delta_24h
        FROM signals
        ORDER BY signal_ts DESC
        LIMIT 1
    """).fetchone()

    if not result:
        return None

    return {
        "signal_ts": result[0],
        "composite_signal": result[1],
        "delta_1h": result[2],
        "delta_4h": result[3],
        "delta_24h": result[4],
    }


def get_wallet_volume_leaders(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 20,
) -> List[Tuple]:
    """Get top wallets by total volume."""
    return conn.execute(
        """
        SELECT wallet, total_fills, total_volume_usd, last_seen
        FROM wallet_registry
        WHERE is_active = TRUE
        ORDER BY total_volume_usd DESC
        LIMIT ?
        """,
        [limit]
    ).fetchall()


def get_fill_count_by_hour(
    conn: duckdb.DuckDBPyConnection,
    hours: int = 24,
) -> List[Tuple]:
    """Get fill counts grouped by hour for the last N hours."""
    return conn.execute(
        f"""
        SELECT
            DATE_TRUNC('hour', ts) as hour,
            COUNT(*) as fill_count,
            SUM(notional_usd) as volume_usd
        FROM fills
        WHERE ts >= NOW() - INTERVAL '{hours} hours'
        GROUP BY DATE_TRUNC('hour', ts)
        ORDER BY hour
        """
    ).fetchall()
