"""
DuckDB schema initialization for CL Signal System.
"""
import os
import duckdb
from pathlib import Path

from config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the database directory if needed."""
    db_dir = Path(DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)


def init_schema(conn: duckdb.DuckDBPyConnection = None) -> None:
    """Initialize all tables in the database."""
    if conn is None:
        conn = get_connection()

    # Every fill on xyz:WTIOIL captured from WebSocket
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fills (
            fill_id VARCHAR PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            coin VARCHAR NOT NULL DEFAULT 'xyz:CL',
            buyer VARCHAR NOT NULL,
            seller VARCHAR NOT NULL,
            taker_side CHAR(1) NOT NULL,
            px DECIMAL(18,6) NOT NULL,
            sz DECIMAL(18,8) NOT NULL,
            hash VARCHAR,
            tid BIGINT,
            notional_usd DECIMAL(18,2)
        )
    """)

    # Periodic snapshots of tracked wallet positions
    conn.execute("""
        CREATE TABLE IF NOT EXISTS position_snapshots (
            snapshot_ts TIMESTAMP NOT NULL,
            wallet VARCHAR NOT NULL,
            coin VARCHAR NOT NULL DEFAULT 'xyz:CL',
            szi DECIMAL(18,8),
            entry_px DECIMAL(18,6),
            leverage DECIMAL(6,2),
            liquidation_px DECIMAL(18,6),
            unrealized_pnl DECIMAL(18,4),
            return_on_equity DECIMAL(10,6),
            account_value DECIMAL(18,4),
            margin_used DECIMAL(18,4),
            is_nymex_open BOOLEAN,
            PRIMARY KEY (snapshot_ts, wallet)
        )
    """)

    # Registry of every wallet ever seen trading CL
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_registry (
            wallet VARCHAR PRIMARY KEY,
            first_seen TIMESTAMP NOT NULL,
            last_seen TIMESTAMP NOT NULL,
            total_fills INTEGER DEFAULT 0,
            total_volume_usd DECIMAL(18,2) DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            behavior_class VARCHAR,
            hyper_tracker_cohort VARCHAR,
            notes VARCHAR
        )
    """)

    # Computed wallet scores (updated daily)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_scores (
            wallet VARCHAR NOT NULL,
            score_date DATE NOT NULL,
            overall_score DECIMAL(8,6),
            pnl_score DECIMAL(8,6),
            consistency_score DECIMAL(8,6),
            timing_score DECIMAL(8,6),
            conviction_score DECIMAL(8,6),
            recency_weight DECIMAL(8,6),
            trade_count_window INTEGER,
            realized_pnl_window DECIMAL(18,4),
            win_rate DECIMAL(6,4),
            profit_factor DECIMAL(8,4),
            max_drawdown DECIMAL(10,4),
            avg_hold_hours DECIMAL(10,2),
            is_scoreable BOOLEAN,
            PRIMARY KEY (wallet, score_date)
        )
    """)

    # Aggregate signal time series (computed every 15 minutes)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            signal_ts TIMESTAMP PRIMARY KEY,
            coin VARCHAR NOT NULL DEFAULT 'xyz:CL',
            weighted_net_long DECIMAL(18,4),
            weighted_net_short DECIMAL(18,4),
            composite_signal DECIMAL(10,6),
            delta_1h DECIMAL(10,6),
            delta_4h DECIMAL(10,6),
            delta_24h DECIMAL(10,6),
            hhi_longs DECIMAL(10,6),
            hhi_shorts DECIMAL(10,6),
            mark_px DECIMAL(18,6),
            funding_rate DECIMAL(12,8),
            open_interest DECIMAL(18,4),
            daily_volume DECIMAL(18,2),
            is_nymex_open BOOLEAN,
            is_weekend BOOLEAN,
            scored_wallets_count INTEGER,
            direction VARCHAR,
            confidence DECIMAL(6,4)
        )
    """)

    # Market data candles
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            ts TIMESTAMP NOT NULL,
            coin VARCHAR NOT NULL DEFAULT 'xyz:CL',
            interval VARCHAR NOT NULL,
            open DECIMAL(18,6),
            high DECIMAL(18,6),
            low DECIMAL(18,6),
            close DECIMAL(18,6),
            volume DECIMAL(18,4),
            num_trades INTEGER,
            PRIMARY KEY (ts, coin, interval)
        )
    """)

    # Create indexes for common queries
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_ts ON fills(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_buyer ON fills(buyer)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_seller ON fills(seller)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_position_snapshots_wallet ON position_snapshots(wallet)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(signal_ts)")

    conn.commit()


def get_table_stats(conn: duckdb.DuckDBPyConnection = None) -> dict:
    """Get row counts for all tables."""
    if conn is None:
        conn = get_connection()

    tables = ["fills", "position_snapshots", "wallet_registry", "wallet_scores", "signals", "market_data"]
    stats = {}

    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        stats[table] = result[0] if result else 0

    return stats


if __name__ == "__main__":
    print(f"Initializing database at {DB_PATH}")
    conn = get_connection()
    init_schema(conn)
    print("Schema initialized successfully.")
    stats = get_table_stats(conn)
    print(f"Table stats: {stats}")
    conn.close()
