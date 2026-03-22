"""
CL Signal System - Main Entry Point

Orchestrates:
1. DuckDB schema initialization
2. WebSocket client for real-time fills
3. REST poller for position validation and market context
4. Telegram bot for commands and alerts
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

from db.schema import init_schema, get_connection, get_table_stats
from ingestion.websocket_client import HyperliquidWebSocket
from ingestion.poller import HyperliquidPoller
from alerting.telegram_bot import TelegramBot
from scoring.runner import ScoringPipeline
from signal_engine.composer import SignalEngine
from config import DB_PATH, is_weekend_window

# Intervals
SCORING_INTERVAL_SECONDS = 3600  # Run scoring every hour
SIGNAL_INTERVAL_SECONDS = 900   # Generate signal every 15 minutes

# Configure logging (stdout only for Docker)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class CLSignalSystem:
    """
    Main orchestrator for the CL Signal System.
    """

    def __init__(self):
        self.ws_client: HyperliquidWebSocket = None
        self.poller: HyperliquidPoller = None
        self.telegram_bot: TelegramBot = None
        self.scoring_pipeline: ScoringPipeline = None
        self.signal_engine: SignalEngine = None
        self.db_conn = None
        self.running = False
        # Weekend tracking
        self.friday_captured_week = None
        self.sunday_captured_week = None

    async def start(self) -> None:
        """Initialize and start all components."""
        logger.info("Starting CL Signal System")
        logger.info(f"Database: {DB_PATH}")

        # Initialize database schema
        logger.info("Initializing database schema...")
        conn = get_connection()
        init_schema(conn)
        stats = get_table_stats(conn)
        conn.close()
        logger.info(f"Database initialized. Tables: {stats}")

        # Create WebSocket client
        self.ws_client = HyperliquidWebSocket(
            on_trade=self._on_trade,
            on_market_data=self._on_market_data,
        )
        self.ws_client.on_flow_alert = self._on_flow_alert

        # Create poller (uses ws_client for top wallets)
        self.poller = HyperliquidPoller(
            get_top_wallets_fn=self.ws_client.get_top_positions
        )

        # Create Telegram bot
        self.telegram_bot = TelegramBot(
            get_ws_stats=self.ws_client.stats,
            get_poller_stats=self.poller.stats,
            get_positions=self.ws_client.get_top_positions,
            get_flow_signals=self.ws_client.get_flow_signals,
        )

        # Create scoring pipeline and signal engine
        self.db_conn = get_connection()
        self.scoring_pipeline = ScoringPipeline(self.db_conn)
        self.signal_engine = SignalEngine(self.db_conn)

        self.running = True

        # Start all components
        await self.telegram_bot.start()

        # Run WebSocket, poller, scoring, and signal generation concurrently
        await asyncio.gather(
            self.ws_client.run(),
            self.poller.run(),
            self._health_monitor(),
            self._scoring_scheduler(),
            self._signal_scheduler(),
            self._weekend_scheduler(),
            self._flow_outcome_backfill(),
        )

    async def stop(self) -> None:
        """Stop all components gracefully."""
        logger.info("Stopping CL Signal System...")
        self.running = False

        if self.ws_client:
            await self.ws_client.stop()

        if self.poller:
            await self.poller.stop()

        if self.telegram_bot:
            await self.telegram_bot.stop()

        logger.info("CL Signal System stopped")

    def _on_trade(self, trade: dict) -> None:
        """Callback for incoming trades."""
        # Log significant trades
        if trade["notional_usd"] > 10000:
            logger.info(
                f"Large trade: {trade['buyer'][:10]}... "
                f"{'bought' if trade['side'] == 'B' else 'sold'} "
                f"{trade['size']:.2f} @ ${trade['price']:.2f} "
                f"(${trade['notional_usd']:,.0f})"
            )

    def _on_market_data(self, data: dict) -> None:
        """Callback for market data updates."""
        # Could trigger signal recomputation here
        pass

    def _on_flow_alert(self, alert: dict) -> None:
        """Callback for flow signal alerts."""
        try:
            # Log alert to database
            self.db_conn.execute("""
                INSERT INTO flow_alerts
                (alert_ts, direction, signal_30s, signal_2m, signal_5m, mark_px_at_alert)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                alert["timestamp"],
                alert["direction"],
                alert["signal_30s"],
                alert["signal_2m"],
                alert["signal_5m"],
                alert["mark_px"]
            ])

            # Format and send Telegram message
            msg = self.ws_client.flow_alert_manager.format_alert_message(alert)
            logger.info(f"Flow alert: {alert['direction']} at ${alert['mark_px']:.2f}")

            # Send Telegram (non-blocking)
            asyncio.create_task(self.telegram_bot.send_message(msg))

        except Exception as e:
            logger.error(f"Error handling flow alert: {e}")

    async def _health_monitor(self) -> None:
        """Monitor system health and send alerts."""
        last_fill_ts = None
        alert_sent = False

        while self.running:
            await asyncio.sleep(60)

            ws_stats = self.ws_client.stats()
            current_fill_ts = ws_stats.get("last_fill_ts")

            # Check for data gaps
            if current_fill_ts == last_fill_ts and last_fill_ts is not None:
                # No new fills in the last minute
                if not alert_sent:
                    logger.warning("No new fills received in 60 seconds")
                    # Could send Telegram alert here after 5 minutes
            else:
                alert_sent = False

            last_fill_ts = current_fill_ts

            # Log periodic health stats
            stats = get_table_stats()
            logger.info(
                f"Health: fills={stats['fills']}, "
                f"wallets={stats['wallet_registry']}, "
                f"snapshots={stats['position_snapshots']}"
            )

    async def _scoring_scheduler(self) -> None:
        """Run wallet scoring pipeline periodically."""
        # Wait before first run to accumulate data
        await asyncio.sleep(300)  # 5 minutes initial delay

        while self.running:
            try:
                logger.info("Running scoring pipeline...")
                results = self.scoring_pipeline.run()
                logger.info(f"Scoring complete: {results}")

                # Update wallet scores for flow signals
                self._load_wallet_scores_for_flow()
            except Exception as e:
                logger.error(f"Scoring pipeline error: {e}", exc_info=True)

            await asyncio.sleep(SCORING_INTERVAL_SECONDS)

    def _load_wallet_scores_for_flow(self) -> None:
        """Load wallet scores into memory for flow signal computation."""
        try:
            result = self.db_conn.execute("""
                WITH latest AS (
                    SELECT wallet, overall_score,
                           ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY score_date DESC) as rn
                    FROM wallet_scores WHERE is_scoreable = TRUE
                )
                SELECT wallet, overall_score FROM latest WHERE rn = 1
            """).fetchall()

            scores = {row[0]: float(row[1]) if row[1] else 0.0 for row in result}
            self.ws_client.update_wallet_scores(scores)
            logger.info(f"Loaded {len(scores)} wallet scores for flow signals")
        except Exception as e:
            logger.error(f"Error loading wallet scores: {e}")

    async def _flow_outcome_backfill(self) -> None:
        """Backfill flow alert outcomes (1m and 5m forward prices)."""
        from datetime import timedelta

        while self.running:
            await asyncio.sleep(60)

            try:
                now = datetime.now(timezone.utc)
                current_px = self.ws_client.get_mark_price()
                if not current_px:
                    continue

                # Fill 1-minute outcomes (alerts from ~60 seconds ago)
                one_min_ago_start = now - timedelta(seconds=70)
                one_min_ago_end = now - timedelta(seconds=50)

                alerts_1m = self.db_conn.execute("""
                    SELECT alert_ts, direction, mark_px_at_alert
                    FROM flow_alerts
                    WHERE alert_ts BETWEEN ? AND ?
                    AND mark_px_1m_later IS NULL
                """, [one_min_ago_start, one_min_ago_end]).fetchall()

                for alert_ts, direction, px_at_alert in alerts_1m:
                    move = current_px - float(px_at_alert)
                    correct = (move > 0 and direction == "LONG") or (move < 0 and direction == "SHORT")
                    self.db_conn.execute("""
                        UPDATE flow_alerts
                        SET mark_px_1m_later = ?, correct_1m = ?
                        WHERE alert_ts = ?
                    """, [current_px, correct, alert_ts])

                # Fill 5-minute outcomes (alerts from ~5 minutes ago)
                five_min_ago_start = now - timedelta(seconds=310)
                five_min_ago_end = now - timedelta(seconds=290)

                alerts_5m = self.db_conn.execute("""
                    SELECT alert_ts, direction, mark_px_at_alert
                    FROM flow_alerts
                    WHERE alert_ts BETWEEN ? AND ?
                    AND mark_px_5m_later IS NULL
                """, [five_min_ago_start, five_min_ago_end]).fetchall()

                for alert_ts, direction, px_at_alert in alerts_5m:
                    move = current_px - float(px_at_alert)
                    correct = (move > 0 and direction == "LONG") or (move < 0 and direction == "SHORT")
                    self.db_conn.execute("""
                        UPDATE flow_alerts
                        SET mark_px_5m_later = ?, correct_5m = ?
                        WHERE alert_ts = ?
                    """, [current_px, correct, alert_ts])

            except Exception as e:
                logger.error(f"Flow outcome backfill error: {e}")

    async def _signal_scheduler(self) -> None:
        """Generate composite signal periodically."""
        # Wait before first run
        await asyncio.sleep(60)

        while self.running:
            try:
                now = datetime.now(timezone.utc)
                signal = self.signal_engine.generate_and_persist()

                # Compute weekend signal if in weekend window
                weekend_composite = None
                if is_weekend_window(now):
                    weekend_composite = self._compute_weekend_signal(now)

                logger.info(
                    f"Signal: {signal.direction} ({signal.composite_signal:+.3f}), "
                    f"confidence={signal.confidence:.2f}, "
                    f"wallets={signal.scored_wallets}/{signal.total_wallets}"
                    + (f", weekend={weekend_composite:+.3f}" if weekend_composite is not None else "")
                )
            except Exception as e:
                logger.error(f"Signal generation error: {e}", exc_info=True)

            await asyncio.sleep(SIGNAL_INTERVAL_SECONDS)

    def _compute_weekend_signal(self, now: datetime) -> float:
        """Compute weekend-specific signal weighting position changes."""
        weekend_id = now.strftime("%G-W%V")

        # Load Friday close baseline
        friday_positions = {}
        result = self.db_conn.execute("""
            SELECT wallet, position_size FROM friday_close_positions
            WHERE weekend_id = ?
        """, [weekend_id]).fetchall()
        for wallet, pos in result:
            friday_positions[wallet] = float(pos) if pos else 0.0

        if not friday_positions:
            return None

        # Get current positions
        current_positions = {}
        pos_result = self.db_conn.execute("""
            WITH latest AS (
                SELECT wallet, szi,
                       ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY snapshot_ts DESC) as rn
                FROM position_snapshots
            )
            SELECT wallet, szi FROM latest WHERE rn = 1 AND szi != 0
        """).fetchall()
        for wallet, szi in pos_result:
            current_positions[wallet] = float(szi)

        # Get wallet scores
        wallet_scores = {}
        score_result = self.db_conn.execute("""
            WITH latest AS (
                SELECT wallet, overall_score,
                       ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY score_date DESC) as rn
                FROM wallet_scores WHERE is_scoreable = TRUE
            )
            SELECT wallet, overall_score FROM latest WHERE rn = 1
        """).fetchall()
        for wallet, score in score_result:
            wallet_scores[wallet] = float(score) if score else 0.0

        weighted_sum = 0.0
        abs_sum = 0.0
        active_count = 0

        for wallet, current_pos in current_positions.items():
            score = wallet_scores.get(wallet, 0.0)
            if score == 0.0 or abs(current_pos) < 0.01:
                continue

            friday_pos = friday_positions.get(wallet, 0.0)
            change = current_pos - friday_pos

            if abs(change) > 0.01:
                # Wallet actively traded this weekend. Full weight on change.
                effective_pos = (0.3 * friday_pos) + (1.0 * change)
                active_count += 1
            else:
                # Wallet did not trade this weekend. Reduced influence.
                effective_pos = 0.3 * current_pos

            weighted_sum += score * effective_pos
            abs_sum += abs(score * effective_pos)

        weekend_composite = weighted_sum / abs_sum if abs_sum > 0 else 0.0

        # Store in signals table
        try:
            self.db_conn.execute("""
                UPDATE signals SET weekend_composite = ?
                WHERE signal_ts = (SELECT MAX(signal_ts) FROM signals)
            """, [weekend_composite])
        except:
            pass

        return weekend_composite

    async def _weekend_scheduler(self) -> None:
        """Handle Friday close snapshot and Sunday gap check."""
        while self.running:
            await asyncio.sleep(60)  # Check every minute

            now = datetime.now(timezone.utc)
            dow = now.weekday()
            hour = now.hour
            minute = now.minute
            weekend_id = now.strftime("%G-W%V")

            # Friday close snapshot at 21:15-21:16 UTC
            if dow == 4 and hour == 21 and 15 <= minute < 16:
                if self.friday_captured_week != weekend_id:
                    await self._capture_friday_close(weekend_id)
                    self.friday_captured_week = weekend_id

            # Sunday gap check at 22:30-22:31 UTC
            if dow == 6 and hour == 22 and 30 <= minute < 31:
                if self.sunday_captured_week != weekend_id:
                    await self._check_sunday_gap(weekend_id)
                    self.sunday_captured_week = weekend_id

    async def _capture_friday_close(self, weekend_id: str) -> None:
        """Capture Friday close positions for weekend tracking."""
        try:
            # Get current positions from in-memory tracking (returns list of tuples)
            positions = self.ws_client.get_top_positions(n=500)
            if not positions:
                logger.warning("No positions to capture for Friday close")
                return

            # Get current mark price
            mark_px = self.ws_client.get_mark_price()
            if not mark_px:
                mark_px = 0.0

            # Insert into database
            count = 0
            for wallet, szi in positions:
                if abs(szi) > 0.01:
                    self.db_conn.execute("""
                        INSERT OR REPLACE INTO friday_close_positions
                        (weekend_id, wallet, position_size, mark_px)
                        VALUES (?, ?, ?, ?)
                    """, [weekend_id, wallet, szi, mark_px])
                    count += 1

            logger.info(f"Friday close captured: {count} wallets, mark_px={mark_px:.2f}")

            # Send Telegram notification
            await self.telegram_bot.send_message(
                f"FRIDAY CLOSE CAPTURED: {weekend_id}\n\n"
                f"Wallets: {count}\n"
                f"Mark price: ${mark_px:.2f}"
            )

        except Exception as e:
            logger.error(f"Friday close capture error: {e}", exc_info=True)

    async def _check_sunday_gap(self, weekend_id: str) -> None:
        """Check weekend gap and compare to signal prediction."""
        try:
            # Get Friday close price
            result = self.db_conn.execute("""
                SELECT mark_px FROM friday_close_positions
                WHERE weekend_id = ? LIMIT 1
            """, [weekend_id]).fetchone()

            if not result:
                logger.warning(f"No Friday close data for {weekend_id}")
                return

            friday_px = float(result[0])

            # Get current price
            sunday_px = self.ws_client.get_mark_price()
            if not sunday_px:
                logger.warning("Could not get Sunday reopen price")
                return

            # Compute gap
            gap_pct = (sunday_px - friday_px) / friday_px * 100

            if gap_pct > 0.05:
                gap_direction = "UP"
            elif gap_pct < -0.05:
                gap_direction = "DOWN"
            else:
                gap_direction = "FLAT"

            # Get last weekend signal
            sig_result = self.db_conn.execute("""
                SELECT weekend_composite FROM signals
                WHERE weekend_composite IS NOT NULL
                ORDER BY signal_ts DESC LIMIT 1
            """).fetchone()

            weekend_signal = float(sig_result[0]) if sig_result and sig_result[0] else 0.0

            if weekend_signal > 0.1:
                signal_direction = "LONG"
            elif weekend_signal < -0.1:
                signal_direction = "SHORT"
            else:
                signal_direction = "FLAT"

            # Determine if correct
            correct = (gap_direction == "FLAT") or (
                (signal_direction == "LONG" and gap_direction == "UP") or
                (signal_direction == "SHORT" and gap_direction == "DOWN")
            )

            # Insert result
            self.db_conn.execute("""
                INSERT OR REPLACE INTO weekend_gap_results
                (weekend_id, friday_close_px, sunday_reopen_px, gap_pct,
                 gap_direction, weekend_signal_value, signal_direction, correct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [weekend_id, friday_px, sunday_px, gap_pct,
                  gap_direction, weekend_signal, signal_direction, correct])

            # Get season record
            record = self.db_conn.execute("""
                SELECT COUNT(*) FILTER (WHERE correct), COUNT(*)
                FROM weekend_gap_results
            """).fetchone()

            wins = record[0] or 0
            total = record[1] or 0
            pct = (wins / total * 100) if total > 0 else 0

            result_text = "CORRECT" if correct else "WRONG"

            logger.info(f"Weekend gap result: {gap_direction} ({gap_pct:+.2f}%), signal was {signal_direction}, {result_text}")

            # Send Telegram notification
            await self.telegram_bot.send_message(
                f"WEEKEND GAP RESULT: {weekend_id}\n\n"
                f"Friday close: ${friday_px:.2f}\n"
                f"Sunday reopen: ${sunday_px:.2f}\n"
                f"Gap: {gap_pct:+.2f}% ({gap_direction})\n\n"
                f"Weekend signal: {weekend_signal:+.2f} ({signal_direction})\n"
                f"Result: {result_text}\n\n"
                f"Season record: {wins}/{total} ({pct:.0f}%)"
            )

        except Exception as e:
            logger.error(f"Sunday gap check error: {e}", exc_info=True)


def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}. Initiating shutdown...")
    raise KeyboardInterrupt


async def main():
    """Main entry point."""
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    system = CLSignalSystem()

    try:
        await system.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await system.stop()


if __name__ == "__main__":
    asyncio.run(main())
