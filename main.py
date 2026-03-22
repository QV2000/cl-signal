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
from config import DB_PATH

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
        self.running = False

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

        # Create poller (uses ws_client for top wallets)
        self.poller = HyperliquidPoller(
            get_top_wallets_fn=self.ws_client.get_top_positions
        )

        # Create Telegram bot
        self.telegram_bot = TelegramBot(
            get_ws_stats=self.ws_client.stats,
            get_poller_stats=self.poller.stats,
            get_positions=self.ws_client.get_top_positions,
        )

        self.running = True

        # Start all components
        await self.telegram_bot.start()

        # Run WebSocket and poller concurrently
        await asyncio.gather(
            self.ws_client.run(),
            self.poller.run(),
            self._health_monitor(),
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
