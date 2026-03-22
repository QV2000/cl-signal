"""Integration test - run WebSocket ingestion for 30s and verify data storage."""
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from db.schema import get_connection, init_schema, get_table_stats
from ingestion.websocket_client import HyperliquidWebSocket
from config import CL_COIN

async def main():
    # Initialize database
    logger.info("Initializing database...")
    conn = get_connection()
    init_schema(conn)

    # Get initial stats
    stats_before = get_table_stats(conn)
    logger.info(f"Before: {stats_before}")
    conn.close()

    # Create and run WebSocket client
    trades_count = [0]

    def on_trade(trade):
        trades_count[0] += 1
        if trades_count[0] <= 5 or trades_count[0] % 10 == 0:
            logger.info(
                f"Trade #{trades_count[0]}: "
                f"{trade['buyer'][:8]}... {trade['side']} {trade['size']:.3f} @ ${trade['price']:.2f}"
            )

    def on_market(data):
        if data.get("type") == "activeAssetCtx":
            logger.info(
                f"Market: mark=${data.get('mark_price', 0):.2f} "
                f"funding={data.get('funding_rate', 0):.6f} "
                f"OI=${data.get('open_interest', 0):,.0f}"
            )

    client = HyperliquidWebSocket(on_trade=on_trade, on_market_data=on_market)

    logger.info(f"Connecting to {CL_COIN} WebSocket feed...")

    async def run_for_duration(seconds):
        await client.connect()
        logger.info(f"Connected! Collecting data for {seconds}s...")

        start = asyncio.get_event_loop().time()
        try:
            async for msg in client.ws:
                await client._handle_message(msg)
                if asyncio.get_event_loop().time() - start > seconds:
                    break
        except Exception as e:
            logger.error(f"WebSocket error: {e}")

    await run_for_duration(30)

    # Get final stats
    conn = get_connection()
    stats_after = get_table_stats(conn)

    logger.info("=" * 50)
    logger.info("RESULTS")
    logger.info("=" * 50)
    logger.info(f"Trades received: {trades_count[0]}")
    logger.info(f"Fills in DB: {stats_after['fills']} (was {stats_before['fills']})")
    logger.info(f"Wallets in registry: {stats_after['wallet_registry']} (was {stats_before['wallet_registry']})")

    # Show some sample data
    logger.info("")
    logger.info("Sample fills:")
    fills = conn.execute("""
        SELECT ts, buyer, seller, taker_side, sz, px, notional_usd
        FROM fills
        ORDER BY ts DESC
        LIMIT 5
    """).fetchall()
    for f in fills:
        logger.info(f"  {f[0]} | {f[1][:8]}... -> {f[2][:8]}... | {f[3]} {f[4]:.3f} @ ${f[5]:.2f} (${f[6]:,.0f})")

    logger.info("")
    logger.info("Top wallets by volume:")
    wallets = conn.execute("""
        SELECT wallet, total_fills, total_volume_usd
        FROM wallet_registry
        ORDER BY total_volume_usd DESC
        LIMIT 5
    """).fetchall()
    for w in wallets:
        logger.info(f"  {w[0][:12]}... | {w[1]} fills | ${w[2]:,.0f} volume")

    logger.info("")
    logger.info("In-memory position estimates:")
    positions = client.get_top_positions(5)
    for wallet, pos in positions:
        direction = "LONG" if pos > 0 else "SHORT"
        logger.info(f"  {wallet[:12]}... | {direction} {abs(pos):.3f}")

    conn.close()
    logger.info("")
    logger.info("Integration test complete!")

if __name__ == "__main__":
    asyncio.run(main())
