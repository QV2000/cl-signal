"""
WebSocket client for Hyperliquid trades, activeAssetCtx, bbo, and l2Book channels.

Captures all fills on xyz:CL with buyer and seller addresses.
Stores L2 order book snapshots every 5 seconds.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Callable, Any
from collections import defaultdict

import websockets
from websockets.exceptions import ConnectionClosed

from config import HL_WS_URL, CL_COIN
from db.schema import get_connection
from utils.nymex_hours import is_nymex_open

logger = logging.getLogger(__name__)


class HyperliquidWebSocket:
    """
    WebSocket client for Hyperliquid market data.

    Subscribes to trades, activeAssetCtx, and bbo channels for CL_COIN.
    Maintains in-memory position tracking and persists fills to DuckDB.
    """

    def __init__(
        self,
        on_trade: Optional[Callable[[dict], None]] = None,
        on_market_data: Optional[Callable[[dict], None]] = None,
    ):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 1.0
        self.max_reconnect_delay = 60.0

        # In-memory position tracking: wallet -> net_position
        self.positions: Dict[str, float] = defaultdict(float)

        # Latest market data
        self.mark_price: Optional[float] = None
        self.funding_rate: Optional[float] = None
        self.open_interest: Optional[float] = None
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None

        # Callbacks
        self.on_trade = on_trade
        self.on_market_data = on_market_data

        # Stats
        self.fills_received = 0
        self.last_fill_ts: Optional[datetime] = None
        self.connect_time: Optional[datetime] = None
        self.disconnect_time: Optional[datetime] = None

        # L2 order book (latest snapshot in memory)
        self.l2_bids: list = []  # [(price, size), ...]
        self.l2_asks: list = []
        self.l2_last_save: Optional[datetime] = None
        self.l2_snapshots_saved = 0
        self.l2_snapshot_interval = 5.0  # seconds

    async def connect(self) -> None:
        """Establish WebSocket connection and subscribe to channels."""
        logger.info(f"Connecting to {HL_WS_URL}")
        self.ws = await websockets.connect(HL_WS_URL, ping_interval=30, ping_timeout=10)
        self.connect_time = datetime.now(timezone.utc)
        self.reconnect_delay = 1.0
        logger.info("WebSocket connected")

        # Subscribe to all channels
        await self._subscribe()

    async def _subscribe(self) -> None:
        """Send subscription messages for all channels."""
        subscriptions = [
            {"method": "subscribe", "subscription": {"type": "trades", "coin": CL_COIN}},
            {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": CL_COIN}},
            {"method": "subscribe", "subscription": {"type": "bbo", "coin": CL_COIN}},
            {"method": "subscribe", "subscription": {"type": "l2Book", "coin": CL_COIN}},
        ]

        for sub in subscriptions:
            await self.ws.send(json.dumps(sub))
            logger.info(f"Subscribed to {sub['subscription']['type']} for {CL_COIN}")

    async def _handle_message(self, message: str) -> None:
        """Parse and route incoming WebSocket messages."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON received: {message[:100]}")
            return

        channel = data.get("channel")

        if channel == "trades":
            await self._handle_trades(data.get("data", []))
        elif channel == "activeAssetCtx":
            await self._handle_active_asset_ctx(data.get("data", {}))
        elif channel == "bbo":
            await self._handle_bbo(data.get("data", {}))
        elif channel == "l2Book":
            await self._handle_l2_book(data.get("data", {}))
        elif channel == "subscriptionResponse":
            logger.debug(f"Subscription response: {data}")
        else:
            logger.debug(f"Unknown channel: {channel}")

    async def _handle_trades(self, trades: list) -> None:
        """
        Process trade messages.

        Each trade has:
        - coin: "xyz:WTIOIL"
        - side: "B" (buy) or "A" (sell) - taker side
        - px: price
        - sz: size
        - time: milliseconds timestamp
        - hash: transaction hash
        - tid: trade id
        - users: [buyer_address, seller_address]
        """
        conn = get_connection()

        for trade in trades:
            if trade.get("coin") != CL_COIN:
                continue

            users = trade.get("users", [])
            if len(users) < 2:
                logger.warning(f"Trade missing user addresses: {trade}")
                continue

            buyer = users[0]
            seller = users[1]
            size = float(trade.get("sz", 0))
            price = float(trade.get("px", 0))
            taker_side = trade.get("side", "")
            ts_ms = trade.get("time", 0)
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            tid = trade.get("tid")
            tx_hash = trade.get("hash")

            # Update in-memory positions
            # Buyer increases position, seller decreases
            self.positions[buyer] += size
            self.positions[seller] -= size

            # Calculate notional
            notional_usd = price * size

            # Create unique fill_id
            fill_id = f"{tid}" if tid else f"{tx_hash}_{ts_ms}"

            # Insert into database
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO fills
                    (fill_id, ts, coin, buyer, seller, taker_side, px, sz, hash, tid, notional_usd)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [fill_id, ts, CL_COIN, buyer, seller, taker_side, price, size, tx_hash, tid, notional_usd]
                )

                # Update wallet registry for both parties
                for wallet in [buyer, seller]:
                    conn.execute(
                        """
                        INSERT INTO wallet_registry (wallet, first_seen, last_seen, total_fills, total_volume_usd)
                        VALUES (?, ?, ?, 1, ?)
                        ON CONFLICT(wallet) DO UPDATE SET
                            last_seen = excluded.last_seen,
                            total_fills = wallet_registry.total_fills + 1,
                            total_volume_usd = wallet_registry.total_volume_usd + excluded.total_volume_usd
                        """,
                        [wallet, ts, ts, notional_usd]
                    )

                conn.commit()
            except Exception as e:
                logger.error(f"Error inserting fill: {e}")

            self.fills_received += 1
            self.last_fill_ts = ts

            # Callback
            if self.on_trade:
                self.on_trade({
                    "buyer": buyer,
                    "seller": seller,
                    "size": size,
                    "price": price,
                    "side": taker_side,
                    "ts": ts,
                    "notional_usd": notional_usd,
                })

        conn.close()

    async def _handle_active_asset_ctx(self, data: dict) -> None:
        """
        Process activeAssetCtx messages.

        Contains: markPx, oraclePx, funding, openInterest, etc.
        """
        ctx = data.get("ctx", {})

        self.mark_price = float(ctx.get("markPx", 0)) if ctx.get("markPx") else None
        self.funding_rate = float(ctx.get("funding", 0)) if ctx.get("funding") else None
        self.open_interest = float(ctx.get("openInterest", 0)) if ctx.get("openInterest") else None

        if self.on_market_data:
            self.on_market_data({
                "type": "activeAssetCtx",
                "mark_price": self.mark_price,
                "funding_rate": self.funding_rate,
                "open_interest": self.open_interest,
            })

    async def _handle_bbo(self, data: dict) -> None:
        """
        Process best bid/offer messages.

        Contains: bid, ask prices and sizes.
        """
        self.best_bid = float(data.get("bid", 0)) if data.get("bid") else None
        self.best_ask = float(data.get("ask", 0)) if data.get("ask") else None

        if self.on_market_data:
            self.on_market_data({
                "type": "bbo",
                "best_bid": self.best_bid,
                "best_ask": self.best_ask,
            })

    async def _handle_l2_book(self, data: dict) -> None:
        """
        Process L2 order book messages.

        Contains: levels array with [{"px": price, "sz": size, "n": numOrders}, ...]
        Stores full book in memory, persists snapshot every 5 seconds.
        """
        levels = data.get("levels", [])
        if len(levels) >= 2:
            # levels[0] = bids (sorted high to low), levels[1] = asks (sorted low to high)
            self.l2_bids = [(float(lvl["px"]), float(lvl["sz"])) for lvl in levels[0][:20]]
            self.l2_asks = [(float(lvl["px"]), float(lvl["sz"])) for lvl in levels[1][:20]]

        # Save snapshot periodically
        now = datetime.now(timezone.utc)
        if self.l2_last_save is None or (now - self.l2_last_save).total_seconds() >= self.l2_snapshot_interval:
            await self._save_l2_snapshot(now)
            self.l2_last_save = now

    async def _save_l2_snapshot(self, ts: datetime) -> None:
        """Persist current L2 order book to database."""
        if not self.l2_bids and not self.l2_asks:
            return

        conn = get_connection()
        try:
            # Insert bids (top 20 levels)
            for level, (price, size) in enumerate(self.l2_bids):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO orderbook_snapshots
                    (ts, coin, side, level, price, size)
                    VALUES (?, ?, 'B', ?, ?, ?)
                    """,
                    [ts, CL_COIN, level, price, size]
                )

            # Insert asks (top 20 levels)
            for level, (price, size) in enumerate(self.l2_asks):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO orderbook_snapshots
                    (ts, coin, side, level, price, size)
                    VALUES (?, ?, 'A', ?, ?, ?)
                    """,
                    [ts, CL_COIN, level, price, size]
                )

            conn.commit()
            self.l2_snapshots_saved += 1

        except Exception as e:
            logger.error(f"Error saving L2 snapshot: {e}")
        finally:
            conn.close()

    async def run(self) -> None:
        """Main run loop with automatic reconnection."""
        self.running = True

        while self.running:
            try:
                await self.connect()

                async for message in self.ws:
                    if not self.running:
                        break
                    await self._handle_message(message)

            except ConnectionClosed as e:
                self.disconnect_time = datetime.now(timezone.utc)
                logger.warning(f"WebSocket connection closed: {e}. Reconnecting in {self.reconnect_delay}s")

            except Exception as e:
                self.disconnect_time = datetime.now(timezone.utc)
                logger.error(f"WebSocket error: {e}. Reconnecting in {self.reconnect_delay}s")

            if self.running:
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)

    async def stop(self) -> None:
        """Stop the WebSocket client."""
        self.running = False
        if self.ws:
            await self.ws.close()

    def get_position(self, wallet: str) -> float:
        """Get estimated net position for a wallet."""
        return self.positions.get(wallet, 0.0)

    def get_top_positions(self, n: int = 20) -> list:
        """Get top N wallets by absolute position size."""
        sorted_positions = sorted(
            self.positions.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )
        return sorted_positions[:n]

    def get_mark_price(self) -> float:
        """Get current mark price for CL."""
        return self.mark_price

    def stats(self) -> dict:
        """Get client statistics."""
        return {
            "fills_received": self.fills_received,
            "last_fill_ts": self.last_fill_ts.isoformat() if self.last_fill_ts else None,
            "connect_time": self.connect_time.isoformat() if self.connect_time else None,
            "mark_price": self.mark_price,
            "funding_rate": self.funding_rate,
            "open_interest": self.open_interest,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "tracked_wallets": len(self.positions),
            "is_nymex_open": is_nymex_open(datetime.now(timezone.utc)),
            "l2_snapshots_saved": self.l2_snapshots_saved,
            "l2_bid_levels": len(self.l2_bids),
            "l2_ask_levels": len(self.l2_asks),
        }


async def main():
    """Test the WebSocket client standalone."""
    logging.basicConfig(level=logging.INFO)

    def on_trade(trade):
        logger.info(f"Trade: {trade['buyer'][:10]}... bought {trade['size']} @ {trade['price']}")

    def on_market_data(data):
        logger.info(f"Market data: {data}")

    client = HyperliquidWebSocket(on_trade=on_trade, on_market_data=on_market_data)

    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
