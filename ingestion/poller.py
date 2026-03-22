"""
REST API poller for Hyperliquid.

Polls clearinghouseState for top wallets and market metadata.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import aiohttp

from config import (
    HL_API_URL,
    HL_DEX,
    CL_COIN,
    POSITION_POLL_INTERVAL_SECONDS,
    MAX_TRACKED_WALLETS,
)
from db.schema import get_connection
from utils.rate_limiter import get_rate_limiter
from utils.nymex_hours import is_nymex_open

logger = logging.getLogger(__name__)


class HyperliquidPoller:
    """
    REST API poller for supplementary data.

    Polls:
    1. clearinghouseState for top N wallets (validate positions)
    2. metaAndAssetCtxs for market context
    """

    def __init__(self, get_top_wallets_fn=None):
        """
        Args:
            get_top_wallets_fn: Function that returns list of (wallet, position) tuples.
                               Should be ws_client.get_top_positions.
        """
        self.get_top_wallets = get_top_wallets_fn
        self.running = False
        self.rate_limiter = get_rate_limiter()

        # Latest market context
        self.market_context: Dict[str, Any] = {}

        # Stats
        self.polls_completed = 0
        self.last_poll_ts: Optional[datetime] = None
        self.wallets_polled = 0

    async def _post(self, session: aiohttp.ClientSession, payload: dict, weight: int = 2) -> Optional[dict]:
        """Make a POST request to Hyperliquid API with rate limiting."""
        if not await self.rate_limiter.acquire(weight, timeout=30):
            logger.warning(f"Rate limit exceeded, skipping request: {payload.get('type')}")
            return None

        try:
            async with session.post(HL_API_URL, json=payload) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"API error {resp.status}: {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    async def poll_market_context(self, session: aiohttp.ClientSession) -> Optional[dict]:
        """
        Fetch aggregate market data for xyz:WTIOIL.

        Returns markPx, oraclePx, funding, openInterest, dayNtlVlm, prevDayPx.
        """
        payload = {"type": "metaAndAssetCtxs", "dex": HL_DEX}
        data = await self._post(session, payload, weight=20)

        if not data:
            return None

        # data is [meta, [assetCtx1, assetCtx2, ...]]
        # Find the context for xyz:WTIOIL
        try:
            meta = data[0] if isinstance(data, list) else data.get("meta", {})
            ctxs = data[1] if isinstance(data, list) and len(data) > 1 else data.get("assetCtxs", [])

            # Get the universe list to find our coin's index
            universe = meta.get("universe", [])
            coin_idx = None
            for i, asset in enumerate(universe):
                if asset.get("name") == CL_COIN or asset.get("szDecimals") is not None:
                    # Match by name or find WTIOIL
                    if "WTIOIL" in str(asset.get("name", "")):
                        coin_idx = i
                        break

            if coin_idx is not None and coin_idx < len(ctxs):
                ctx = ctxs[coin_idx]
                self.market_context = {
                    "mark_px": float(ctx.get("markPx", 0)),
                    "oracle_px": float(ctx.get("oraclePx", 0)) if ctx.get("oraclePx") else None,
                    "funding": float(ctx.get("funding", 0)),
                    "open_interest": float(ctx.get("openInterest", 0)),
                    "day_ntl_vlm": float(ctx.get("dayNtlVlm", 0)),
                    "prev_day_px": float(ctx.get("prevDayPx", 0)) if ctx.get("prevDayPx") else None,
                }
                return self.market_context

        except Exception as e:
            logger.error(f"Error parsing market context: {e}")

        return None

    async def poll_wallet_positions(
        self, session: aiohttp.ClientSession, wallets: List[str]
    ) -> Dict[str, dict]:
        """
        Fetch clearinghouseState for a list of wallets.

        Returns dict mapping wallet -> position data.
        """
        results = {}
        conn = get_connection()
        now = datetime.now(timezone.utc)
        is_open = is_nymex_open(now)

        for wallet in wallets:
            payload = {"type": "clearinghouseState", "user": wallet, "dex": HL_DEX}
            data = await self._post(session, payload, weight=2)

            if not data:
                continue

            try:
                # Find xyz:WTIOIL position in assetPositions
                asset_positions = data.get("assetPositions", [])
                cl_position = None

                for ap in asset_positions:
                    pos = ap.get("position", {})
                    if pos.get("coin") == CL_COIN:
                        cl_position = pos
                        break

                if cl_position:
                    margin_summary = data.get("marginSummary", {})

                    position_data = {
                        "szi": float(cl_position.get("szi", 0)),
                        "entry_px": float(cl_position.get("entryPx", 0)),
                        "leverage": float(cl_position.get("leverage", {}).get("value", 0)),
                        "liquidation_px": float(cl_position.get("liquidationPx", 0)) if cl_position.get("liquidationPx") else None,
                        "unrealized_pnl": float(cl_position.get("unrealizedPnl", 0)),
                        "return_on_equity": float(cl_position.get("returnOnEquity", 0)),
                        "account_value": float(margin_summary.get("accountValue", 0)),
                        "margin_used": float(margin_summary.get("totalMarginUsed", 0)),
                    }

                    results[wallet] = position_data

                    # Insert snapshot into database
                    conn.execute(
                        """
                        INSERT INTO position_snapshots
                        (snapshot_ts, wallet, coin, szi, entry_px, leverage, liquidation_px,
                         unrealized_pnl, return_on_equity, account_value, margin_used, is_nymex_open)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            now, wallet, CL_COIN,
                            position_data["szi"],
                            position_data["entry_px"],
                            position_data["leverage"],
                            position_data["liquidation_px"],
                            position_data["unrealized_pnl"],
                            position_data["return_on_equity"],
                            position_data["account_value"],
                            position_data["margin_used"],
                            is_open,
                        ]
                    )

            except Exception as e:
                logger.error(f"Error parsing position for {wallet}: {e}")

        conn.commit()
        conn.close()

        self.wallets_polled = len(results)
        return results

    async def poll_cycle(self) -> None:
        """Run one complete polling cycle."""
        async with aiohttp.ClientSession() as session:
            # Poll market context
            await self.poll_market_context(session)

            # Poll top wallet positions
            if self.get_top_wallets:
                top_wallets = self.get_top_wallets(MAX_TRACKED_WALLETS)
                wallet_addrs = [w[0] for w in top_wallets]

                if wallet_addrs:
                    await self.poll_wallet_positions(session, wallet_addrs)

        self.polls_completed += 1
        self.last_poll_ts = datetime.now(timezone.utc)

        logger.info(
            f"Poll cycle {self.polls_completed} complete. "
            f"Wallets: {self.wallets_polled}, "
            f"Rate limit: {self.rate_limiter.stats()['utilization_pct']}%"
        )

    async def run(self) -> None:
        """Main polling loop."""
        self.running = True
        logger.info(f"Starting poller with {POSITION_POLL_INTERVAL_SECONDS}s interval")

        while self.running:
            try:
                await self.poll_cycle()
            except Exception as e:
                logger.error(f"Poll cycle error: {e}")

            await asyncio.sleep(POSITION_POLL_INTERVAL_SECONDS)

    async def stop(self) -> None:
        """Stop the poller."""
        self.running = False

    def stats(self) -> dict:
        """Get poller statistics."""
        return {
            "polls_completed": self.polls_completed,
            "last_poll_ts": self.last_poll_ts.isoformat() if self.last_poll_ts else None,
            "wallets_polled": self.wallets_polled,
            "market_context": self.market_context,
            "rate_limiter": self.rate_limiter.stats(),
        }


async def main():
    """Test the poller standalone."""
    logging.basicConfig(level=logging.INFO)

    # Dummy wallet provider for testing
    def get_test_wallets(n):
        return []  # No wallets to poll in test mode

    poller = HyperliquidPoller(get_top_wallets_fn=get_test_wallets)

    try:
        # Run just one cycle for testing
        await poller.poll_cycle()
        print(f"Market context: {poller.market_context}")
        print(f"Stats: {poller.stats()}")
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
