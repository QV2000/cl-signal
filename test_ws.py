"""Quick test of WebSocket connection."""
import asyncio
import json
import logging

import websockets

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
CL_COIN = "xyz:CL"

async def test():
    logger.info(f"Connecting to {HL_WS_URL}")

    async with websockets.connect(HL_WS_URL, ping_interval=20, ping_timeout=10) as ws:
        logger.info("Connected!")

        # Subscribe to trades
        sub = {"method": "subscribe", "subscription": {"type": "trades", "coin": CL_COIN}}
        await ws.send(json.dumps(sub))
        logger.info(f"Sent: {sub}")

        # Subscribe to activeAssetCtx
        sub2 = {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": CL_COIN}}
        await ws.send(json.dumps(sub2))
        logger.info(f"Sent: {sub2}")

        # Listen for messages
        msg_count = 0
        start = asyncio.get_event_loop().time()

        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                msg_count += 1

                channel = data.get("channel", "unknown")

                if channel == "subscriptionResponse":
                    logger.info(f"Subscription response: {data}")
                elif channel == "trades":
                    trades = data.get("data", [])
                    logger.info(f"Trades: {len(trades)} fills")
                    for t in trades[:3]:
                        logger.info(f"  {t.get('users', ['?','?'])[0][:8]}... {t.get('side')} {t.get('sz')} @ {t.get('px')}")
                elif channel == "activeAssetCtx":
                    ctx = data.get("data", {}).get("ctx", {})
                    logger.info(f"Market: mark={ctx.get('markPx')} funding={ctx.get('funding')} OI={ctx.get('openInterest')}")
                else:
                    logger.debug(f"Other: {channel} - {str(data)[:100]}")

                elapsed = asyncio.get_event_loop().time() - start
                if elapsed > 30:
                    logger.info("30s elapsed, stopping")
                    break

            except asyncio.TimeoutError:
                logger.info("No message in 5s (market may be quiet)")
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed > 30:
                    break
            except Exception as e:
                logger.error(f"Error: {e}")
                break

        logger.info(f"Received {msg_count} messages total")

if __name__ == "__main__":
    asyncio.run(test())
