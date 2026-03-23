"""
CL Signal API - HTTP endpoints for querying data remotely
"""
import json
from datetime import datetime, timezone, timedelta
from aiohttp import web
import duckdb
from config import DB_PATH

routes = web.RouteTableDef()

def get_db():
    return duckdb.connect(DB_PATH, read_only=True)

@routes.get('/health')
async def health(request):
    return web.json_response({"status": "ok", "ts": datetime.now(timezone.utc).isoformat()})

@routes.get('/stats')
async def stats(request):
    """Get current system stats"""
    con = get_db()
    try:
        fills = con.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        wallets = con.execute('SELECT COUNT(*) FROM wallet_registry').fetchone()[0]
        scores = con.execute('SELECT COUNT(*) FROM wallet_scores WHERE is_scoreable = true').fetchone()[0]
        positions = con.execute('SELECT COUNT(*) FROM position_snapshots').fetchone()[0]

        time_range = con.execute('SELECT MIN(ts), MAX(ts) FROM fills').fetchone()
        first_ts, last_ts = time_range

        uptime_hours = (last_ts - first_ts).total_seconds() / 3600 if first_ts and last_ts else 0

        vol = con.execute('SELECT SUM(CAST(notional_usd AS DOUBLE)) FROM fills').fetchone()[0] or 0

        recent_1h = con.execute('''
            SELECT COUNT(*) FROM fills
            WHERE ts > (SELECT MAX(ts) FROM fills) - INTERVAL 1 HOUR
        ''').fetchone()[0]

        return web.json_response({
            "fills": fills,
            "wallets": wallets,
            "scored_wallets": scores,
            "position_snapshots": positions,
            "uptime_hours": round(uptime_hours, 2),
            "total_volume_usd": round(vol, 2),
            "fills_last_hour": recent_1h,
            "first_fill": first_ts.isoformat() if first_ts else None,
            "last_fill": last_ts.isoformat() if last_ts else None,
        })
    finally:
        con.close()

@routes.get('/signals')
async def signals(request):
    """Get recent signals"""
    limit = int(request.query.get('limit', 10))
    con = get_db()
    try:
        df = con.execute(f'''
            SELECT signal_ts, coin, composite_signal, direction, confidence,
                   scored_wallets_count, hhi_longs, hhi_shorts
            FROM signals
            ORDER BY signal_ts DESC
            LIMIT {limit}
        ''').fetchdf()

        records = df.to_dict(orient='records')
        for r in records:
            if r.get('signal_ts'):
                r['signal_ts'] = r['signal_ts'].isoformat()

        return web.json_response({"signals": records})
    finally:
        con.close()

@routes.get('/fills')
async def fills(request):
    """Get recent fills"""
    limit = int(request.query.get('limit', 50))
    min_notional = float(request.query.get('min_notional', 0))

    con = get_db()
    try:
        df = con.execute(f'''
            SELECT ts, buyer, seller, taker_side, px, sz, notional_usd
            FROM fills
            WHERE notional_usd >= {min_notional}
            ORDER BY ts DESC
            LIMIT {limit}
        ''').fetchdf()

        records = df.to_dict(orient='records')
        for r in records:
            if r.get('ts'):
                r['ts'] = r['ts'].isoformat()

        return web.json_response({"fills": records})
    finally:
        con.close()

@routes.get('/wallets/top')
async def top_wallets(request):
    """Get top scored wallets"""
    limit = int(request.query.get('limit', 20))
    con = get_db()
    try:
        df = con.execute(f'''
            SELECT wallet, overall_score, pnl_score, win_rate, profit_factor,
                   trade_count_window, realized_pnl_window
            FROM wallet_scores
            WHERE is_scoreable = true
            ORDER BY overall_score DESC
            LIMIT {limit}
        ''').fetchdf()

        return web.json_response({"wallets": df.to_dict(orient='records')})
    finally:
        con.close()

@routes.get('/wallets/{wallet}')
async def wallet_detail(request):
    """Get details for a specific wallet"""
    wallet = request.match_info['wallet'].lower()
    con = get_db()
    try:
        # Get score
        score = con.execute('''
            SELECT * FROM wallet_scores WHERE wallet = ? ORDER BY score_date DESC LIMIT 1
        ''', [wallet]).fetchdf().to_dict(orient='records')

        # Get recent fills
        fills = con.execute('''
            SELECT ts, taker_side, px, sz, notional_usd
            FROM fills
            WHERE buyer = ? OR seller = ?
            ORDER BY ts DESC LIMIT 50
        ''', [wallet, wallet]).fetchdf()

        fills_records = fills.to_dict(orient='records')
        for r in fills_records:
            if r.get('ts'):
                r['ts'] = r['ts'].isoformat()

        # Get position history
        positions = con.execute('''
            SELECT snapshot_ts, szi, entry_px, unrealized_pnl
            FROM position_snapshots
            WHERE wallet = ?
            ORDER BY snapshot_ts DESC LIMIT 100
        ''', [wallet]).fetchdf()

        pos_records = positions.to_dict(orient='records')
        for r in pos_records:
            if r.get('snapshot_ts'):
                r['snapshot_ts'] = r['snapshot_ts'].isoformat()

        return web.json_response({
            "wallet": wallet,
            "score": score[0] if score else None,
            "recent_fills": fills_records,
            "position_history": pos_records
        })
    finally:
        con.close()

@routes.get('/positions')
async def positions(request):
    """Get current positions (latest snapshot per wallet)"""
    min_size = float(request.query.get('min_size', 1.0))
    con = get_db()
    try:
        df = con.execute(f'''
            WITH latest AS (
                SELECT wallet, szi, entry_px, unrealized_pnl, snapshot_ts,
                       ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY snapshot_ts DESC) as rn
                FROM position_snapshots
            )
            SELECT wallet, szi, entry_px, unrealized_pnl, snapshot_ts
            FROM latest
            WHERE rn = 1 AND ABS(szi) >= {min_size}
            ORDER BY ABS(szi) DESC
            LIMIT 100
        ''').fetchdf()

        records = df.to_dict(orient='records')
        for r in records:
            if r.get('snapshot_ts'):
                r['snapshot_ts'] = r['snapshot_ts'].isoformat()

        return web.json_response({"positions": records})
    finally:
        con.close()

@routes.get('/flow')
async def flow_alerts(request):
    """Get recent flow alerts"""
    limit = int(request.query.get('limit', 20))
    con = get_db()
    try:
        df = con.execute(f'''
            SELECT alert_ts, direction, signal_30s, signal_2m, signal_5m,
                   mark_px_at_alert, mark_px_1m_later, mark_px_5m_later,
                   correct_1m, correct_5m
            FROM flow_alerts
            ORDER BY alert_ts DESC
            LIMIT {limit}
        ''').fetchdf()

        records = df.to_dict(orient='records')
        for r in records:
            if r.get('alert_ts'):
                r['alert_ts'] = r['alert_ts'].isoformat()

        return web.json_response({"flow_alerts": records})
    except Exception as e:
        return web.json_response({"flow_alerts": [], "error": str(e)})
    finally:
        con.close()

@routes.get('/summary')
async def summary(request):
    """Get a comprehensive summary for quick status check"""
    con = get_db()
    try:
        # Basic stats
        fills = con.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        wallets = con.execute('SELECT COUNT(*) FROM wallet_registry').fetchone()[0]

        time_range = con.execute('SELECT MIN(ts), MAX(ts) FROM fills').fetchone()
        first_ts, last_ts = time_range
        uptime_hours = (last_ts - first_ts).total_seconds() / 3600 if first_ts and last_ts else 0

        vol = con.execute('SELECT SUM(CAST(notional_usd AS DOUBLE)) FROM fills').fetchone()[0] or 0

        # Latest signal
        sig = con.execute('''
            SELECT direction, confidence, composite_signal, signal_ts
            FROM signals ORDER BY signal_ts DESC LIMIT 1
        ''').fetchone()

        # Top 5 wallets
        top_wallets = con.execute('''
            SELECT wallet, overall_score, win_rate, realized_pnl_window
            FROM wallet_scores WHERE is_scoreable = true
            ORDER BY overall_score DESC LIMIT 5
        ''').fetchdf().to_dict(orient='records')

        # Large trades last hour
        large_trades = con.execute('''
            SELECT COUNT(*) FROM fills
            WHERE notional_usd > 50000
            AND ts > (SELECT MAX(ts) FROM fills) - INTERVAL 1 HOUR
        ''').fetchone()[0]

        return web.json_response({
            "status": "running",
            "uptime_hours": round(uptime_hours, 2),
            "fills": fills,
            "wallets": wallets,
            "volume_usd": round(vol, 2),
            "last_fill": last_ts.isoformat() if last_ts else None,
            "latest_signal": {
                "direction": sig[0] if sig else None,
                "confidence": float(sig[1]) if sig else None,
                "composite": float(sig[2]) if sig else None,
                "ts": sig[3].isoformat() if sig and sig[3] else None
            } if sig else None,
            "top_wallets": top_wallets,
            "large_trades_1h": large_trades
        })
    finally:
        con.close()


def create_api_app():
    app = web.Application()
    app.add_routes(routes)
    return app


async def start_api_server(host='0.0.0.0', port=8080):
    """Start the API server"""
    app = create_api_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner
