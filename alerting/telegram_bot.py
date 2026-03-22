"""
Telegram bot for CL Signal alerts and commands.

Commands:
- /status - Current signal snapshot
- /whales - Top wallets by weighted contribution
- /health - System health and uptime
- /scores - Top and bottom wallets by score
- /history N - Last N signal readings
- /flow - Real-time flow signal readings
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Callable, Any

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, CL_COIN
from db.schema import get_connection, get_table_stats
from utils.nymex_hours import is_nymex_open, is_weekend

logger = logging.getLogger(__name__)


class TelegramBot:
    """
    Telegram bot for CL Signal system.

    Provides commands and sends alerts on signal events.
    """

    def __init__(
        self,
        get_ws_stats: Optional[Callable[[], dict]] = None,
        get_poller_stats: Optional[Callable[[], dict]] = None,
        get_positions: Optional[Callable[[int], list]] = None,
        get_flow_signals: Optional[Callable[[], dict]] = None,
    ):
        """
        Args:
            get_ws_stats: Function to get WebSocket client stats.
            get_poller_stats: Function to get poller stats.
            get_positions: Function to get top N positions.
            get_flow_signals: Function to get flow signal readings.
        """
        self.get_ws_stats = get_ws_stats
        self.get_poller_stats = get_poller_stats
        self.get_positions = get_positions
        self.get_flow_signals = get_flow_signals

        self.app: Optional[Application] = None
        self.start_time = datetime.now(timezone.utc)

    async def start(self) -> None:
        """Initialize and start the Telegram bot."""
        if not TELEGRAM_BOT_TOKEN:
            logger.warning("TELEGRAM_BOT_TOKEN not set. Bot disabled.")
            return

        self.app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Register command handlers
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("health", self.cmd_health))
        self.app.add_handler(CommandHandler("whales", self.cmd_whales))
        self.app.add_handler(CommandHandler("scores", self.cmd_scores))
        self.app.add_handler(CommandHandler("history", self.cmd_history))
        self.app.add_handler(CommandHandler("flow", self.cmd_flow))
        self.app.add_handler(CommandHandler("help", self.cmd_help))

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()

        logger.info("Telegram bot started")

    async def stop(self) -> None:
        """Stop the Telegram bot."""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    async def send_message(self, text: str, chat_id: str = None) -> None:
        """Send a message to the configured chat."""
        if not self.app or not TELEGRAM_BOT_TOKEN:
            logger.warning("Bot not initialized. Cannot send message.")
            return

        target_chat = chat_id or TELEGRAM_CHAT_ID
        if not target_chat:
            logger.warning("No chat ID configured. Cannot send message.")
            return

        try:
            await self.app.bot.send_message(chat_id=target_chat, text=text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /status command. Shows current signal snapshot."""
        now = datetime.now(timezone.utc)
        nymex_open = is_nymex_open(now)
        weekend = is_weekend(now)

        # Get latest stats
        ws_stats = self.get_ws_stats() if self.get_ws_stats else {}
        poller_stats = self.get_poller_stats() if self.get_poller_stats else {}
        market = poller_stats.get("market_context", {})

        # Get latest signal from DB
        conn = get_connection()
        latest_signal = conn.execute(
            "SELECT * FROM signals ORDER BY signal_ts DESC LIMIT 1"
        ).fetchone()
        conn.close()

        status_lines = [
            f"<b>CL Signal Status</b>",
            f"",
            f"Mark: ${market.get('mark_px', 'N/A')}",
            f"Funding: {market.get('funding', 'N/A')}",
            f"OI: ${market.get('open_interest', 'N/A'):,.0f}" if market.get('open_interest') else "OI: N/A",
            f"",
            f"CME Status: {'OPEN' if nymex_open else 'CLOSED'}" + (" (weekend)" if weekend else ""),
            f"Tracked wallets: {ws_stats.get('tracked_wallets', 0)}",
            f"Fills today: {ws_stats.get('fills_received', 0)}",
        ]

        if latest_signal:
            status_lines.extend([
                f"",
                f"<b>Latest Signal</b>",
                f"Direction: {latest_signal[17]}",  # direction column
                f"Composite: {latest_signal[4]:.3f}" if latest_signal[4] else "Composite: N/A",
                f"Confidence: {latest_signal[18]:.2f}" if latest_signal[18] else "Confidence: N/A",
            ])

        await update.message.reply_text("\n".join(status_lines), parse_mode="HTML")

    async def cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /health command. Shows system health."""
        now = datetime.now(timezone.utc)
        uptime = now - self.start_time

        ws_stats = self.get_ws_stats() if self.get_ws_stats else {}
        poller_stats = self.get_poller_stats() if self.get_poller_stats else {}
        table_stats = get_table_stats()

        # Calculate uptime string
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        uptime_str = f"{days}d {hours}h {minutes}m"

        health_lines = [
            f"<b>System Health</b>",
            f"",
            f"Uptime: {uptime_str}",
            f"Last fill: {ws_stats.get('last_fill_ts', 'Never')}",
            f"Polls completed: {poller_stats.get('polls_completed', 0)}",
            f"",
            f"<b>Database</b>",
            f"Fills: {table_stats.get('fills', 0):,}",
            f"Snapshots: {table_stats.get('position_snapshots', 0):,}",
            f"Wallets: {table_stats.get('wallet_registry', 0):,}",
            f"Signals: {table_stats.get('signals', 0):,}",
            f"",
            f"<b>Rate Limiter</b>",
        ]

        rl_stats = poller_stats.get("rate_limiter", {})
        health_lines.append(f"Utilization: {rl_stats.get('utilization_pct', 0)}%")
        health_lines.append(f"Headroom: {rl_stats.get('headroom', 0)} weight")

        await update.message.reply_text("\n".join(health_lines), parse_mode="HTML")

    async def cmd_whales(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /whales command. Shows top positions."""
        if not self.get_positions:
            await update.message.reply_text("Position tracking not available.")
            return

        positions = self.get_positions(10)

        if not positions:
            await update.message.reply_text("No positions tracked yet.")
            return

        lines = [f"<b>Top 10 Positions ({CL_COIN})</b>", ""]

        for wallet, size in positions:
            direction = "LONG" if size > 0 else "SHORT"
            wallet_short = f"{wallet[:6]}...{wallet[-4:]}"
            lines.append(f"{wallet_short}: {direction} {abs(size):.2f}")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_scores(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /scores command. Shows top and bottom wallet scores."""
        conn = get_connection()

        # Get latest scores
        top_scores = conn.execute("""
            SELECT wallet, overall_score, trade_count_window
            FROM wallet_scores
            WHERE score_date = (SELECT MAX(score_date) FROM wallet_scores)
              AND is_scoreable = TRUE
            ORDER BY overall_score DESC
            LIMIT 5
        """).fetchall()

        bottom_scores = conn.execute("""
            SELECT wallet, overall_score, trade_count_window
            FROM wallet_scores
            WHERE score_date = (SELECT MAX(score_date) FROM wallet_scores)
              AND is_scoreable = TRUE
            ORDER BY overall_score ASC
            LIMIT 5
        """).fetchall()

        conn.close()

        lines = [f"<b>Wallet Scores</b>", ""]

        if top_scores:
            lines.append("<b>Top 5 (follow)</b>")
            for wallet, score, trades in top_scores:
                wallet_short = f"{wallet[:6]}...{wallet[-4:]}"
                lines.append(f"{wallet_short}: {score:+.3f} ({trades} trades)")
        else:
            lines.append("No scores computed yet.")

        if bottom_scores:
            lines.append("")
            lines.append("<b>Bottom 5 (fade)</b>")
            for wallet, score, trades in bottom_scores:
                wallet_short = f"{wallet[:6]}...{wallet[-4:]}"
                lines.append(f"{wallet_short}: {score:+.3f} ({trades} trades)")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /history N command. Shows last N signals."""
        # Parse count argument
        try:
            n = int(context.args[0]) if context.args else 10
            n = min(max(n, 1), 50)  # Clamp to 1-50
        except ValueError:
            n = 10

        conn = get_connection()
        signals = conn.execute(f"""
            SELECT signal_ts, composite_signal, direction, confidence, mark_px
            FROM signals
            ORDER BY signal_ts DESC
            LIMIT {n}
        """).fetchall()
        conn.close()

        if not signals:
            await update.message.reply_text("No signals recorded yet.")
            return

        lines = [f"<b>Last {len(signals)} Signals</b>", ""]

        for ts, composite, direction, confidence, mark_px in signals:
            ts_str = ts.strftime("%m-%d %H:%M") if ts else "N/A"
            comp_str = f"{composite:+.3f}" if composite else "N/A"
            dir_str = direction or "FLAT"
            lines.append(f"{ts_str}: {dir_str} {comp_str} (${mark_px:.2f})" if mark_px else f"{ts_str}: {dir_str} {comp_str}")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /flow command. Shows real-time flow signal readings."""
        if not self.get_flow_signals:
            await update.message.reply_text("Flow signals not available.")
            return

        flow = self.get_flow_signals()

        # Get last alert info
        last_alert_secs = flow.get("last_alert_seconds_ago", float('inf'))
        if last_alert_secs < 60:
            last_alert_str = f"{int(last_alert_secs)}s ago"
        elif last_alert_secs < 3600:
            last_alert_str = f"{int(last_alert_secs / 60)}m ago"
        elif last_alert_secs < float('inf'):
            last_alert_str = f"{int(last_alert_secs / 3600)}h ago"
        else:
            last_alert_str = "Never"

        last_alert_dir = flow.get("last_alert_direction", "N/A")

        # Get flow alert hit rates from DB
        conn = get_connection()
        hit_rates = conn.execute("""
            SELECT
                COUNT(*) FILTER (WHERE correct_1m = TRUE) as wins_1m,
                COUNT(*) FILTER (WHERE correct_1m IS NOT NULL) as total_1m,
                COUNT(*) FILTER (WHERE correct_5m = TRUE) as wins_5m,
                COUNT(*) FILTER (WHERE correct_5m IS NOT NULL) as total_5m
            FROM flow_alerts
        """).fetchone()
        conn.close()

        wins_1m, total_1m, wins_5m, total_5m = hit_rates if hit_rates else (0, 0, 0, 0)
        rate_1m = (wins_1m / total_1m * 100) if total_1m > 0 else 0
        rate_5m = (wins_5m / total_5m * 100) if total_5m > 0 else 0

        lines = [
            f"<b>CL Flow Signal (live)</b>",
            f"",
            f"30s: {flow['30s']['signal']:+.2f} ({flow['30s']['fills']} fills)",
            f" 2m: {flow['2m']['signal']:+.2f} ({flow['2m']['fills']} fills)",
            f" 5m: {flow['5m']['signal']:+.2f} ({flow['5m']['fills']} fills)",
            f"",
            f"Alert threshold: all windows > +/-0.4",
            f"Last alert: {last_alert_str} ({last_alert_dir})",
            f"",
            f"<b>Flow alert hit rate:</b>",
            f"  1-min: {rate_1m:.0f}% ({wins_1m}/{total_1m})",
            f"  5-min: {rate_5m:.0f}% ({wins_5m}/{total_5m})",
        ]

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command."""
        help_text = """<b>CL Signal Bot Commands</b>

/status - Current signal snapshot
/health - System uptime and stats
/whales - Top 10 positions
/scores - Best and worst wallet scores
/history N - Last N signals (default 10)
/flow - Real-time flow signals
/help - This message"""

        await update.message.reply_text(help_text, parse_mode="HTML")

    async def alert_signal_change(
        self,
        direction: str,
        composite: float,
        delta_4h: float,
        confidence: float,
        mark_px: float,
        funding: float,
        open_interest: float,
    ) -> None:
        """Send alert when signal crosses threshold."""
        nymex_open = is_nymex_open(datetime.now(timezone.utc))
        weekend = is_weekend(datetime.now(timezone.utc))

        emoji = "+" if direction == "LONG" else "-" if direction == "SHORT" else "~"

        message = f"""<b>CL SIGNAL UPDATE</b>

Direction: {direction} {emoji}
Composite: {composite:+.3f} (was {composite - delta_4h:+.3f} 4h ago)
Confidence: {confidence:.2f}

Mark: ${mark_px:.2f} | Funding: {funding:+.4%}
OI: ${open_interest:,.0f}

CME Status: {'OPEN' if nymex_open else 'CLOSED'}{' (weekend)' if weekend else ''}"""

        await self.send_message(message)

    async def alert_health_issue(self, issue: str) -> None:
        """Send alert for system health issues."""
        message = f"""<b>HEALTH ALERT</b>

{issue}

Check /health for details."""

        await self.send_message(message)


async def main():
    """Test the bot standalone."""
    logging.basicConfig(level=logging.INFO)

    bot = TelegramBot()

    try:
        await bot.start()
        logger.info("Bot running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
