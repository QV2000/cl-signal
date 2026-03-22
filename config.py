"""
Configuration for CL Signal System.
"""
import os

# Hyperliquid
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_API_URL = "https://api.hyperliquid.xyz/info"
CL_COIN = "xyz:CL"
HL_DEX = "xyz"

# MoonDev
MOONDEV_BASE = "https://api.moondev.com/api"

# DuckDB
DB_PATH = os.environ.get("CL_SIGNAL_DB_PATH", os.path.expanduser("~/cl_signal/cl_signal.ddb"))

# Polling intervals
POSITION_POLL_INTERVAL_SECONDS = 60
SIGNAL_COMPUTE_INTERVAL_SECONDS = 900  # 15 minutes
SCORE_RECOMPUTE_INTERVAL_HOURS = 24

# Scoring parameters
MIN_TRADES_FOR_SCORING = 15
SCORING_WINDOW_DAYS = 60
RECENCY_HALF_LIFE_DAYS = 30
BAYESIAN_PRIOR_STRENGTH = 30

# Signal thresholds
SIGNAL_LONG_THRESHOLD = 0.5
SIGNAL_SHORT_THRESHOLD = -0.5
SIGNAL_FLAT_BAND = 0.3
RAPID_SHIFT_THRESHOLD = 0.4
WHALE_ALERT_NOTIONAL_USD = 50000

# Alerting
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Top wallet tracking
MAX_TRACKED_WALLETS = 200

# Rate limits
HL_WEIGHT_LIMIT_PER_MINUTE = 1200
HL_WS_MAX_CONNECTIONS = 100
HL_WS_MAX_SUBSCRIPTIONS = 1000
HL_WS_MAX_MESSAGES_PER_MINUTE = 2000
