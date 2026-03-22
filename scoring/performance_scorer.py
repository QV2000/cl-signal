"""
Performance Scorer Module

Computes wallet performance scores: PnL, consistency, timing, conviction.
Applies Bayesian shrinkage for low-sample wallets.
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import math

import numpy as np
import duckdb

from scoring.trade_reconstruction import RoundTrip
from scoring.behavioral_classifier import WalletBehavior

logger = logging.getLogger(__name__)

MIN_TRADES_FOR_SCORING = 15
BAYESIAN_PRIOR_STRENGTH = 30  # k parameter for shrinkage
RECENCY_HALFLIFE_DAYS = 30


@dataclass
class WalletScore:
    """Complete scoring output for a wallet."""
    wallet: str
    is_scoreable: bool
    trade_count: int
    total_pnl: float
    win_rate: float
    profit_factor: float

    # Component scores (-1 to +1)
    pnl_score: float
    consistency_score: float
    timing_score: float
    conviction_score: float

    # Composite
    raw_composite: float
    shrunk_composite: float  # After Bayesian shrinkage
    recency_weight: float
    final_score: float  # shrunk_composite * recency_weight

    # Behavioral
    archetype: str
    archetype_weight: float

    last_trade_time: Optional[datetime] = None


def compute_pnl_metrics(round_trips: List[RoundTrip]) -> Dict:
    """Compute basic PnL metrics from round trips."""
    if not round_trips:
        return {
            'total_pnl': 0,
            'win_rate': 0.5,
            'profit_factor': 1.0,
            'wins': 0,
            'losses': 0,
            'gross_profit': 0,
            'gross_loss': 0,
        }

    wins = [rt for rt in round_trips if rt.realized_pnl > 0]
    losses = [rt for rt in round_trips if rt.realized_pnl < 0]

    total_pnl = sum(rt.realized_pnl for rt in round_trips)
    win_rate = len(wins) / len(round_trips) if round_trips else 0.5

    gross_profit = sum(rt.realized_pnl for rt in wins)
    gross_loss = abs(sum(rt.realized_pnl for rt in losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 2.0

    return {
        'total_pnl': total_pnl,
        'win_rate': win_rate,
        'profit_factor': min(profit_factor, 10.0),  # Cap extreme values
        'wins': len(wins),
        'losses': len(losses),
        'gross_profit': gross_profit,
        'gross_loss': gross_loss,
    }


def compute_pnl_score(wallet_pnl: float, all_pnls: List[float]) -> float:
    """
    Compute PnL percentile rank, rescaled to [-1, +1].
    90th percentile = +0.8, 10th percentile = -0.8, median = 0
    """
    if not all_pnls or len(all_pnls) < 2:
        return 0.0

    rank = sum(1 for p in all_pnls if p < wallet_pnl) / len(all_pnls)
    return (rank - 0.5) * 2


def compute_consistency_score(win_rate: float, profit_factor: float) -> float:
    """
    Compute consistency score from win rate and profit factor.
    Returns value in [-1, +1].
    """
    # Win rate adjustment: center on 50%
    win_rate_adj = (win_rate - 0.5) * 2  # 50% -> 0, 75% -> 0.5, 25% -> -0.5

    # Profit factor adjustment: PF of 1 = 0, PF of 3 = 1, PF of 0.33 = -1
    pf_adj = min(max((profit_factor - 1.0) / 2.0, -1), 1)

    return 0.5 * win_rate_adj + 0.5 * pf_adj


def compute_timing_score(round_trips: List[RoundTrip]) -> float:
    """
    Compute timing score based on post-entry alpha.
    Positive = enters before favorable moves, negative = enters at bad times.
    """
    alphas_4h = [rt.alpha_4h for rt in round_trips if rt.alpha_4h is not None]

    if not alphas_4h:
        return 0.0

    # Average 4h alpha, normalized
    avg_alpha = np.mean(alphas_4h)

    # Typical market noise is ~0.5-1% in 4h, so scale accordingly
    # +2% avg alpha = excellent timing (+1), -2% = terrible (-1)
    return max(min(avg_alpha * 50, 1.0), -1.0)


def compute_conviction_score(round_trips: List[RoundTrip]) -> float:
    """
    Compute conviction score: correlation between position size and trade return.
    Positive = sizes up on better trades.
    """
    if len(round_trips) < 5:
        return 0.0

    sizes = np.array([rt.size for rt in round_trips])
    returns = np.array([rt.realized_pnl / (rt.size * rt.entry_price) if rt.size * rt.entry_price > 0 else 0
                        for rt in round_trips])

    # Handle constant arrays
    if np.std(sizes) == 0 or np.std(returns) == 0:
        return 0.0

    correlation = np.corrcoef(sizes, returns)[0, 1]

    if np.isnan(correlation):
        return 0.0

    return max(min(correlation, 1.0), -1.0)


def apply_bayesian_shrinkage(raw_score: float, trade_count: int,
                              population_mean: float = 0.0,
                              prior_strength: int = BAYESIAN_PRIOR_STRENGTH) -> float:
    """
    Apply Bayesian shrinkage toward population mean.
    Low trade count = pulled more toward mean.
    """
    return (trade_count * raw_score + prior_strength * population_mean) / (trade_count + prior_strength)


def compute_recency_weight(last_trade_time: Optional[datetime],
                            halflife_days: int = RECENCY_HALFLIFE_DAYS) -> float:
    """
    Compute recency weight with exponential decay.
    Recent traders get weight close to 1, old traders decay toward 0.
    """
    if not last_trade_time:
        return 0.0

    days_since = (datetime.utcnow() - last_trade_time).days
    decay_rate = 0.693 / halflife_days  # ln(2) / halflife
    return math.exp(-decay_rate * days_since)


def score_wallet(wallet: str,
                 round_trips: List[RoundTrip],
                 behavior: Optional[WalletBehavior],
                 all_pnls: List[float]) -> WalletScore:
    """Compute complete score for a single wallet."""
    trade_count = len(round_trips)
    is_scoreable = trade_count >= MIN_TRADES_FOR_SCORING

    # Basic metrics
    metrics = compute_pnl_metrics(round_trips)

    # Component scores
    if is_scoreable:
        pnl_score = compute_pnl_score(metrics['total_pnl'], all_pnls)
        consistency_score = compute_consistency_score(metrics['win_rate'], metrics['profit_factor'])
        timing_score = compute_timing_score(round_trips)
        conviction_score = compute_conviction_score(round_trips)
    else:
        pnl_score = consistency_score = timing_score = conviction_score = 0.0

    # Composite score (weighted average)
    raw_composite = (
        0.35 * pnl_score +
        0.25 * consistency_score +
        0.25 * timing_score +
        0.15 * conviction_score
    )

    # Bayesian shrinkage
    shrunk_composite = apply_bayesian_shrinkage(raw_composite, trade_count) if is_scoreable else 0.0

    # Recency weight
    last_trade = max((rt.exit_time for rt in round_trips), default=None)
    recency_weight = compute_recency_weight(last_trade)

    # Final score
    final_score = shrunk_composite * recency_weight if is_scoreable else 0.0

    # Behavioral info
    archetype = behavior.archetype.value if behavior else "unknown"
    archetype_weight = behavior.signal_weight if behavior else 0.5

    return WalletScore(
        wallet=wallet,
        is_scoreable=is_scoreable,
        trade_count=trade_count,
        total_pnl=metrics['total_pnl'],
        win_rate=metrics['win_rate'],
        profit_factor=metrics['profit_factor'],
        pnl_score=pnl_score,
        consistency_score=consistency_score,
        timing_score=timing_score,
        conviction_score=conviction_score,
        raw_composite=raw_composite,
        shrunk_composite=shrunk_composite,
        recency_weight=recency_weight,
        final_score=final_score,
        archetype=archetype,
        archetype_weight=archetype_weight,
        last_trade_time=last_trade
    )


def score_all_wallets(all_round_trips: Dict[str, List[RoundTrip]],
                      behaviors: Dict[str, WalletBehavior]) -> List[WalletScore]:
    """Score all wallets and return sorted list."""
    # Collect all PnLs for percentile ranking
    all_pnls = []
    for wallet, trips in all_round_trips.items():
        if len(trips) >= MIN_TRADES_FOR_SCORING:
            total_pnl = sum(rt.realized_pnl for rt in trips)
            all_pnls.append(total_pnl)

    scores = []
    for wallet, trips in all_round_trips.items():
        behavior = behaviors.get(wallet)
        score = score_wallet(wallet, trips, behavior, all_pnls)
        scores.append(score)

    # Sort by absolute final score (most informative first)
    scores.sort(key=lambda s: abs(s.final_score), reverse=True)

    # Log summary
    scoreable = [s for s in scores if s.is_scoreable]
    positive = [s for s in scoreable if s.final_score > 0.3]
    negative = [s for s in scoreable if s.final_score < -0.3]

    logger.info(f"Scored {len(scores)} wallets: {len(scoreable)} scoreable, "
                f"{len(positive)} strong positive, {len(negative)} strong negative")

    return scores


def persist_scores(conn: duckdb.DuckDBPyConnection, scores: List[WalletScore]):
    """Save wallet scores to database."""
    today = datetime.utcnow().date()

    for score in scores:
        conn.execute("""
            INSERT OR REPLACE INTO wallet_scores
            (wallet, score_date, trade_count_window, is_scoreable,
             pnl_score, consistency_score, timing_score, conviction_score,
             overall_score, win_rate, profit_factor, realized_pnl_window,
             avg_hold_hours, recency_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            score.wallet,
            today,
            score.trade_count,
            score.is_scoreable,
            score.pnl_score,
            score.consistency_score,
            score.timing_score,
            score.conviction_score,
            score.final_score,
            score.win_rate,
            score.profit_factor,
            score.total_pnl,
            None,  # avg_hold_hours - would need to compute
            score.recency_weight,
        ])

    logger.info(f"Persisted {len(scores)} wallet scores to database")