"""
Behavioral Classifier Module

Classifies wallets into trading archetypes using k-means clustering.
Archetypes: directional_trader, market_maker, funding_farmer, momentum_chaser, liquidation_hunter
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import duckdb

from config import CL_COIN
from scoring.trade_reconstruction import RoundTrip

logger = logging.getLogger(__name__)


class TraderArchetype(Enum):
    DIRECTIONAL_TRADER = "directional_trader"  # Low frequency, long holds, high conviction
    MARKET_MAKER = "market_maker"  # High frequency, tight position management
    FUNDING_FARMER = "funding_farmer"  # Holds for funding, flips at rate changes
    MOMENTUM_CHASER = "momentum_chaser"  # Enters after large moves, short holds
    LIQUIDATION_HUNTER = "liquidation_hunter"  # Extreme leverage, binary outcomes
    UNKNOWN = "unknown"


# Archetypes with useful signal (positive weight in composite)
SIGNAL_ARCHETYPES = {
    TraderArchetype.DIRECTIONAL_TRADER,
    TraderArchetype.MOMENTUM_CHASER,
}

# Archetypes to fade (inverse signal)
FADE_ARCHETYPES = {
    TraderArchetype.LIQUIDATION_HUNTER,
}

# Archetypes to exclude from signal
EXCLUDE_ARCHETYPES = {
    TraderArchetype.MARKET_MAKER,
    TraderArchetype.FUNDING_FARMER,
}


@dataclass
class WalletBehavior:
    """Behavioral features for a wallet."""
    wallet: str
    trade_frequency: float  # trades per day
    avg_hold_duration: float  # hours
    position_size_cv: float  # coefficient of variation
    directional_consistency: float  # fraction of trades in dominant direction
    avg_fills_per_trade: float  # many small = MM, few large = directional
    nymex_hour_ratio: float  # fraction during NYMEX hours
    archetype: TraderArchetype = TraderArchetype.UNKNOWN
    signal_weight: float = 1.0  # weight multiplier for this wallet's signal


def compute_behavioral_features(wallet: str,
                                 round_trips: List[RoundTrip],
                                 conn: duckdb.DuckDBPyConnection) -> Optional[WalletBehavior]:
    """Compute behavioral features for a wallet from its round trips."""
    if len(round_trips) < 3:
        return None

    # Trade frequency (trades per day)
    if round_trips:
        first_trade = min(rt.entry_time for rt in round_trips)
        last_trade = max(rt.exit_time for rt in round_trips)
        days_active = max((last_trade - first_trade).days, 1)
        trade_frequency = len(round_trips) / days_active
    else:
        trade_frequency = 0

    # Average hold duration
    hold_durations = [rt.hold_duration_hours for rt in round_trips]
    avg_hold_duration = np.mean(hold_durations) if hold_durations else 0

    # Position size coefficient of variation
    sizes = [rt.size for rt in round_trips]
    if sizes and np.mean(sizes) > 0:
        position_size_cv = np.std(sizes) / np.mean(sizes)
    else:
        position_size_cv = 0

    # Directional consistency (what fraction are in the dominant direction)
    long_count = sum(1 for rt in round_trips if rt.direction == 'long')
    short_count = len(round_trips) - long_count
    dominant_count = max(long_count, short_count)
    directional_consistency = dominant_count / len(round_trips) if round_trips else 0.5

    # Average fills per trade (entry fills)
    avg_fills = np.mean([rt.entry_fills for rt in round_trips])

    # NYMEX hours ratio (6:00-17:00 ET = 11:00-22:00 UTC on weekdays)
    nymex_count = 0
    for rt in round_trips:
        hour = rt.entry_time.hour
        weekday = rt.entry_time.weekday()
        if weekday < 5 and 11 <= hour < 22:
            nymex_count += 1
    nymex_hour_ratio = nymex_count / len(round_trips) if round_trips else 0.5

    return WalletBehavior(
        wallet=wallet,
        trade_frequency=trade_frequency,
        avg_hold_duration=avg_hold_duration,
        position_size_cv=position_size_cv,
        directional_consistency=directional_consistency,
        avg_fills_per_trade=avg_fills,
        nymex_hour_ratio=nymex_hour_ratio
    )


def classify_wallets(behaviors: List[WalletBehavior], n_clusters: int = 5) -> List[WalletBehavior]:
    """
    Classify wallets into archetypes using k-means clustering.
    Returns behaviors with archetype and signal_weight assigned.
    """
    if len(behaviors) < n_clusters:
        logger.warning(f"Only {len(behaviors)} wallets, need at least {n_clusters} for clustering")
        for b in behaviors:
            b.archetype = TraderArchetype.UNKNOWN
            b.signal_weight = 0.5
        return behaviors

    # Build feature matrix
    features = np.array([
        [
            b.trade_frequency,
            b.avg_hold_duration,
            b.position_size_cv,
            b.directional_consistency,
            b.avg_fills_per_trade,
            b.nymex_hour_ratio,
        ]
        for b in behaviors
    ])

    # Standardize features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # K-means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(features_scaled)

    # Analyze cluster centroids to assign archetype labels
    centroids = scaler.inverse_transform(kmeans.cluster_centers_)
    archetype_map = assign_archetypes_to_clusters(centroids)

    # Assign archetypes and weights
    for i, behavior in enumerate(behaviors):
        cluster = labels[i]
        behavior.archetype = archetype_map.get(cluster, TraderArchetype.UNKNOWN)

        # Assign signal weights based on archetype
        if behavior.archetype in SIGNAL_ARCHETYPES:
            behavior.signal_weight = 1.0
        elif behavior.archetype in FADE_ARCHETYPES:
            behavior.signal_weight = -0.5  # Fade these wallets
        elif behavior.archetype in EXCLUDE_ARCHETYPES:
            behavior.signal_weight = 0.0  # Exclude from signal
        else:
            behavior.signal_weight = 0.3  # Unknown gets low weight

    # Log cluster distribution
    archetype_counts = {}
    for b in behaviors:
        archetype_counts[b.archetype.value] = archetype_counts.get(b.archetype.value, 0) + 1
    logger.info(f"Archetype distribution: {archetype_counts}")

    return behaviors


def assign_archetypes_to_clusters(centroids: np.ndarray) -> Dict[int, TraderArchetype]:
    """
    Heuristically assign archetypes to clusters based on centroid characteristics.
    Centroid features: [freq, hold_duration, size_cv, dir_consistency, fills_per_trade, nymex_ratio]
    """
    archetype_map = {}

    for i, centroid in enumerate(centroids):
        freq, hold_dur, size_cv, dir_cons, fills, nymex = centroid

        # Market Maker: high frequency, low hold duration, many fills
        if freq > 5 and hold_dur < 2 and fills > 3:
            archetype_map[i] = TraderArchetype.MARKET_MAKER

        # Directional Trader: low frequency, long holds, high consistency
        elif freq < 2 and hold_dur > 12 and dir_cons > 0.7:
            archetype_map[i] = TraderArchetype.DIRECTIONAL_TRADER

        # Funding Farmer: medium frequency, holds around 8h (funding intervals)
        elif 4 < hold_dur < 12 and freq < 3:
            archetype_map[i] = TraderArchetype.FUNDING_FARMER

        # Momentum Chaser: medium-high frequency, short holds, low consistency
        elif freq > 3 and hold_dur < 6 and dir_cons < 0.6:
            archetype_map[i] = TraderArchetype.MOMENTUM_CHASER

        # Liquidation Hunter: high size variance, short holds
        elif size_cv > 1.5 and hold_dur < 4:
            archetype_map[i] = TraderArchetype.LIQUIDATION_HUNTER

        else:
            archetype_map[i] = TraderArchetype.UNKNOWN

    return archetype_map


def get_wallet_archetype_weight(archetype: TraderArchetype) -> float:
    """Get the signal weight for a given archetype."""
    if archetype in SIGNAL_ARCHETYPES:
        return 1.0
    elif archetype in FADE_ARCHETYPES:
        return -0.5
    elif archetype in EXCLUDE_ARCHETYPES:
        return 0.0
    return 0.3