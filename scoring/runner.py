"""
Scoring Pipeline Runner

Orchestrates the full scoring pipeline:
1. Reconstruct trades from fills
2. Classify wallet behavior
3. Compute performance scores
4. Persist results
"""

import logging
from typing import List, Dict
from datetime import datetime

import duckdb

from scoring.trade_reconstruction import (
    reconstruct_all_wallets,
    RoundTrip
)
from scoring.behavioral_classifier import (
    compute_behavioral_features,
    classify_wallets,
    WalletBehavior
)
from scoring.performance_scorer import (
    score_all_wallets,
    persist_scores,
    WalletScore
)

logger = logging.getLogger(__name__)


class ScoringPipeline:
    """Orchestrates the wallet scoring pipeline."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.last_run: datetime = None
        self.last_results: Dict = {}

    def run(self, min_fills: int = 5, lookback_days: int = 60) -> Dict:
        """
        Run the full scoring pipeline.

        Returns dict with summary statistics.
        """
        start_time = datetime.utcnow()
        logger.info("Starting scoring pipeline run")

        # Step 1: Reconstruct round trips
        logger.info("Step 1: Reconstructing round trips from fills")
        all_round_trips = reconstruct_all_wallets(
            self.conn, min_fills=min_fills, lookback_days=lookback_days
        )

        if not all_round_trips:
            logger.warning("No round trips found, skipping scoring")
            return {'status': 'no_data', 'wallets': 0}

        # Step 2: Compute behavioral features
        logger.info("Step 2: Computing behavioral features")
        behaviors: List[WalletBehavior] = []
        for wallet, trips in all_round_trips.items():
            behavior = compute_behavioral_features(wallet, trips, self.conn)
            if behavior:
                behaviors.append(behavior)

        # Step 3: Classify wallets
        if len(behaviors) >= 5:
            logger.info("Step 3: Classifying wallet archetypes")
            behaviors = classify_wallets(behaviors, n_clusters=min(5, len(behaviors)))
        else:
            logger.info("Step 3: Skipping clustering (too few wallets)")

        behavior_map = {b.wallet: b for b in behaviors}

        # Step 4: Compute performance scores
        logger.info("Step 4: Computing performance scores")
        scores = score_all_wallets(all_round_trips, behavior_map)

        # Step 5: Persist results
        logger.info("Step 5: Persisting scores to database")
        persist_scores(self.conn, scores)

        # Summary
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        self.last_run = datetime.utcnow()

        results = {
            'status': 'success',
            'elapsed_seconds': elapsed,
            'wallets_analyzed': len(all_round_trips),
            'total_round_trips': sum(len(trips) for trips in all_round_trips.values()),
            'wallets_scored': len([s for s in scores if s.is_scoreable]),
            'strong_positive': len([s for s in scores if s.final_score > 0.3]),
            'strong_negative': len([s for s in scores if s.final_score < -0.3]),
            'archetype_distribution': self._count_archetypes(behaviors),
        }

        self.last_results = results
        logger.info(f"Scoring pipeline complete in {elapsed:.1f}s: {results}")

        return results

    def _count_archetypes(self, behaviors: List[WalletBehavior]) -> Dict[str, int]:
        """Count wallets by archetype."""
        counts = {}
        for b in behaviors:
            key = b.archetype.value
            counts[key] = counts.get(key, 0) + 1
        return counts

    def get_top_wallets(self, n: int = 20, direction: str = 'both') -> List[Dict]:
        """
        Get top-scoring wallets from the database.

        Args:
            n: Number of wallets to return
            direction: 'positive', 'negative', or 'both'
        """
        if direction == 'positive':
            where = "WHERE composite_score > 0"
            order = "ORDER BY composite_score DESC"
        elif direction == 'negative':
            where = "WHERE composite_score < 0"
            order = "ORDER BY composite_score ASC"
        else:
            where = ""
            order = "ORDER BY ABS(composite_score) DESC"

        query = f"""
            SELECT wallet, composite_score, archetype, archetype_weight,
                   trade_count, pnl_score, timing_score, updated_at
            FROM wallet_scores
            WHERE is_scoreable = TRUE
            {where if direction != 'both' else ''}
            {order}
            LIMIT ?
        """

        results = self.conn.execute(query, [n]).fetchall()

        return [
            {
                'wallet': row[0],
                'score': row[1],
                'archetype': row[2],
                'weight': row[3],
                'trades': row[4],
                'pnl_score': row[5],
                'timing_score': row[6],
                'updated': row[7],
            }
            for row in results
        ]

    def get_scoring_summary(self) -> Dict:
        """Get summary of current wallet scores."""
        result = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_scoreable THEN 1 ELSE 0 END) as scoreable,
                SUM(CASE WHEN composite_score > 0.3 THEN 1 ELSE 0 END) as strong_positive,
                SUM(CASE WHEN composite_score < -0.3 THEN 1 ELSE 0 END) as strong_negative,
                AVG(composite_score) as avg_score,
                MAX(updated_at) as last_update
            FROM wallet_scores
        """).fetchone()

        return {
            'total_wallets': result[0],
            'scoreable_wallets': result[1],
            'strong_positive': result[2],
            'strong_negative': result[3],
            'average_score': result[4],
            'last_update': result[5],
        }


def run_scoring_pipeline(conn: duckdb.DuckDBPyConnection) -> Dict:
    """Convenience function to run the scoring pipeline."""
    pipeline = ScoringPipeline(conn)
    return pipeline.run()