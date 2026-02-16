"""
analyzer.py — Main analysis pipeline

Orchestrates: fetch market → fetch trades → build positions →
              filter & enrich wallets (all 6 layers) → score → aggregate signal

SPEED IMPROVEMENT:
  Old approach: fetch wallet stats one-by-one, 0.1s sleep each
                100 wallets = ~100 × 0.15s = 15+ seconds just for this step

  New approach: ThreadPoolExecutor with 10 concurrent workers
                100 wallets / 10 concurrent = ~10 batches × 0.5s avg = ~5 seconds
                Roughly 3-5× faster depending on network latency.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from fetcher import (
    fetch_market, fetch_all_trades, build_positions, fetch_wallet_stats,
    MOCK_MARKET, MOCK_POSITIONS, MOCK_STATS,
)
from scorer import score_wallet, aggregate_signal

# How many wallet stat requests to fire simultaneously.
# 10 is safe — aggressive enough to be fast, gentle enough not to get rate-limited.
CONCURRENT_WORKERS = 10


def _fetch_and_score_wallet(
    wallet: str,
    position: Dict,
    market: Dict,
    market_category: str,
    market_start_ts: int,
    min_profit: float,
    use_mock: bool,
) -> Tuple[str, Optional[Dict], Dict]:
    """
    Single-wallet task: fetch stats then score.
    Returns (wallet_address, profile_or_None, stats_dict).

    Designed to run inside a thread. Each thread handles its own
    HTTP request independently — no shared state, no locks needed.
    """
    try:
        if use_mock:
            stats = MOCK_STATS.get(wallet, {})
        else:
            stats = fetch_wallet_stats(wallet, target_category=market_category)
    except Exception:
        stats = {}

    profile = score_wallet(
        wallet_addr=wallet,
        position=position,
        stats=stats,
        market=market,
        market_start_ts=market_start_ts,
        min_profit=min_profit,
    )
    return wallet, profile, stats


def run_analysis(
    market_url: str,
    min_profit: float = 5_000,
    use_mock: bool = False,
    progress: Optional[Callable[[str, int], None]] = None,
) -> Dict:
    """
    Full analysis pipeline with all 6 filtering layers.
    Wallet stats are fetched concurrently for speed.
    """
    def _p(msg: str, pct: int):
        if progress:
            progress(msg, pct)

    t0 = time.time()

    # ── Step 1: Market metadata ───────────────────────────────────────────────
    _p("Fetching market metadata…", 5)
    try:
        if use_mock:
            market = MOCK_MARKET.copy()
        else:
            market = fetch_market(market_url)
        if not market.get("condition_id"):
            return _err("Market resolved or not found")
    except Exception as e:
        return _err(f"Market fetch failed: {e}")

    condition_id    = market["condition_id"]
    market_category = market.get("category", "")

    # ── Step 2: Fetch all trades → build positions ────────────────────────────
    _p("Fetching market trades…", 15)
    try:
        if use_mock:
            raw_positions = MOCK_POSITIONS
        else:
            trades = fetch_all_trades(condition_id, max_pages=6)
            if not trades:
                return _err("No trades found — market may be too new or inactive")
            raw_positions = build_positions(trades)
    except Exception as e:
        return _err(f"Trades fetch failed: {e}")

    if not raw_positions:
        return _err("No current holders found after aggregating trades")

    total_holders = len(raw_positions)

    market_start_ts = min(
        (v["first_trade_ts"] for v in raw_positions.values() if v["first_trade_ts"]),
        default=int(datetime.now(timezone.utc).timestamp()) - 86400 * 90,
    )

    # ── Step 3: Fetch wallet stats CONCURRENTLY ───────────────────────────────
    # Old: for wallet in wallets: fetch(wallet); sleep(0.1)   → sequential
    # New: ThreadPoolExecutor fires 10 requests simultaneously → 3-5× faster
    _p(f"Profiling {total_holders} holders (concurrent)…", 30)

    profiles: List[Dict] = []
    wallets_checked  = 0
    dropped_pnl      = 0
    dropped_realized = 0
    dropped_markets  = 0
    dropped_wins     = 0
    completed        = 0

    wallet_items = list(raw_positions.items())

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        # Submit ALL wallets to the thread pool at once.
        # executor.submit() is non-blocking — it queues the work and returns
        # a Future object immediately. The actual HTTP requests run in parallel.
        futures = {
            executor.submit(
                _fetch_and_score_wallet,
                wallet, position,
                market, market_category,
                market_start_ts, min_profit,
                use_mock,
            ): wallet
            for wallet, position in wallet_items
        }

        # as_completed() yields each Future as soon as it finishes —
        # we don't have to wait for all of them before processing results.
        for future in as_completed(futures):
            completed += 1

            # Update progress bar every 10 completions
            if completed % 10 == 0 or completed == total_holders:
                pct = 30 + int((completed / total_holders) * 55)
                _p(f"Profiled {completed}/{total_holders} wallets…", pct)

            try:
                wallet, profile, stats = future.result()
            except Exception:
                continue

            wallets_checked += 1

            # Track which filter dropped this wallet
            pnl         = stats.get("total_pnl", 0)
            realized    = stats.get("realized_pnl", 0)
            mkts        = stats.get("markets_traded", 0)
            closed_wins = stats.get("closed_wins", 0)

            if pnl < min_profit:
                dropped_pnl += 1
            elif realized < 500:
                dropped_realized += 1
            elif mkts < 5:
                dropped_markets += 1
            elif closed_wins < 3:
                dropped_wins += 1

            if profile is not None:
                profiles.append(profile)

    if not profiles:
        return _err(
            f"No wallets passed all filters ({wallets_checked} checked). "
            f"Dropped: {dropped_pnl} below ${min_profit:,.0f} profit, "
            f"{dropped_realized} below $500 realized, "
            f"{dropped_markets} below 5 markets, "
            f"{dropped_wins} below 3 wins."
        )

    # ── Step 4: Rank and signal ───────────────────────────────────────────────
    _p("Generating signal…", 90)
    profiles.sort(key=lambda x: x["composite"], reverse=True)
    signal = aggregate_signal(profiles)

    elapsed = round(time.time() - t0, 1)
    _p("Done", 100)

    return {
        "success":  True,
        "market":   market,
        "signal":   signal,
        "profiles": profiles,
        "stats": {
            "total_holders":   total_holders,
            "wallets_checked": wallets_checked,
            "qualified":       len(profiles),
            "elapsed_s":       elapsed,
        },
        "filter_stats": {
            "dropped_below_pnl":      dropped_pnl,
            "dropped_below_realized": dropped_realized,
            "dropped_below_markets":  dropped_markets,
            "dropped_below_wins":     dropped_wins,
        },
    }


def _err(msg: str) -> Dict:
    return {"success": False, "error": msg}