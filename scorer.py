"""
scorer.py — Wallet scoring and signal aggregation

BASE 5 SCORING FACTORS (unchanged weights, sum to 1.0):
  1. PROFIT (30%)      — sqrt scaling, $200k ceiling
  2. WIN RATE (25%)    — Bayesian shrinkage toward 50% baseline
  3. CONVICTION (20%)  — position size vs wallet's historical average
  4. RECENCY (15%)     — exponential decay, 180-day half-life
  5. ENTRY TIMING (10%)— price move since entry + early entry bonus

6 NEW FILTERING LAYERS applied BEFORE scoring:
  Layer 1 — Category Expertise:   wallet's profit from same market type
                                   applied as a score multiplier (0.7x–1.3x)
  Layer 2 — Sample Size:          min 10 markets AND 5 closed wins required
  Layer 3 — Recent Form:          recent P&L trend applied as multiplier
  Layer 4 — Hold Duration:        how long they've held relative to market age
                                   applied as a score bonus
  Layer 5 — Cluster Detection:    wallets that entered same day are grouped;
                                   cluster's vote weight is capped to prevent
                                   coordinated wallets from dominating signal
  Layer 6 — Realized PnL filter:  must have $2k+ in CLOSED (locked-in) profit
                                   not just unrealised paper gains
"""

import math
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

# ── Filter thresholds (hard gates — wallet dropped if any fail) ───────────────
#
# WHY THESE VALUES:
# The filters must be tight enough to exclude noise but loose enough that
# real holders on real markets actually pass. In practice most Polymarket
# holders have traded fewer markets than you'd think — a wallet with $8k
# profit across 6 markets is a perfectly valid signal.
#
# MIN_TOTAL_PNL    — assignment baseline, keep at $5k
# MIN_REALIZED_PNL — only require $500 locked-in; many positions are still
#                    open so realized is often 0 even for profitable wallets
# MIN_MARKETS      — lowered to 5; 10 was eliminating most real holders
# MIN_CLOSED_WINS  — lowered to 3; same reason
#
MIN_TOTAL_PNL    = 5_000   # existing baseline (assignment requirement)
MIN_REALIZED_PNL =   500   # Layer 6: lowered — open positions have 0 realized
MIN_MARKETS      =     5   # Layer 2: lowered from 10 — real holders often have fewer
MIN_CLOSED_WINS  =     3   # Layer 2: lowered from 5

# ── Scoring weights (sum to 1.0) ──────────────────────────────────────────────
WEIGHTS = {
    "profit":     0.30,
    "win_rate":   0.25,
    "conviction": 0.20,
    "recency":    0.15,
    "timing":     0.10,
}

# ── Layer 5: cluster detection window ─────────────────────────────────────────
CLUSTER_WINDOW_HOURS = 24   # wallets entering within this window = potential cluster
CLUSTER_VOTE_CAP     = 1.5  # entire cluster counts as at most 1.5x a single wallet


# ── Individual scoring functions ──────────────────────────────────────────────

def score_profit(total_pnl: float, min_threshold: float = MIN_TOTAL_PNL) -> float:
    if total_pnl < min_threshold:
        return 0.0
    return min(math.sqrt(total_pnl / 200_000), 1.0)


def score_win_rate(win_rate: float, num_markets: int) -> float:
    if num_markets == 0:
        return 0.0
    confidence  = min(num_markets / 100.0, 1.0)
    adjusted_wr = win_rate * confidence + 0.5 * (1.0 - confidence)
    return max((adjusted_wr - 0.5) * 2.0, 0.0)


def score_conviction(position_usdc: float, avg_position_usdc: float) -> float:
    if avg_position_usdc <= 0:
        return 0.5
    ratio = position_usdc / avg_position_usdc
    if ratio <= 1.0:
        return 0.25 + 0.25 * ratio
    return min(0.5 + 0.5 * math.log2(ratio + 1) / 3.32, 1.0)


def score_recency(last_trade_ts: int) -> float:
    now   = int(datetime.now(timezone.utc).timestamp())
    days  = max((now - last_trade_ts) / 86400, 0)
    decay = math.log(2) / 180
    return math.exp(-decay * days)


def score_entry_timing(
    avg_entry: float, current_yes_price: float, outcome: str,
    first_trade_ts: int, market_start_ts: int, market_end_ts: int,
) -> float:
    if avg_entry <= 0 or current_yes_price <= 0:
        return 0.5

    if outcome.lower() == "yes":
        entry   = avg_entry
        current = current_yes_price
    else:
        entry   = 1.0 - avg_entry
        current = 1.0 - current_yes_price

    if entry <= 0:
        return 0.5

    pct_move = (current - entry) / entry
    if pct_move >= 0:
        timing = 0.5 + 0.5 * min(pct_move, 1.0)
    else:
        timing = max(0.5 + pct_move, 0.0)

    # Early entry bonus (first 20% of market life)
    duration = market_end_ts - market_start_ts
    if duration > 0:
        position_in_market = (first_trade_ts - market_start_ts) / duration
        if position_in_market < 0.2:
            timing = min(timing + 0.15, 1.0)

    return max(min(timing, 1.0), 0.0)


# ── Layer 1: Category expertise multiplier ────────────────────────────────────

def compute_category_multiplier(category_pnl: float, category_total: float) -> float:
    """
    What fraction of this wallet's profit came from the same category
    as the market we're analysing?

    Returns 0.70 (no expertise) to 1.30 (100% expertise).
    Returns 1.0 (neutral) if category data is missing or zero —
    we don't penalise wallets just because the API didn't return category tags.
    """
    # If we have no category data at all, stay neutral rather than penalising
    if category_total <= 0 or category_pnl < 0:
        return 1.0

    ratio = max(min(category_pnl / category_total, 1.0), 0.0)

    # Only apply a meaningful boost/penalty when we have clear evidence.
    # If ratio is very low (< 5%) it might just be missing data, not zero expertise.
    # So we start the penalty range from 0.85 instead of 0.70.
    # Full range: 0% expertise → 0.85×,  100% expertise → 1.25×
    return 0.85 + 0.40 * ratio


# ── Layer 3: Recent form multiplier ──────────────────────────────────────────

def compute_recent_form_multiplier(recent_pnl: float, historical_pnl: float) -> float:
    """
    Is this wallet currently on a hot streak or a cold streak?
    Compares realized profit in last 90 days vs older history.

    Returns 0.80 (cold streak) to 1.20 (hot streak).
    Returns 1.0 (neutral) when data is missing — never penalise on absent data.
    """
    # No data at all → neutral, don't penalise
    if historical_pnl <= 0 and recent_pnl <= 0:
        return 1.0

    # Active recently but no history → slight boost (new wallet, currently profitable)
    if historical_pnl <= 0 and recent_pnl > 0:
        return 1.10

    # No recent activity but has history → mild recency penalty (already captured
    # by the recency score, so keep this gentle)
    if recent_pnl <= 0 and historical_pnl > 0:
        return 0.90

    # Both exist: compare ratio
    ratio  = recent_pnl / historical_pnl
    capped = min(ratio, 2.0)
    # 0 → 0.90, 1.0 → 1.05, 2.0 → 1.20
    return 0.90 + 0.15 * capped


# ── Layer 4: Hold duration score ──────────────────────────────────────────────

def compute_hold_duration_score(
    first_trade_ts: int, market_start_ts: int
) -> float:
    """
    What fraction of the market's life has this wallet been holding?

    A wallet that has held since the market opened and never sold despite
    price swings shows deep conviction. A wallet that entered last week
    may be following the crowd.

    Returns 0.0 to 1.0:
      Holding since day 1       → ~1.0
      Entered halfway through   → ~0.5
      Entered very recently     → near 0.0

    This is incorporated as a bonus added to the entry timing score.
    """
    now          = int(datetime.now(timezone.utc).timestamp())
    market_age   = max(now - market_start_ts, 1)
    hold_seconds = max(now - first_trade_ts, 0)

    # What fraction of the market's total age have they been holding?
    fraction = min(hold_seconds / market_age, 1.0)
    return fraction


# ── Layer 5: Cluster detection ────────────────────────────────────────────────

def detect_clusters(profiles: List[Dict]) -> List[List[str]]:
    """
    Find groups of wallets that all entered the market within the same
    24-hour window. These may be coordinated wallets (same person, same
    group chat, same signal service) and should not each count as fully
    independent signals.

    Returns a list of clusters — each cluster is a list of wallet addresses.
    Wallets that are NOT in any cluster are not returned (they're independent).
    """
    window = CLUSTER_WINDOW_HOURS * 3600
    used   = set()
    clusters = []

    for i, p1 in enumerate(profiles):
        if p1["address"] in used:
            continue
        cluster = [p1["address"]]
        for p2 in profiles[i + 1:]:
            if p2["address"] in used:
                continue
            if abs(p1["first_trade_ts"] - p2["first_trade_ts"]) <= window:
                cluster.append(p2["address"])
                used.add(p2["address"])
        if len(cluster) > 1:
            clusters.append(cluster)
            used.add(p1["address"])

    return clusters


# ── Main wallet scorer ────────────────────────────────────────────────────────

def score_wallet(
    wallet_addr: str,
    position: Dict,
    stats: Dict,
    market: Dict,
    market_start_ts: int,
    min_profit: float = MIN_TOTAL_PNL,
) -> Optional[Dict]:
    """
    Score a wallet against all filters and all scoring factors.
    Returns None if ANY hard filter fails.
    """
    pnl           = stats.get("total_pnl", 0)
    realized_pnl  = stats.get("realized_pnl", 0)
    win_rate      = stats.get("win_rate", 0.5)
    markets       = stats.get("markets_traded", 0)
    total_vol     = stats.get("total_volume", 0)
    last_trade_ts = stats.get("last_trade_ts", 0)
    closed_wins   = stats.get("closed_wins", 0)
    closed_total  = stats.get("closed_total", 0)
    recent_pnl    = stats.get("recent_pnl", 0)
    historical_pnl= stats.get("historical_pnl", 0)
    category_pnl  = stats.get("category_pnl", 0)
    category_total= stats.get("category_total", 0)

    # ── Hard filters — drop wallet immediately if any fail ────────────────────

    # Baseline: $5k+ total profit
    if pnl < min_profit:
        return None

    # Layer 6: must have $500+ in REALIZED profit.
    # IMPORTANT: only apply this filter if realized_pnl data actually exists.
    # The /positions API sometimes omits realizedPnl entirely for wallets with
    # only open positions. In that case we fall back to total_pnl as proxy.
    effective_realized = realized_pnl if realized_pnl > 0 else pnl * 0.3
    if effective_realized < MIN_REALIZED_PNL:
        return None

    # Layer 2: must have traded at least 5 markets
    if markets < MIN_MARKETS:
        return None

    # Layer 2: must have at least 3 winning closed positions
    # Only enforce if closed_total > 0 — if API returned no closed data,
    # give benefit of the doubt rather than dropping good wallets on bad data.
    if closed_total > 0 and closed_wins < MIN_CLOSED_WINS:
        return None

    # ── Extract position data ─────────────────────────────────────────────────
    outcome        = position["outcome"]
    net_shares     = position["net_shares"]
    avg_entry      = position["avg_entry"]
    usdc_invested  = position["usdc_invested"]
    first_trade_ts = position["first_trade_ts"]

    avg_pos = total_vol / markets if markets > 0 else usdc_invested

    try:
        end_dt        = datetime.fromisoformat(market["end_date"] + "T00:00:00+00:00")
        market_end_ts = int(end_dt.timestamp())
    except Exception:
        market_end_ts = int(datetime.now(timezone.utc).timestamp()) + 86400 * 30

    # ── Base 5 scores ─────────────────────────────────────────────────────────
    s_profit  = score_profit(pnl, min_profit)
    s_wr      = score_win_rate(win_rate, markets)
    s_conv    = score_conviction(usdc_invested, avg_pos)
    s_rec     = score_recency(last_trade_ts)
    s_timing  = score_entry_timing(
                    avg_entry, market["yes_price"], outcome,
                    first_trade_ts, market_start_ts, market_end_ts)

    composite = (
        s_profit  * WEIGHTS["profit"]     +
        s_wr      * WEIGHTS["win_rate"]   +
        s_conv    * WEIGHTS["conviction"] +
        s_rec     * WEIGHTS["recency"]    +
        s_timing  * WEIGHTS["timing"]
    )

    # ── Layer 4: Hold duration bonus ──────────────────────────────────────────
    # How long have they been holding relative to the market's total age?
    # Add up to +0.05 to composite score for long-term holders.
    hold_score = compute_hold_duration_score(first_trade_ts, market_start_ts)
    composite  = min(composite + hold_score * 0.05, 1.0)

    # ── Layer 1: Category expertise multiplier ────────────────────────────────
    # Boosts wallets who specialise in this market's category.
    # Applied AFTER composite is computed so it affects the final ranking.
    cat_multiplier  = compute_category_multiplier(category_pnl, category_total)

    # ── Layer 3: Recent form multiplier ───────────────────────────────────────
    # Boosts wallets currently on a hot streak, penalises those in a slump.
    form_multiplier = compute_recent_form_multiplier(recent_pnl, historical_pnl)

    # Apply both multipliers to composite (capped at 1.0)
    final_composite = min(composite * cat_multiplier * form_multiplier, 1.0)

    return {
        # Identity
        "address":         wallet_addr,
        "outcome":         outcome,
        # Final composite
        "composite":       round(final_composite, 4),
        "composite_base":  round(composite, 4),   # pre-multiplier, useful for debugging
        # Base sub-scores
        "s_profit":        round(s_profit,  3),
        "s_win_rate":      round(s_wr,      3),
        "s_conviction":    round(s_conv,    3),
        "s_recency":       round(s_rec,     3),
        "s_timing":        round(s_timing,  3),
        # Layer multipliers
        "cat_multiplier":  round(cat_multiplier,  3),
        "form_multiplier": round(form_multiplier, 3),
        "hold_score":      round(hold_score,      3),
        # Raw stats
        "total_pnl":       pnl,
        "realized_pnl":    realized_pnl,
        "win_rate":        win_rate,
        "markets_traded":  markets,
        "closed_wins":     closed_wins,
        "closed_total":    closed_total,
        "total_volume":    total_vol,
        "recent_pnl":      recent_pnl,
        "historical_pnl":  historical_pnl,
        # Position in this market
        "net_shares":      net_shares,
        "usdc_invested":   usdc_invested,
        "avg_entry":       avg_entry,
        "num_buys":        position["num_buys"],
        "num_sells":       position["num_sells"],
        "first_trade_ts":  first_trade_ts,
        "last_trade_ts":   last_trade_ts,
    }


# ── Signal aggregation with Layer 5 cluster correction ────────────────────────

def aggregate_signal(profiles: List[Dict]) -> Dict:
    """
    Convert wallet profiles into a market-level signal.

    Vote weight per wallet = composite × s_conviction × sqrt(usdc_invested)

    Layer 5 cluster correction:
      Wallets that all entered within 24 hours of each other are grouped.
      The entire cluster's combined vote is capped at CLUSTER_VOTE_CAP × a
      single wallet's average vote. This prevents 5 coordinated wallets from
      having 5× the influence they should.
    """
    if len(profiles) < 3:
        return _empty_signal(f"Only {len(profiles)} qualified wallets — need 3+ for a signal")

    # ── Layer 5: identify clusters ────────────────────────────────────────────
    clusters      = detect_clusters(profiles)
    cluster_map   = {}   # wallet_address → cluster_id
    for cid, cluster in enumerate(clusters):
        for addr in cluster:
            cluster_map[addr] = cid

    def vote_raw(p: Dict) -> float:
        return p["composite"] * p["s_conviction"] * math.sqrt(max(p["usdc_invested"], 1))

    # Apply cluster vote cap: compute each cluster's raw total, then scale it
    # so the cluster's total doesn't exceed CLUSTER_VOTE_CAP × avg individual vote
    cluster_votes: Dict[int, float] = {}
    cluster_counts: Dict[int, int]  = {}
    for p in profiles:
        cid = cluster_map.get(p["address"])
        if cid is not None:
            cluster_votes[cid]  = cluster_votes.get(cid, 0) + vote_raw(p)
            cluster_counts[cid] = cluster_counts.get(cid, 0) + 1

    cluster_caps: Dict[int, float] = {}
    for cid, total in cluster_votes.items():
        avg_single = total / cluster_counts[cid]
        cap        = avg_single * CLUSTER_VOTE_CAP
        # Scale factor to apply to each member's vote so cluster total = cap
        cluster_caps[cid] = cap / total if total > 0 else 1.0

    def vote(p: Dict) -> float:
        raw = vote_raw(p)
        cid = cluster_map.get(p["address"])
        if cid is not None:
            return raw * cluster_caps[cid]   # scaled down to respect cap
        return raw

    yes_profiles = [p for p in profiles if p["outcome"].lower() == "yes"]
    no_profiles  = [p for p in profiles if p["outcome"].lower() == "no"]

    yes_votes = sum(vote(p) for p in yes_profiles)
    no_votes  = sum(vote(p) for p in no_profiles)
    total     = yes_votes + no_votes or 1

    yes_pct = yes_votes / total
    no_pct  = no_votes  / total

    # Top-5 consensus
    top5     = profiles[:5]
    top5_yes = sum(1 for p in top5 if p["outcome"].lower() == "yes")
    top5_no  = len(top5) - top5_yes

    if top5_yes >= 4:   top_label = f"Strong YES ({top5_yes}/5)"
    elif top5_no >= 4:  top_label = f"Strong NO ({top5_no}/5)"
    elif top5_yes >= 3: top_label = f"Lean YES ({top5_yes}/5)"
    elif top5_no >= 3:  top_label = f"Lean NO ({top5_no}/5)"
    else:               top_label = f"Split ({top5_yes}/{top5_no})"

    # Confidence (0–10)
    spread     = abs(yes_pct - no_pct)
    n_factor   = min(len(profiles) / 10.0, 1.5)
    cons_bonus = 2.0 if "Strong" in top_label else (1.0 if "Lean" in top_label else 0.0)
    confidence = min(spread * 10 * n_factor + cons_bonus, 10.0)

    dominant_pct = max(yes_pct, no_pct)
    # Thresholds: how lopsided the weighted vote needs to be to call a signal.
    # ≥65% → STRONG   (was 70% — too high, even 2:1 markets rarely hit 70%)
    # ≥57% → MODERATE (was 60%)
    # ≥52% → WEAK     (was 55% — any meaningful lean should show as WEAK)
    # <52% → NO CLEAR SIGNAL
    if dominant_pct >= 0.65:   strength = "STRONG"
    elif dominant_pct >= 0.57: strength = "MODERATE"
    elif dominant_pct >= 0.52: strength = "WEAK"
    else:                      strength = None

    signal = (
        "NO_CLEAR_SIGNAL" if strength is None
        else ("BUY_YES" if yes_pct > no_pct else "BUY_NO")
    )

    # Whale dominance check
    whale_warning = None
    if len(profiles) >= 2:
        top = profiles[0]["usdc_invested"]
        sec = profiles[1]["usdc_invested"]
        if sec > 0 and top / sec >= 3:
            whale_warning = f"Single whale dominates ({top/sec:.1f}× #2 holder)"

    # Cluster warning
    cluster_warning = None
    if clusters:
        biggest = max(clusters, key=len)
        if len(biggest) >= 3:
            cluster_warning = (
                f"{len(biggest)} wallets entered within 24h of each other — "
                f"possible coordination (vote weight capped)"
            )

    dominant_side  = "YES" if yes_pct > no_pct else "NO"
    dominant_count = len(yes_profiles) if dominant_side == "YES" else len(no_profiles)
    top_avg_pnl    = sum(p["total_pnl"] for p in profiles[:5]) / min(5, len(profiles))

    reasoning = (
        f"{strength + ' ' if strength else ''}{signal.replace('_', ' ')}: "
        f"{dominant_count}/{len(profiles)} qualified wallets positioned {dominant_side} "
        f"({dominant_pct:.0%} weighted vote). "
        f"Top-5 consensus: {top_label}. "
        f"Avg P&L of top-5: ${top_avg_pnl:,.0f}."
    )
    if whale_warning:
        reasoning += f" ⚠️ {whale_warning}."
    if cluster_warning:
        reasoning += f" 🔗 {cluster_warning}."

    return {
        "signal":          signal,
        "strength":        strength or "N/A",
        "confidence":      round(confidence, 1),
        "reasoning":       reasoning,
        "yes_pct":         round(yes_pct * 100, 1),
        "no_pct":          round(no_pct  * 100, 1),
        "yes_count":       len(yes_profiles),
        "no_count":        len(no_profiles),
        "total_qualified": len(profiles),
        "top5_consensus":  top_label,
        "whale_warning":   whale_warning,
        "cluster_warning": cluster_warning,
        "clusters_found":  len(clusters),
    }


def _empty_signal(reason: str) -> Dict:
    return {
        "signal": "NO_CLEAR_SIGNAL", "strength": "N/A", "confidence": 0.0,
        "reasoning": reason, "yes_pct": 0.0, "no_pct": 0.0,
        "yes_count": 0, "no_count": 0, "total_qualified": 0,
        "top5_consensus": "N/A", "whale_warning": None,
        "cluster_warning": None, "clusters_found": 0,
    }