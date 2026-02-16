"""
fetcher.py — Polymarket data layer

Strategy: use /trades (not /holders) as the primary source.
- /trades paginates to get EVERY wallet that ever touched the market
- Aggregate buys - sells per wallet to get current positions
- /positions per wallet to get lifetime P&L, win rate, market count

Additional data now collected per wallet for new filtering layers:
  - realized_pnl      (Layer 6: only locked-in profit, not paper gains)
  - closed_wins       (Layer 2: minimum winning closed positions)
  - recent_pnl        (Layer 3: profit in last 90 days)
  - historical_pnl    (Layer 3: profit older than 90 days)
  - category_pnl      (Layer 1: profit from same category as target market)
  - category_total    (Layer 1: total profit as denominator)
"""

import requests
import time
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

_S = requests.Session()
_S.headers.update({"Accept": "application/json", "User-Agent": "PolySmartMoney/2.0"})

GAMMA = "https://gamma-api.polymarket.com"
DATA  = "https://data-api.polymarket.com"
T     = 15

RECENT_WINDOW_DAYS = 90   # Layer 3: what counts as "recent"


# ── 1. Market metadata ────────────────────────────────────────────────────────

def parse_url(url: str) -> Tuple[str, Optional[str]]:
    clean = url.strip().split("?")[0].rstrip("/")
    for prefix in ("https://polymarket.com", "http://polymarket.com", "polymarket.com"):
        if clean.startswith(prefix):
            clean = clean[len(prefix):]
    parts = [p for p in clean.split("/") if p]
    if len(parts) >= 2 and parts[0] == "event":
        return parts[1], (parts[2] if len(parts) >= 3 else None)
    raise ValueError(f"Cannot parse URL: {url}")


def fetch_market(url: str) -> Dict:
    event_slug, market_slug = parse_url(url)
    resp = _S.get(f"{GAMMA}/events", params={"slug": event_slug}, timeout=T)
    resp.raise_for_status()
    events = resp.json()
    if not events:
        raise ValueError(f"No event found for slug '{event_slug}'")

    event   = events[0] if isinstance(events, list) else events
    markets = event.get("markets", [])
    if not markets:
        raise ValueError(f"Event '{event_slug}' has no markets")

    if market_slug and len(markets) > 1:
        chosen = next((m for m in markets if m.get("slug") == market_slug), markets[0])
    else:
        chosen = markets[0]

    try:
        prices = json.loads(chosen.get("outcomePrices", "[0.5,0.5]"))
    except Exception:
        prices = [0.5, 0.5]

    # Layer 1: capture the market category so we can match it against wallet history
    category = (
        event.get("category")
        or (event.get("tags") or [{}])[0].get("label", "")
        or chosen.get("category", "")
        or "General"
    )

    return {
        "condition_id": chosen.get("conditionId", ""),
        "question":     chosen.get("question") or event.get("title", "Unknown"),
        "yes_price":    float(prices[0]) if prices else 0.5,
        "no_price":     float(prices[1]) if len(prices) > 1 else 0.5,
        "volume":       float(chosen.get("volumeNum", 0) or 0),
        "liquidity":    float(chosen.get("liquidityNum", 0) or 0),
        "end_date":     chosen.get("endDateIso", ""),
        "category":     category,
    }


# ── 2. Trades → reconstruct current positions ─────────────────────────────────

def fetch_all_trades(condition_id: str, max_pages: int = 6) -> List[Dict]:
    trades = []
    offset = 0
    limit  = 500

    for _ in range(max_pages):
        try:
            r = _S.get(
                f"{DATA}/trades",
                params={"market": condition_id, "limit": limit, "offset": offset},
                timeout=T,
            )
            r.raise_for_status()
            batch = r.json() or []
        except Exception as e:
            print(f"[trades] offset={offset}: {e}")
            break

        trades.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.12)

    return trades


def build_positions(trades: List[Dict]) -> Dict[str, Dict]:
    raw: Dict[str, Dict] = {}

    for t in trades:
        wallet = t.get("proxyWallet", "")
        if not wallet:
            continue

        side        = (t.get("side") or "").upper()
        size        = float(t.get("size", 0) or 0)
        price       = float(t.get("price", 0) or 0)
        usdc        = float(t.get("usdcSize", 0) or 0) or price * size
        outcome     = t.get("outcome", "Yes")
        outcome_idx = int(t.get("outcomeIndex", 0) or 0)
        ts          = int(t.get("timestamp", 0) or 0)

        if wallet not in raw:
            raw[wallet] = {
                "outcome": outcome, "outcome_index": outcome_idx,
                "net_shares": 0.0, "usdc_invested": 0.0,
                "total_buy_shares": 0.0, "avg_entry": 0.0,
                "first_trade_ts": ts, "last_trade_ts": ts,
                "num_buys": 0, "num_sells": 0,
            }

        w = raw[wallet]
        w["first_trade_ts"] = min(w["first_trade_ts"], ts) if ts else w["first_trade_ts"]
        w["last_trade_ts"]  = max(w["last_trade_ts"],  ts) if ts else w["last_trade_ts"]

        if side == "BUY":
            w["net_shares"]       += size
            w["usdc_invested"]    += usdc
            w["total_buy_shares"] += size
            w["num_buys"]         += 1
            w["outcome"]       = outcome
            w["outcome_index"] = outcome_idx
        elif side == "SELL":
            w["net_shares"] -= size
            w["num_sells"]  += 1

    positions = {}
    for wallet, w in raw.items():
        if w["total_buy_shares"] > 0:
            w["avg_entry"] = w["usdc_invested"] / w["total_buy_shares"]
        if w["net_shares"] > 5:
            positions[wallet] = w

    return positions


# ── 3. Lifetime wallet stats — enriched for all 6 layers ─────────────────────

def fetch_wallet_stats(wallet: str, target_category: str = "") -> Dict:
    """
    Fetches all historical positions for a wallet and computes:

    Existing:
      total_pnl, win_rate, markets_traded, total_volume, last_trade_ts

    Layer 6 — Realized PnL only:
      realized_pnl    — sum of realizedPnl only (locked-in, not paper gains)

    Layer 2 — Sample size:
      closed_wins     — count of winning closed positions
      closed_total    — count of all closed positions

    Layer 3 — Recent form:
      recent_pnl      — realized profit in last 90 days
      historical_pnl  — realized profit older than 90 days

    Layer 1 — Category expertise:
      category_pnl    — total PnL from markets matching target_category
      category_total  — total PnL across all categories (denominator)
    """
    try:
        r = _S.get(
            f"{DATA}/positions",
            params={"user": wallet, "sizeThreshold": 0, "limit": 500},
            timeout=T,
        )
        if r.status_code != 200:
            return {}
        positions = r.json() or []
    except Exception:
        return {}

    if not positions:
        return {}

    now    = int(datetime.now(timezone.utc).timestamp())
    cutoff = now - RECENT_WINDOW_DAYS * 86400

    total_pnl    = total_vol = 0.0
    realized_pnl = 0.0
    wins = closed = closed_wins = 0
    market_ids   = set()
    last_ts      = 0
    recent_pnl   = historical_pnl = 0.0
    category_pnl = category_total = 0.0

    for p in positions:
        cash         = float(p.get("cashPnl",     0) or 0)
        real         = float(p.get("realizedPnl", 0) or 0)
        cur          = float(p.get("currentValue",0) or 0)
        pos_category = (p.get("category") or "").strip().lower()

        total_pnl    += cash + real
        realized_pnl += real
        total_vol    += cur

        # Layer 1 — accumulate by category
        category_total += cash + real
        if target_category and pos_category == target_category.lower():
            category_pnl += cash + real

        cid = p.get("conditionId")
        if cid:
            market_ids.add(cid)

        # Determine if position is closed
        is_closed = bool(p.get("redeemable")) or real != 0
        if is_closed:
            closed += 1
            if real > 0:
                wins       += 1
                closed_wins += 1   # Layer 2

        # Parse end timestamp for Layer 3 split
        end_ts = 0
        end    = p.get("endDate", "")
        if end:
            try:
                dt     = datetime.fromisoformat(end.replace("Z", "+00:00"))
                end_ts = int(dt.timestamp())
                last_ts = max(last_ts, end_ts)
            except Exception:
                pass

        # Layer 3 — split realized PnL: recent vs historical
        if is_closed and real != 0:
            if end_ts >= cutoff:
                recent_pnl     += real
            else:
                historical_pnl += real

    return {
        # Core
        "total_pnl":      total_pnl,
        "win_rate":       wins / closed if closed > 0 else 0.5,
        "markets_traded": len(market_ids) or max(closed, 1),
        "total_volume":   total_vol,
        "last_trade_ts":  last_ts or now - 86400 * 30,
        # Layer 6
        "realized_pnl":   realized_pnl,
        # Layer 2
        "closed_wins":    closed_wins,
        "closed_total":   closed,
        # Layer 3
        "recent_pnl":     recent_pnl,
        "historical_pnl": historical_pnl,
        # Layer 1
        "category_pnl":   category_pnl,
        "category_total": category_total,
    }


# ── Mock data ─────────────────────────────────────────────────────────────────

MOCK_MARKET = {
    "condition_id": "0xmock0001",
    "question":     "Will Bitcoin exceed $120,000 by end of Q2 2025?",
    "yes_price":    0.54,
    "no_price":     0.46,
    "volume":       1_450_000,
    "liquidity":    520_000,
    "end_date":     "2025-06-30",
    "category":     "Crypto",
}

def _ts(days_ago: int) -> int:
    return int(datetime.now(timezone.utc).timestamp()) - days_ago * 86400

MOCK_POSITIONS = {
    "0xWhale001": {"outcome":"Yes","outcome_index":0,"net_shares":95000,"usdc_invested":31350,"total_buy_shares":95000,"avg_entry":0.33,"first_trade_ts":_ts(60),"last_trade_ts":_ts(2), "num_buys":3,"num_sells":0},
    "0xSmart002": {"outcome":"Yes","outcome_index":0,"net_shares":62000,"usdc_invested":25420,"total_buy_shares":62000,"avg_entry":0.41,"first_trade_ts":_ts(45),"last_trade_ts":_ts(5), "num_buys":2,"num_sells":0},
    "0xPro003":   {"outcome":"No", "outcome_index":1,"net_shares":48000,"usdc_invested":30720,"total_buy_shares":48000,"avg_entry":0.64,"first_trade_ts":_ts(30),"last_trade_ts":_ts(1), "num_buys":4,"num_sells":1},
    "0xSavvy004": {"outcome":"Yes","outcome_index":0,"net_shares":38000,"usdc_invested":14820,"total_buy_shares":38000,"avg_entry":0.39,"first_trade_ts":_ts(55),"last_trade_ts":_ts(8), "num_buys":2,"num_sells":0},
    "0xSharp005": {"outcome":"No", "outcome_index":1,"net_shares":29500,"usdc_invested":17405,"total_buy_shares":29500,"avg_entry":0.59,"first_trade_ts":_ts(20),"last_trade_ts":_ts(3), "num_buys":2,"num_sells":0},
    "0xKeen006":  {"outcome":"Yes","outcome_index":0,"net_shares":22000,"usdc_invested":9680, "total_buy_shares":22000,"avg_entry":0.44,"first_trade_ts":_ts(40),"last_trade_ts":_ts(14),"num_buys":1,"num_sells":0},
    "0xAce007":   {"outcome":"Yes","outcome_index":0,"net_shares":18500,"usdc_invested":8695, "total_buy_shares":18500,"avg_entry":0.47,"first_trade_ts":_ts(25),"last_trade_ts":_ts(20),"num_buys":1,"num_sells":0},
    "0xEdge008":  {"outcome":"No", "outcome_index":1,"net_shares":15000,"usdc_invested":8250, "total_buy_shares":15000,"avg_entry":0.55,"first_trade_ts":_ts(15),"last_trade_ts":_ts(6), "num_buys":2,"num_sells":0},
    "0xClev009":  {"outcome":"Yes","outcome_index":0,"net_shares":12000,"usdc_invested":6000, "total_buy_shares":12000,"avg_entry":0.50,"first_trade_ts":_ts(35),"last_trade_ts":_ts(120),"num_buys":1,"num_sells":0},
    "0xBold010":  {"outcome":"No", "outcome_index":1,"net_shares":10200,"usdc_invested":6324, "total_buy_shares":10200,"avg_entry":0.62,"first_trade_ts":_ts(10),"last_trade_ts":_ts(10),"num_buys":1,"num_sells":0},
    "0xQuik011":  {"outcome":"Yes","outcome_index":0,"net_shares":8800, "usdc_invested":3344, "total_buy_shares":8800, "avg_entry":0.38,"first_trade_ts":_ts(50),"last_trade_ts":_ts(45),"num_buys":1,"num_sells":0},
    "0xEarl013":  {"outcome":"Yes","outcome_index":0,"net_shares":6900, "usdc_invested":2139, "total_buy_shares":6900, "avg_entry":0.31,"first_trade_ts":_ts(70),"last_trade_ts":_ts(7), "num_buys":2,"num_sells":0},
    "0xTiny015":  {"outcome":"Yes","outcome_index":0,"net_shares":5300, "usdc_invested":2756, "total_buy_shares":5300, "avg_entry":0.52,"first_trade_ts":_ts(18),"last_trade_ts":_ts(40),"num_buys":1,"num_sells":0},
}

MOCK_STATS = {
    # crypto expert, hot streak, big realized profit
    "0xWhale001": {"total_pnl":312000,"realized_pnl":290000,"win_rate":0.74,"closed_wins":355,"closed_total":480,"markets_traded":480,"total_volume":4100000,"last_trade_ts":_ts(2), "recent_pnl":42000,"historical_pnl":248000,"category_pnl":198000,"category_total":290000},
    # solid crypto trader, consistent
    "0xSmart002": {"total_pnl":88000, "realized_pnl":79000, "win_rate":0.69,"closed_wins":131,"closed_total":190,"markets_traded":190,"total_volume":1200000,"last_trade_ts":_ts(5), "recent_pnl":18000,"historical_pnl":61000, "category_pnl":55000,"category_total":79000},
    # politics specialist — less relevant for crypto market
    "0xPro003":   {"total_pnl":145000,"realized_pnl":130000,"win_rate":0.78,"closed_wins":242,"closed_total":310,"markets_traded":310,"total_volume":2300000,"last_trade_ts":_ts(1), "recent_pnl":8000, "historical_pnl":122000,"category_pnl":9100,"category_total":130000},
    # crypto specialist, smaller scale
    "0xSavvy004": {"total_pnl":52000, "realized_pnl":46000, "win_rate":0.65,"closed_wins":84, "closed_total":130,"markets_traded":130,"total_volume":780000, "last_trade_ts":_ts(8), "recent_pnl":14000,"historical_pnl":32000, "category_pnl":35000,"category_total":46000},
    # diversified, recent cold streak (recent_pnl negative)
    "0xSharp005": {"total_pnl":71000, "realized_pnl":63000, "win_rate":0.72,"closed_wins":151,"closed_total":210,"markets_traded":210,"total_volume":1050000,"last_trade_ts":_ts(3), "recent_pnl":-4000,"historical_pnl":67000, "category_pnl":12000,"category_total":63000},
    # decent crypto record
    "0xKeen006":  {"total_pnl":28000, "realized_pnl":24000, "win_rate":0.61,"closed_wins":52, "closed_total":85, "markets_traded":85, "total_volume":420000, "last_trade_ts":_ts(14),"recent_pnl":6000, "historical_pnl":18000, "category_pnl":16000,"category_total":24000},
    # sports bettor, weak crypto expertise
    "0xAce007":   {"total_pnl":19500, "realized_pnl":17000, "win_rate":0.58,"closed_wins":39, "closed_total":67, "markets_traded":67, "total_volume":310000, "last_trade_ts":_ts(20),"recent_pnl":2000, "historical_pnl":15000, "category_pnl":1700,"category_total":17000},
    # consistent, some crypto
    "0xEdge008":  {"total_pnl":33000, "realized_pnl":29000, "win_rate":0.63,"closed_wins":69, "closed_total":110,"markets_traded":110,"total_volume":560000, "last_trade_ts":_ts(6), "recent_pnl":7000, "historical_pnl":22000, "category_pnl":18000,"category_total":29000},
    # inactive 120 days — stale profits (last_trade_ts far back)
    "0xClev009":  {"total_pnl":11000, "realized_pnl":9500,  "win_rate":0.54,"closed_wins":24, "closed_total":44, "markets_traded":44, "total_volume":180000, "last_trade_ts":_ts(120),"recent_pnl":0,   "historical_pnl":9500,  "category_pnl":4000,"category_total":9500},
    # new, promising, hot streak
    "0xBold010":  {"total_pnl":22000, "realized_pnl":19000, "win_rate":0.60,"closed_wins":47, "closed_total":79, "markets_traded":79, "total_volume":350000, "last_trade_ts":_ts(10),"recent_pnl":9000, "historical_pnl":10000,"category_pnl":14000,"category_total":19000},
    # barely qualifies, thin record
    "0xQuik011":  {"total_pnl":7800,  "realized_pnl":6200,  "win_rate":0.52,"closed_wins":16, "closed_total":31, "markets_traded":31, "total_volume":120000, "last_trade_ts":_ts(45),"recent_pnl":1000, "historical_pnl":5200,  "category_pnl":2000,"category_total":6200},
    # early entrant, crypto focus
    "0xEarl013":  {"total_pnl":14000, "realized_pnl":12000, "win_rate":0.57,"closed_wins":31, "closed_total":55, "markets_traded":55, "total_volume":210000, "last_trade_ts":_ts(7), "recent_pnl":4000, "historical_pnl":8000,  "category_pnl":9000,"category_total":12000},
    # thin track record
    "0xTiny015":  {"total_pnl":6200,  "realized_pnl":5100,  "win_rate":0.53,"closed_wins":13, "closed_total":25, "markets_traded":25, "total_volume":95000,  "last_trade_ts":_ts(40),"recent_pnl":500,  "historical_pnl":4600,  "category_pnl":1000,"category_total":5100},
}