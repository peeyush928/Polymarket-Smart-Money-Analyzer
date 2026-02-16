"""
app.py — Polymarket Smart Money Analyzer
Streamlit web interface
"""

import streamlit as st
import time
from analyzer import run_analysis

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Smart Money | Polymarket",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0a0a0f;
    color: #c8ccd4;
}

/* Hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 2rem; max-width: 1200px; }

/* Header */
.header-wrap {
    border-bottom: 1px solid #1e2030;
    padding-bottom: 1.2rem;
    margin-bottom: 2rem;
}
.header-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.6rem;
    font-weight: 600;
    color: #e2e8f0;
    letter-spacing: -0.03em;
}
.header-sub {
    font-size: 0.8rem;
    color: #4a5568;
    margin-top: 0.2rem;
    font-family: 'IBM Plex Mono', monospace;
}

/* Signal cards */
.signal-card {
    border-radius: 6px;
    padding: 1.6rem 2rem;
    margin: 1rem 0;
    text-align: center;
    border: 1px solid;
}
.signal-buy-yes {
    background: linear-gradient(135deg, #0d2318 0%, #0a1a12 100%);
    border-color: #22c55e;
    box-shadow: 0 0 30px rgba(34,197,94,0.08);
}
.signal-buy-no {
    background: linear-gradient(135deg, #2d0e0e 0%, #1a0a0a 100%);
    border-color: #ef4444;
    box-shadow: 0 0 30px rgba(239,68,68,0.08);
}
.signal-wait {
    background: linear-gradient(135deg, #1e1a0a 0%, #141000 100%);
    border-color: #f59e0b;
    box-shadow: 0 0 30px rgba(245,158,11,0.08);
}
.signal-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 2rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    margin: 0;
}
.signal-yes  { color: #22c55e; }
.signal-no   { color: #ef4444; }
.signal-wait { color: #f59e0b; }
.signal-conf {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.9rem;
    color: #94a3b8;
    margin-top: 0.4rem;
}

/* Metric boxes */
.metric-row {
    display: flex;
    gap: 1rem;
    margin: 1rem 0;
}
.metric-box {
    flex: 1;
    background: #0f1117;
    border: 1px solid #1e2030;
    border-radius: 6px;
    padding: 1rem 1.2rem;
}
.metric-label {
    font-size: 0.7rem;
    color: #4a5568;
    font-family: 'IBM Plex Mono', monospace;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.3rem;
    font-weight: 600;
    color: #e2e8f0;
    margin-top: 0.2rem;
}

/* Wallet table */
.wallet-row {
    background: #0f1117;
    border: 1px solid #1a1d2e;
    border-radius: 6px;
    padding: 0.9rem 1.2rem;
    margin-bottom: 0.5rem;
    display: grid;
    grid-template-columns: 1.5rem 7rem 4rem 5.5rem 5.5rem 5rem 4.5rem 4.5rem 4.5rem 4.5rem;
    gap: 0.5rem;
    align-items: center;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
}
.wallet-header {
    background: transparent;
    border-color: transparent;
    color: #4a5568;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    padding-bottom: 0;
}
.wallet-rank { color: #4a5568; }
.wallet-addr { color: #7c86a0; }
.tag-yes  { color: #22c55e; font-weight: 600; }
.tag-no   { color: #ef4444; font-weight: 600; }
.val-pos  { color: #22c55e; }
.val-neg  { color: #ef4444; }
.val-neu  { color: #94a3b8; }

/* Score bar */
.score-bar-wrap { display: flex; align-items: center; gap: 0.4rem; }
.score-bar-bg {
    flex: 1; height: 4px; background: #1a1d2e; border-radius: 2px; overflow: hidden;
}
.score-bar-fill { height: 100%; background: #3b82f6; border-radius: 2px; }
.score-val { font-size: 0.75rem; color: #e2e8f0; min-width: 2.5rem; }

/* Reasoning box */
.reasoning-box {
    background: #0f1117;
    border: 1px solid #1e2030;
    border-left: 3px solid #3b82f6;
    border-radius: 0 6px 6px 0;
    padding: 1rem 1.2rem;
    font-size: 0.85rem;
    color: #94a3b8;
    line-height: 1.6;
    margin: 1rem 0;
}

/* Sidebar styling */
section[data-testid="stSidebar"] {
    background: #080810 !important;
    border-right: 1px solid #1e2030;
}
section[data-testid="stSidebar"] * { color: #94a3b8 !important; }

/* Input overrides */
div[data-testid="stTextInput"] input {
    background: #0f1117 !important;
    border: 1px solid #2d3148 !important;
    color: #e2e8f0 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.85rem !important;
    border-radius: 6px !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 2px rgba(59,130,246,0.15) !important;
}

/* Button */
div[data-testid="stButton"] button {
    background: #1d4ed8 !important;
    color: #fff !important;
    border: none !important;
    border-radius: 6px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 600 !important;
    letter-spacing: 0.03em !important;
    padding: 0.5rem 1.5rem !important;
    transition: background 0.15s !important;
}
div[data-testid="stButton"] button:hover {
    background: #2563eb !important;
}

/* Divider */
hr { border-color: #1e2030 !important; }

/* Progress */
div[data-testid="stProgress"] > div { background: #1e2030 !important; }
div[data-testid="stProgress"] > div > div { background: #3b82f6 !important; }

</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_usd(v: float) -> str:
    if abs(v) >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:.0f}"

def fmt_addr(addr: str) -> str:
    if len(addr) >= 12:
        return addr[:6] + "…" + addr[-4:]
    return addr

def score_bar(val: float, color: str = "#3b82f6") -> str:
    pct = int(val * 100)
    return f"""
    <div class="score-bar-wrap">
      <div class="score-bar-bg"><div class="score-bar-fill" style="width:{pct}%;background:{color}"></div></div>
      <span class="score-val">{val:.2f}</span>
    </div>"""


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### ◈ Smart Money")
    st.markdown("---")
    st.markdown("**Settings**")
    min_profit = st.number_input(
        "Min wallet profit ($)",
        min_value=1_000, max_value=100_000, value=5_000, step=1_000,
        help="Exclude wallets with less than this total lifetime P&L"
    )
    use_mock = st.checkbox(
        "Use mock data (offline test)",
        value=False,
    )
    st.markdown("---")
    st.markdown("**Methodology**")
    st.markdown("""
<div style="font-size:0.78rem; line-height:1.7; color:#4a5568;">
<b style="color:#6b7280">30%</b> Lifetime profit (√ scaled)<br>
<b style="color:#6b7280">25%</b> Win rate (Bayesian adj.)<br>
<b style="color:#6b7280">20%</b> Position conviction<br>
<b style="color:#6b7280">15%</b> Recency (180d decay)<br>
<b style="color:#6b7280">10%</b> Entry timing
</div>
""", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("""
<div style="font-size:0.72rem; color:#2d3148;">
Signal = weighted vote of $5k+ wallets.<br>
Vote weight = score × conviction × √position.<br>
≥70% → STRONG · 60-70% → MODERATE<br>
55-60% → WEAK · &lt;55% → NO SIGNAL
</div>
""", unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="header-wrap">
  <div class="header-title">◈ Polymarket Smart Money Analyzer</div>
  <div class="header-sub">surfaces informed holder behavior · generates directional signal</div>
</div>
""", unsafe_allow_html=True)


# ── Input ─────────────────────────────────────────────────────────────────────

col_url, col_btn = st.columns([5, 1])
with col_url:
    market_url = st.text_input(
        "Market URL",
        placeholder="https://polymarket.com/event/will-there-be-a-us-recession-in-2025",
        label_visibility="collapsed",
    )
with col_btn:
    run = st.button("Analyze →", use_container_width=True)

with st.expander("How to use"):
    st.markdown("""
1. Go to **polymarket.com** and open any active market
2. Copy the URL from your browser address bar
3. Paste it above and click **Analyze →**

Or tick **Use mock data** in the sidebar to test without a live URL.

Example URLs:
- `https://polymarket.com/event/will-there-be-a-us-recession-in-2025`
- `https://polymarket.com/event/another-us-government-shutdown-by-february-14`
- `https://polymarket.com/event/fed-decision-in-march-885`
    """)


# ── Analysis ──────────────────────────────────────────────────────────────────

if run:
    if not market_url and not use_mock:
        st.error("Please paste a Polymarket URL above")
        st.stop()

    url = market_url if not use_mock else "https://polymarket.com/mock/test"

    progress_bar   = st.progress(0)
    status_slot    = st.empty()

    def on_progress(msg: str, pct: int):
        progress_bar.progress(pct)
        status_slot.markdown(
            f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.78rem;color:#4a5568">'
            f'{msg}</div>', unsafe_allow_html=True
        )

    result = run_analysis(
        market_url=url,
        min_profit=min_profit,
        use_mock=use_mock,
        progress=on_progress,
    )

    progress_bar.empty()
    status_slot.empty()

    if not result.get("success"):
        st.error(f"❌  {result.get('error', 'Unknown error')}")
        st.stop()

    market   = result["market"]
    signal   = result["signal"]
    profiles = result["profiles"]
    stats    = result["stats"]

    # ── Market header ─────────────────────────────────────────────────────────
    st.markdown(f"""
<div style="margin-bottom:1.5rem">
  <div style="font-size:1.1rem;font-weight:600;color:#e2e8f0;margin-bottom:0.5rem">
    {market['question']}
  </div>
  <div style="font-size:0.75rem;font-family:'IBM Plex Mono',monospace;color:#4a5568">
    ends {market['end_date']} &nbsp;·&nbsp;
    volume {fmt_usd(market['volume'])} &nbsp;·&nbsp;
    liquidity {fmt_usd(market['liquidity'])} &nbsp;·&nbsp;
    YES {market['yes_price']:.0%} &nbsp;/&nbsp; NO {market['no_price']:.0%}
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Metrics row ───────────────────────────────────────────────────────────
    st.markdown(f"""
<div class="metric-row">
  <div class="metric-box">
    <div class="metric-label">Holders found</div>
    <div class="metric-value">{stats['total_holders']}</div>
  </div>
  <div class="metric-box">
    <div class="metric-label">Wallets checked</div>
    <div class="metric-value">{stats['wallets_checked']}</div>
  </div>
  <div class="metric-box">
    <div class="metric-label">Qualified (${min_profit//1000}k+)</div>
    <div class="metric-value">{stats['qualified']}</div>
  </div>
  <div class="metric-box">
    <div class="metric-label">YES holders</div>
    <div class="metric-value" style="color:#22c55e">{signal['yes_count']}</div>
  </div>
  <div class="metric-box">
    <div class="metric-label">NO holders</div>
    <div class="metric-value" style="color:#ef4444">{signal['no_count']}</div>
  </div>
  <div class="metric-box">
    <div class="metric-label">Analysis time</div>
    <div class="metric-value">{stats['elapsed_s']}s</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Signal card ───────────────────────────────────────────────────────────
    sig = signal["signal"]
    if sig == "BUY_YES":
        card_cls  = "signal-buy-yes"
        txt_cls   = "signal-yes"
        label     = "◆ BUY YES"
    elif sig == "BUY_NO":
        card_cls  = "signal-buy-no"
        txt_cls   = "signal-no"
        label     = "◆ BUY NO"
    else:
        card_cls  = "signal-wait"
        txt_cls   = "signal-wait"
        label     = "◇ NO CLEAR SIGNAL"

    conf_bar_pct = int(signal["confidence"] * 10)
    conf_color   = "#22c55e" if sig == "BUY_YES" else ("#ef4444" if sig == "BUY_NO" else "#f59e0b")

    st.markdown(f"""
<div class="signal-card {card_cls}">
  <div class="signal-label {txt_cls}">{label}</div>
  <div class="signal-conf">
    confidence {signal['confidence']}/10 &nbsp;·&nbsp;
    {signal['strength']} &nbsp;·&nbsp;
    {signal['yes_pct']:.0f}% YES&nbsp;vs&nbsp;{signal['no_pct']:.0f}% NO weighted vote
  </div>
  <div style="margin-top:0.8rem;display:flex;justify-content:center">
    <div style="width:200px;height:4px;background:#1e2030;border-radius:2px;overflow:hidden">
      <div style="width:{conf_bar_pct}%;height:100%;background:{conf_color};border-radius:2px"></div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    # Reasoning
    st.markdown(f'<div class="reasoning-box">{signal["reasoning"]}</div>', unsafe_allow_html=True)

    if signal.get("whale_warning"):
        st.markdown(f"""
<div style="background:#1a0f00;border:1px solid #92400e;border-radius:6px;padding:0.7rem 1rem;
     font-size:0.8rem;color:#fbbf24;font-family:'IBM Plex Mono',monospace;margin-bottom:1rem">
  ⚠ {signal['whale_warning']}
</div>
""", unsafe_allow_html=True)

    # ── Wallet table ──────────────────────────────────────────────────────────
    st.markdown("""
<div style="font-size:0.7rem;font-family:'IBM Plex Mono',monospace;color:#4a5568;
     text-transform:uppercase;letter-spacing:0.08em;margin:1.5rem 0 0.5rem">
  Top Qualified Wallets
</div>
""", unsafe_allow_html=True)

    # Table header
    st.markdown("""
<div class="wallet-row wallet-header">
  <span>#</span>
  <span>Wallet</span>
  <span>Side</span>
  <span>Score</span>
  <span>USD Invested</span>
  <span>Avg Entry</span>
  <span>Total P&L</span>
  <span>Win Rate</span>
  <span>Markets</span>
  <span>Buys/Sells</span>
</div>
""", unsafe_allow_html=True)

    for i, p in enumerate(profiles[:20], 1):
        side_cls   = "tag-yes" if p["outcome"].lower() == "yes" else "tag-no"
        side_label = "YES" if p["outcome"].lower() == "yes" else "NO"
        pnl_cls    = "val-pos" if p["total_pnl"] >= 0 else "val-neg"
        entry_note = ""
        if p["avg_entry"] > 0:
            if p["outcome"].lower() == "yes" and p["avg_entry"] < market["yes_price"] * 0.85:
                entry_note = " ▲"
            elif p["outcome"].lower() == "no" and p["avg_entry"] < market["no_price"] * 0.85:
                entry_note = " ▲"

        st.markdown(f"""
<div class="wallet-row">
  <span class="wallet-rank">{i}</span>
  <span class="wallet-addr">{fmt_addr(p['address'])}</span>
  <span class="{side_cls}">{side_label}</span>
  <span>{score_bar(p['composite'])}</span>
  <span class="val-neu">{fmt_usd(p['usdc_invested'])}</span>
  <span class="val-neu">{p['avg_entry']:.3f}{entry_note}</span>
  <span class="{pnl_cls}">{fmt_usd(p['total_pnl'])}</span>
  <span class="val-neu">{p['win_rate']:.0%}</span>
  <span class="val-neu">{p['markets_traded']}</span>
  <span class="val-neu">{p['num_buys']}/{p['num_sells']}</span>
</div>
""", unsafe_allow_html=True)

    # ── Score breakdown expander ──────────────────────────────────────────────
    with st.expander("Score breakdown for top wallets"):
        header_cols = st.columns([2, 1.5, 1, 1, 1, 1, 1])
        header_cols[0].markdown("**Wallet**")
        header_cols[1].markdown("**Composite**")
        header_cols[2].markdown("**Profit**")
        header_cols[3].markdown("**Win Rate**")
        header_cols[4].markdown("**Conviction**")
        header_cols[5].markdown("**Recency**")
        header_cols[6].markdown("**Timing**")

        for p in profiles[:10]:
            cols = st.columns([2, 1.5, 1, 1, 1, 1, 1])
            cols[0].code(fmt_addr(p["address"]))
            cols[1].markdown(f"**{p['composite']:.3f}**")
            cols[2].markdown(f"{p['s_profit']:.2f}")
            cols[3].markdown(f"{p['s_win_rate']:.2f}")
            cols[4].markdown(f"{p['s_conviction']:.2f}")
            cols[5].markdown(f"{p['s_recency']:.2f}")
            cols[6].markdown(f"{p['s_timing']:.2f}")

    # ── Raw JSON ──────────────────────────────────────────────────────────────
    with st.expander("Raw data (JSON)"):
        st.json({
            "signal":   signal,
            "market":   market,
            "stats":    stats,
            "profiles": profiles[:20],
        })

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:3rem;padding-top:1rem;border-top:1px solid #1e2030;
     text-align:center;font-size:0.7rem;color:#2d3148;font-family:'IBM Plex Mono',monospace">
  Not financial advice. Data from Polymarket public APIs.
</div>
""", unsafe_allow_html=True)