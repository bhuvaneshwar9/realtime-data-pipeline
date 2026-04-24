"""
Realtime Data Pipeline — Live Demo API
Fetches live cryptocurrency market data from CoinGecko (free, no API key)
and processes it through Bronze → Silver → Gold medallion architecture.
Auto-refreshes every 60 seconds.
"""
import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(__file__))

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timezone

app = FastAPI(
    title="Realtime Data Pipeline — CoinGecko Live",
    description="Bronze → Silver → Gold medallion pipeline on live crypto market data",
    version="2.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"
HDR       = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
CACHE_TTL = 300

_cache    = {"df": None, "fetched_at": None, "source": ""}
_last_run = {}


# ── Data ingestion ─────────────────────────────────────────────────────────────
def fetch_coins() -> tuple[pd.DataFrame, str]:
    now = datetime.now(timezone.utc)
    if _cache["df"] is not None:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["df"], _cache["source"]
    try:
        resp = requests.get(COINGECKO, headers=HDR, timeout=12, params={
            "vs_currency": "usd", "order": "market_cap_desc",
            "per_page": 100, "page": 1, "sparkline": "false",
            "price_change_percentage": "24h,7d",
        })
        resp.raise_for_status()
        coins = resp.json()

        rows = []
        for c in coins:
            mc = c.get("market_cap") or 0
            tier = ("Large Cap" if mc >= 10_000_000_000 else
                    "Mid Cap"   if mc >= 1_000_000_000  else
                    "Small Cap" if mc >= 100_000_000     else "Micro Cap")
            pc24 = c.get("price_change_percentage_24h") or 0.0
            rows.append({
                "id":              c["id"],
                "symbol":          c["symbol"].upper(),
                "name":            c["name"],
                "category":        tier,
                "price_usd":       c.get("current_price") or 0.0,
                "market_cap":      mc,
                "volume_24h":      c.get("total_volume") or 0.0,
                "price_change_24h": pc24,
                "price_change_7d": c.get("price_change_percentage_7d_in_currency") or 0.0,
                "high_24h":        c.get("high_24h") or 0.0,
                "low_24h":         c.get("low_24h") or 0.0,
                "event_type":      "bullish" if pc24 >= 0 else "bearish",
                "timestamp":       now,
            })
        df  = pd.DataFrame(rows)
        src = f"CoinGecko API — top {len(df)} coins by market cap"
        _cache.update(df=df, fetched_at=now, source=src)
        return df, src
    except Exception as e:
        df  = _fallback()
        src = f"Synthetic fallback (CoinGecko error: {e})"
        _cache.update(df=df, fetched_at=now, source=src)
        return df, src


def _fallback() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    now = datetime.now(timezone.utc)

    # Realistic coin data: (id, symbol, name, approx_price, market_cap, tier)
    coins = [
        ("bitcoin",       "BTC",  "Bitcoin",          94000, 1.85e12, "Large Cap"),
        ("ethereum",      "ETH",  "Ethereum",          3200, 3.85e11, "Large Cap"),
        ("tether",        "USDT", "Tether",               1, 1.44e11, "Large Cap"),
        ("binancecoin",   "BNB",  "BNB",                620, 8.90e10, "Large Cap"),
        ("solana",        "SOL",  "Solana",             175, 8.30e10, "Large Cap"),
        ("ripple",        "XRP",  "XRP",               0.52, 2.96e10, "Large Cap"),
        ("usd-coin",      "USDC", "USD Coin",             1, 4.40e10, "Large Cap"),
        ("steth",         "STETH","Lido Staked ETH",   3190, 3.60e10, "Large Cap"),
        ("dogecoin",      "DOGE", "Dogecoin",          0.17, 2.50e10, "Large Cap"),
        ("tron",          "TRX",  "TRON",             0.135, 1.17e10, "Large Cap"),
        ("cardano",       "ADA",  "Cardano",           0.45, 1.59e10, "Large Cap"),
        ("avalanche-2",   "AVAX", "Avalanche",          38,  1.57e10, "Large Cap"),
        ("chainlink",     "LINK", "Chainlink",          17,  1.05e10, "Large Cap"),
        ("polkadot",      "DOT",  "Polkadot",           8.5, 1.26e10, "Large Cap"),
        ("uniswap",       "UNI",  "Uniswap",             12, 7.20e9,  "Mid Cap"),
        ("litecoin",      "LTC",  "Litecoin",           90,  6.60e9,  "Mid Cap"),
        ("near",          "NEAR", "NEAR Protocol",     7.2,  7.70e9,  "Mid Cap"),
        ("polygon",       "MATIC","Polygon",           0.85, 7.90e9,  "Mid Cap"),
        ("internet-computer","ICP","Internet Computer",13.5, 6.30e9,  "Mid Cap"),
        ("stellar",       "XLM",  "Stellar",          0.125, 3.60e9,  "Mid Cap"),
        ("cosmos",        "ATOM", "Cosmos",              9,  3.50e9,  "Mid Cap"),
        ("monero",        "XMR",  "Monero",            165,  3.04e9,  "Mid Cap"),
        ("ethereum-classic","ETC","Ethereum Classic",   27,  3.93e9,  "Mid Cap"),
        ("filecoin",      "FIL",  "Filecoin",            6,  3.50e9,  "Mid Cap"),
        ("hedera",        "HBAR", "Hedera",            0.09, 3.40e9,  "Mid Cap"),
        ("vechain",       "VET",  "VeChain",           0.04, 3.30e9,  "Small Cap"),
        ("algorand",      "ALGO", "Algorand",          0.19, 1.56e9,  "Small Cap"),
        ("the-sandbox",   "SAND", "The Sandbox",       0.45, 1.04e9,  "Small Cap"),
        ("decentraland",  "MANA", "Decentraland",      0.48, 9.00e8,  "Small Cap"),
        ("chiliz",        "CHZ",  "Chiliz",            0.095,8.70e8,  "Small Cap"),
        ("enjincoin",     "ENJ",  "Enjin Coin",        0.28, 4.70e8,  "Small Cap"),
        ("1inch",         "1INCH","1inch",              0.4,  4.30e8,  "Small Cap"),
        ("basic-attention-token","BAT","Basic Attention",0.22,3.30e8, "Small Cap"),
        ("zcash",         "ZEC",  "Zcash",               33,  5.40e8,  "Small Cap"),
        ("dash",          "DASH", "Dash",                33,  3.90e8,  "Small Cap"),
        ("horizen",       "ZEN",  "Horizen",           11.5, 1.40e8,  "Micro Cap"),
        ("syscoin",       "SYS",  "Syscoin",           0.11, 9.00e7,  "Micro Cap"),
        ("wanchain",      "WAN",  "Wanchain",          0.22, 6.50e7,  "Micro Cap"),
        ("dent",          "DENT", "Dent",             0.001, 1.30e7,  "Micro Cap"),
        ("funtoken",      "FUN",  "FunToken",          0.01, 8.50e7,  "Micro Cap"),
    ]

    rows = []
    for i, (cid, sym, name, price, mc, tier) in enumerate(coins):
        pc24 = float(rng.normal(0, 5))
        pc7d = float(rng.normal(0, 10))
        vol  = mc * rng.uniform(0.03, 0.25)
        rows.append({
            "id":               cid,
            "symbol":           sym,
            "name":             name,
            "category":         tier,
            "price_usd":        round(price * (1 + rng.uniform(-0.03, 0.03)), 4 if price < 1 else 2),
            "market_cap":       mc,
            "volume_24h":       round(vol, 0),
            "price_change_24h": round(pc24, 2),
            "price_change_7d":  round(pc7d, 2),
            "high_24h":         round(price * 1.04, 4 if price < 1 else 2),
            "low_24h":          round(price * 0.96, 4 if price < 1 else 2),
            "event_type":       "bullish" if pc24 >= 0 else "bearish",
            "timestamp":        now,
        })
    return pd.DataFrame(rows)


# ── Medallion pipeline layers ──────────────────────────────────────────────────
def run_bronze(df: pd.DataFrame) -> dict:
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return {
        "rows": len(df),
        "columns": list(df.columns),
        "sample": df[["symbol","name","price_usd","price_change_24h","category"]].head(5).to_dict(orient="records"),
        "size_kb": round(df.memory_usage(deep=True).sum() / 1024, 1),
        "bullish": int((df["event_type"] == "bullish").sum()),
        "bearish": int((df["event_type"] == "bearish").sum()),
    }


def run_silver(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    before = len(df)
    df = df.dropna(subset=["id", "price_usd"]).copy()
    df = df[df["price_usd"] > 0]

    # Anomaly detection — coins with abnormal 24h price movement
    mean = df["price_change_24h"].mean()
    std  = df["price_change_24h"].std() + 1e-9
    z    = (df["price_change_24h"] - mean) / std
    df["z_score"]    = z.round(3)
    df["is_anomaly"] = z.abs() > 2.0   # 2σ threshold (crypto is volatile)

    # Volume spike detection
    vm, vs = df["volume_24h"].mean(), df["volume_24h"].std() + 1e-9
    df["volume_spike"] = ((df["volume_24h"] - vm) / vs) > 3.0

    anomalies = int(df["is_anomaly"].sum())
    return df, {
        "rows_in": before, "rows_out": len(df), "dropped": before - len(df),
        "price_anomalies":    anomalies,
        "volume_spikes":      int(df["volume_spike"].sum()),
        "anomaly_rate_pct":   round(anomalies / len(df) * 100, 2),
        "avg_change_24h":     round(float(mean), 2),
        "market_sentiment":   "Bullish 🟢" if mean > 0 else "Bearish 🔴",
    }


def run_gold(df: pd.DataFrame) -> dict:
    tier_order = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]
    by_tier = df.groupby("category").agg(
        total_market_cap_B=("market_cap",      lambda x: round(x.sum() / 1e9, 2)),
        total_volume_B    =("volume_24h",      lambda x: round(x.sum() / 1e9, 2)),
        coin_count        =("id",              "count"),
        avg_change_24h    =("price_change_24h","mean"),
        anomaly_count     =("is_anomaly",      "sum"),
    ).reset_index()
    by_tier["avg_change_24h"] = by_tier["avg_change_24h"].round(2)
    # Sort by defined tier order
    by_tier["_ord"] = by_tier["category"].map({t: i for i, t in enumerate(tier_order)})
    by_tier = by_tier.sort_values("_ord").drop(columns="_ord")

    top5_gain = df.nlargest(5,  "price_change_24h")[["symbol","name","price_usd","price_change_24h"]].to_dict(orient="records")
    top5_loss = df.nsmallest(5, "price_change_24h")[["symbol","name","price_usd","price_change_24h"]].to_dict(orient="records")

    return {
        "by_tier":          by_tier.to_dict(orient="records"),
        "top_gainers":      top5_gain,
        "top_losers":       top5_loss,
        "total_market_cap_B": round(float(df["market_cap"].sum()) / 1e9, 2),
        "total_volume_B":   round(float(df["volume_24h"].sum()) / 1e9, 2),
        "bullish_coins":    int((df["price_change_24h"] > 0).sum()),
        "bearish_coins":    int((df["price_change_24h"] <= 0).sum()),
        "total_coins":      len(df),
    }


# ── API routes ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "realtime-data-pipeline", "version": "2.0.0"}


@app.get("/run", summary="Run the full Bronze → Silver → Gold pipeline on live CoinGecko data")
def run_pipeline():
    t0     = datetime.now(timezone.utc)
    raw_df, source = fetch_coins()
    bronze         = run_bronze(raw_df.copy())
    silver_df, silver = run_silver(raw_df.copy())
    gold           = run_gold(silver_df)
    elapsed        = round((datetime.now(timezone.utc) - t0).total_seconds() * 1000, 1)

    result = {
        "run_at": t0.isoformat(), "elapsed_ms": elapsed, "source": source,
        "pipeline": {"bronze": bronze, "silver": silver, "gold": gold},
    }
    global _last_run
    _last_run = result
    return result


@app.get("/status", summary="Return the last pipeline run")
def status():
    return _last_run if _last_run else run_pipeline()


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    data   = run_pipeline()
    p      = data["pipeline"]
    b, s, g = p["bronze"], p["silver"], p["gold"]
    source = data.get("source", "")

    tier_rows = ""
    for row in g["by_tier"]:
        chg   = row["avg_change_24h"]
        color = "#00ff88" if chg >= 0 else "#ff4d6d"
        sign  = "+" if chg >= 0 else ""
        tier_rows += f"""<tr>
          <td style="color:#c8d8f0">{row['category']}</td>
          <td>${row['total_market_cap_B']:,.1f}B</td>
          <td>${row['total_volume_B']:,.2f}B</td>
          <td>{row['coin_count']}</td>
          <td style="color:{color};font-weight:700">{sign}{chg}%</td>
          <td style="color:#ffd60a">{int(row['anomaly_count'])}</td>
        </tr>"""

    gainer_rows = ""
    for c in g["top_gainers"]:
        p_str = f"${c['price_usd']:,.4f}" if c['price_usd'] < 1 else f"${c['price_usd']:,.2f}"
        gainer_rows += f"""<tr>
          <td style="color:#00d4ff;font-weight:700">{c['symbol']}</td><td style="color:#8ab0d0">{c['name']}</td>
          <td>{p_str}</td>
          <td style="color:#00ff88;font-weight:700">+{c['price_change_24h']:.2f}%</td>
        </tr>"""

    loser_rows = ""
    for c in g["top_losers"]:
        p_str = f"${c['price_usd']:,.4f}" if c['price_usd'] < 1 else f"${c['price_usd']:,.2f}"
        loser_rows += f"""<tr>
          <td style="color:#00d4ff;font-weight:700">{c['symbol']}</td><td style="color:#8ab0d0">{c['name']}</td>
          <td>{p_str}</td>
          <td style="color:#ff4d6d;font-weight:700">{c['price_change_24h']:.2f}%</td>
        </tr>"""

    sentiment_color = "#34d399" if "Bullish" in s["market_sentiment"] else "#ef4444"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Realtime Data Pipeline</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700&display=swap');
    *{{box-sizing:border-box;margin:0;padding:0}}
    :root{{--bg:#060d1a;--bg2:#0b1628;--bg3:#0f1f35;--green:#00ff88;--cyan:#00d4ff;--red:#ff4d6d;--yellow:#ffd60a;--text:#c8d8f0;--muted:#4a6080;--border:#1a3050}}
    body{{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden}}
    /* scanline overlay */
    body::before{{content:'';position:fixed;top:0;left:0;width:100%;height:100%;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,212,255,.015) 2px,rgba(0,212,255,.015) 4px);pointer-events:none;z-index:0}}
    .wrap{{position:relative;z-index:1;padding:1.5rem 2rem;max-width:1400px;margin:0 auto}}
    /* top bar */
    .topbar{{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);padding-bottom:1rem;margin-bottom:1.5rem;flex-wrap:wrap;gap:.8rem}}
    .logo{{font-family:'JetBrains Mono',monospace;font-size:1.1rem;color:var(--green);text-shadow:0 0 12px rgba(0,255,136,.5)}}
    .logo span{{color:var(--cyan)}}
    .live-pill{{display:flex;align-items:center;gap:.5rem;background:rgba(0,255,136,.08);border:1px solid rgba(0,255,136,.3);border-radius:20px;padding:.3rem .9rem;font-size:.75rem;font-family:'JetBrains Mono',monospace;color:var(--green)}}
    .pulse{{width:7px;height:7px;background:var(--green);border-radius:50%;box-shadow:0 0 6px var(--green);animation:blink 1.2s infinite}}
    @keyframes blink{{0%,100%{{opacity:1;box-shadow:0 0 6px var(--green)}}50%{{opacity:.3;box-shadow:none}}}}
    .sentiment{{font-family:'JetBrains Mono',monospace;font-size:.85rem;font-weight:700}}
    /* pipeline flow */
    .flow{{display:flex;align-items:stretch;gap:0;margin-bottom:2rem;border:1px solid var(--border);border-radius:12px;overflow:hidden}}
    .layer{{flex:1;padding:1.2rem 1.4rem;position:relative}}
    .layer+.layer{{border-left:1px solid var(--border)}}
    .l-bronze{{background:linear-gradient(135deg,rgba(255,160,0,.08),rgba(255,160,0,.03))}}
    .l-silver{{background:linear-gradient(135deg,rgba(0,212,255,.08),rgba(0,212,255,.03))}}
    .l-gold{{background:linear-gradient(135deg,rgba(0,255,136,.08),rgba(0,255,136,.03))}}
    .l-tag{{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:3px;text-transform:uppercase;margin-bottom:.6rem}}
    .l-bronze .l-tag{{color:#ffa000}}.l-silver .l-tag{{color:var(--cyan)}}.l-gold .l-tag{{color:var(--green)}}
    .l-val{{font-size:2rem;font-weight:700;line-height:1;margin-bottom:.4rem}}
    .l-bronze .l-val{{color:#ffa000}}.l-silver .l-val{{color:var(--cyan)}}.l-gold .l-val{{color:var(--green)}}
    .l-desc{{font-size:.73rem;color:var(--muted);line-height:1.5}}
    .l-arrow{{display:flex;align-items:center;justify-content:center;padding:0 .4rem;color:var(--border);font-size:1.2rem;background:var(--bg2);border-left:1px solid var(--border);border-right:1px solid var(--border)}}
    /* metric grid */
    .metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:.8rem;margin-bottom:2rem}}
    .metric{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:1rem 1.2rem;transition:.2s}}
    .metric:hover{{border-color:var(--cyan);box-shadow:0 0 12px rgba(0,212,255,.1)}}
    .m-val{{font-size:1.6rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}}
    .m-label{{font-size:.7rem;color:var(--muted);margin-top:.4rem;text-transform:uppercase;letter-spacing:.5px}}
    /* section header */
    .sec{{display:flex;align-items:center;gap:.7rem;margin:1.6rem 0 .8rem}}
    .sec-line{{flex:1;height:1px;background:var(--border)}}
    .sec-title{{font-family:'JetBrains Mono',monospace;font-size:.7rem;letter-spacing:2px;text-transform:uppercase;color:var(--cyan);white-space:nowrap}}
    /* tables */
    table{{width:100%;border-collapse:collapse;background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:1rem}}
    thead tr{{background:var(--bg3)}}
    th{{padding:.65rem 1rem;text-align:left;font-size:.68rem;letter-spacing:1.5px;text-transform:uppercase;color:var(--cyan);font-family:'JetBrains Mono',monospace;font-weight:400;border-bottom:1px solid var(--border)}}
    td{{padding:.6rem 1rem;border-bottom:1px solid rgba(26,48,80,.6);font-size:.82rem;font-family:'JetBrains Mono',monospace}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:rgba(0,212,255,.03)}}
    .two{{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem}}
    @media(max-width:700px){{.two{{grid-template-columns:1fr}}.flow{{flex-direction:column}}.l-arrow{{display:none}}}}
    /* buttons */
    .btns{{display:flex;gap:.7rem;flex-wrap:wrap;margin-top:1.8rem;padding-top:1.2rem;border-top:1px solid var(--border)}}
    .btn{{display:inline-flex;align-items:center;gap:.4rem;padding:.5rem 1.2rem;border-radius:6px;text-decoration:none;font-weight:600;font-size:.8rem;font-family:'JetBrains Mono',monospace;transition:.2s;cursor:pointer;border:none}}
    .btn-primary{{background:var(--green);color:#000}}.btn-primary:hover{{background:#00e07a;box-shadow:0 0 16px rgba(0,255,136,.4)}}
    .btn-ghost{{background:transparent;color:var(--cyan);border:1px solid rgba(0,212,255,.35)}}.btn-ghost:hover{{background:rgba(0,212,255,.08)}}
    .btn-dim{{background:transparent;color:var(--muted);border:1px solid var(--border)}}.btn-dim:hover{{color:var(--text);border-color:var(--muted)}}
    .cd-bar{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:.5rem 1rem;font-size:.75rem;font-family:'JetBrains Mono',monospace;color:var(--muted);display:flex;justify-content:space-between;align-items:center;margin-bottom:1.5rem}}
    #cd{{color:var(--cyan)}}
  </style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="logo">PIPELINE<span>.io</span> <span style="color:var(--muted);font-size:.8rem">/ crypto market feed</span></div>
    <div style="display:flex;gap:1rem;align-items:center;flex-wrap:wrap">
      <span style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--muted)">{data['run_at'][:19]}Z · {data['elapsed_ms']}ms</span>
      <div class="live-pill"><span class="pulse"></span>LIVE</div>
      <span class="sentiment" style="color:{sentiment_color}">{s['market_sentiment']}</span>
    </div>
  </div>

  <div class="cd-bar">
    <span>FEED SOURCE: <span style="color:var(--text)">{source}</span></span>
    <span>REFRESH IN <span id="cd">60</span>s</span>
  </div>

  <!-- Pipeline layers -->
  <div class="flow">
    <div class="layer l-bronze">
      <div class="l-tag">▶ Bronze  /  ingest</div>
      <div class="l-val">{b['rows']}</div>
      <div class="l-desc">raw records · {b['size_kb']} KB<br/>{b['bullish']} bullish · {b['bearish']} bearish</div>
    </div>
    <div class="l-arrow">→</div>
    <div class="layer l-silver">
      <div class="l-tag">▶ Silver  /  clean + detect</div>
      <div class="l-val">{s['rows_out']}</div>
      <div class="l-desc">{s['dropped']} dropped · {s['price_anomalies']} anomalies<br/>{s['volume_spikes']} vol spikes · {s['anomaly_rate_pct']}% flagged</div>
    </div>
    <div class="l-arrow">→</div>
    <div class="layer l-gold">
      <div class="l-tag">▶ Gold  /  aggregate</div>
      <div class="l-val">${g['total_market_cap_B']:,.0f}B</div>
      <div class="l-desc">total market cap<br/>${g['total_volume_B']:,.1f}B volume · {g['total_coins']} coins</div>
    </div>
  </div>

  <!-- Metrics -->
  <div class="metrics">
    <div class="metric"><div class="m-val" style="color:var(--green)">{g['bullish_coins']}</div><div class="m-label">Bullish</div></div>
    <div class="metric"><div class="m-val" style="color:var(--red)">{g['bearish_coins']}</div><div class="m-label">Bearish</div></div>
    <div class="metric"><div class="m-val" style="color:var(--yellow)">{s['price_anomalies']}</div><div class="m-label">Price Anomalies</div></div>
    <div class="metric"><div class="m-val" style="color:var(--cyan)">{s['volume_spikes']}</div><div class="m-label">Vol Spikes</div></div>
    <div class="metric"><div class="m-val" style="color:{sentiment_color}">{s['avg_change_24h']:+.2f}%</div><div class="m-label">Avg 24h Δ</div></div>
    <div class="metric"><div class="m-val" style="color:var(--text)">${g['total_volume_B']:,.1f}B</div><div class="m-label">24h Volume</div></div>
  </div>

  <div class="sec"><span class="sec-title">market cap tier breakdown</span><span class="sec-line"></span></div>
  <table>
    <thead><tr><th>Tier</th><th>Market Cap</th><th>24h Volume</th><th>Coins</th><th>Avg 24h Δ</th><th>Anomalies</th></tr></thead>
    <tbody>{tier_rows}</tbody>
  </table>

  <div class="two">
    <div>
      <div class="sec"><span class="sec-title">top 5 gainers · 24h</span><span class="sec-line"></span></div>
      <table>
        <thead><tr><th>Sym</th><th>Name</th><th>Price</th><th>Δ 24h</th></tr></thead>
        <tbody>{gainer_rows}</tbody>
      </table>
    </div>
    <div>
      <div class="sec"><span class="sec-title">top 5 losers · 24h</span><span class="sec-line"></span></div>
      <table>
        <thead><tr><th>Sym</th><th>Name</th><th>Price</th><th>Δ 24h</th></tr></thead>
        <tbody>{loser_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="btns">
    <button class="btn btn-primary" onclick="location.reload()">⟳ Re-run Pipeline</button>
    <a class="btn btn-ghost" href="/docs">API Docs</a>
    <a class="btn btn-dim" href="https://www.coingecko.com" target="_blank">↗ CoinGecko</a>
  </div>
</div>
<script>
  let t=60;const el=document.getElementById("cd");
  setInterval(()=>{{t--;el.textContent=t;if(t<=0)location.reload();}},1000);
</script>
</body>
</html>"""
