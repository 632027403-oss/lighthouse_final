from __future__ import annotations

"""
Lighthouse Crypto v3 — structure + risk dashboard (no predictive score).
Principles: real data only · no fabricated probabilities · clear invalidation · risk first.
"""

import asyncio
import csv
import io
import math
import re
import statistics
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

APP_VERSION = "3.0-structure-risk"
CRYPTO = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "LTCUSDT", "BCHUSDT",
    "DOTUSDT", "TRXUSDT", "TONUSDT",
]
BINANCE_SPOT = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
]
OKX = "https://www.okx.com"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
UA = "LighthouseCrypto/3.0"

state: dict[str, Any] = {
    "updated": 0,
    "assets": {},
    "rank": [],
    "sources": {},
    "macro": {},
    "scan_status": "启动中",
    "app_version": APP_VERSION,
    "note": "列表按结构是否破坏与波动排列，不是预测排名",
}
cache: dict[str, Any] = {}


def ts() -> int:
    return int(time.time() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def pct(a: float, b: float) -> float | None:
    return (a - b) / b * 100 if b else None


def safe_mean(xs: list[float]) -> float | None:
    return statistics.mean(xs) if xs else None


def max_drawdown(closes: list[float]) -> float:
    peak = -float("inf")
    worst = 0.0
    for x in closes:
        if x > peak:
            peak = x
        if peak > 0:
            worst = min(worst, (x - peak) / peak)
    return worst


def reliability_level(n: int) -> str:
    if n >= 50:
        return "高"
    if n >= 20:
        return "中"
    if n >= 10:
        return "低"
    return "不足"


async def get(c: httpx.AsyncClient, url: str, params=None, timeout=12):
    r = await c.get(url, params=params, timeout=timeout, follow_redirects=True)
    r.raise_for_status()
    return r


async def first_json(c, bases, path, params=None, timeout=12):
    last = None
    for base in bases:
        try:
            return (await get(c, base + path, params, timeout)).json(), base, None
        except Exception as e:
            last = str(e)
    return None, None, last


async def spot_price(c: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
    out: dict[str, Any] = {"sources": {}}
    prices = []
    # Binance
    try:
        data, base, err = await first_json(c, BINANCE_SPOT, "/api/v3/ticker/24hr", {"symbol": symbol})
        if data:
            p = float(data["lastPrice"])
            prices.append(p)
            out["sources"]["binance"] = {"available": True, "price": p, "change24h": float(data.get("priceChangePercent") or 0)}
        else:
            out["sources"]["binance"] = {"available": False, "error": err}
    except Exception as e:
        out["sources"]["binance"] = {"available": False, "error": str(e)}
    # OKX
    try:
        inst = symbol.replace("USDT", "-USDT")
        t = (await get(c, f"{OKX}/api/v5/market/ticker", {"instId": inst})).json()["data"][0]
        p = float(t["last"])
        o = float(t["open24h"])
        prices.append(p)
        out["sources"]["okx"] = {"available": True, "price": p, "change24h": pct(p, o)}
    except Exception as e:
        out["sources"]["okx"] = {"available": False, "error": str(e)}
    if not prices:
        out["available"] = False
        return out
    out["available"] = True
    out["price"] = statistics.median(prices)
    ch = [s.get("change24h") for s in out["sources"].values() if s.get("available") and s.get("change24h") is not None]
    out["change24h"] = safe_mean(ch)
    return out


async def daily_history(c: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
    key = "hist:" + symbol
    cached = cache.get(key)
    if cached and ts() - cached["ts"] < 6 * 3600 * 1000:
        return cached["data"]
    k, base, err = await first_json(
        c, BINANCE_SPOT, "/api/v3/klines",
        {"symbol": symbol, "interval": "1d", "limit": 1000},
    )
    source = base
    if not k:
        try:
            inst = symbol.replace("USDT", "-USDT")
            x = (await get(c, f"{OKX}/api/v5/market/candles", {"instId": inst, "bar": "1D", "limit": "300"})).json()
            data = list(reversed(x.get("data", [])))
            k = [[int(r[0]), 0, 0, 0, float(r[4]), float(r[5])] for r in data]
            source = "OKX"
        except Exception:
            data = {"closes": [], "source": None}
            cache[key] = {"ts": ts(), "data": data}
            return data
    rows = [{"ts": int(x[0]), "close": float(x[4])} for x in k if len(x) >= 5]
    day_start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    rows = [r for r in rows if r["ts"] < day_start]
    closes = [r["close"] for r in rows]
    data = {
        "closes": closes,
        "source": source,
        "last_date": iso(rows[-1]["ts"]) if rows else None,
        "n": len(closes),
    }
    cache[key] = {"ts": ts(), "data": data}
    return data


async def macro_background(c: httpx.AsyncClient) -> dict[str, Any]:
    cached = cache.get("macro")
    if cached and ts() - cached["ts"] < 15 * 60 * 1000:
        return cached["data"]

    async def fred_one(series: str):
        try:
            txt = (await get(c, FRED, {"id": series}, timeout=15)).text
            rows = [r for r in csv.DictReader(io.StringIO(txt)) if r.get(series) not in (None, "", ".")]
            if not rows:
                return None
            return {"value": float(rows[-1][series]), "date": rows[-1]["observation_date"]}
        except Exception:
            return None

    fed, dgs10, dxy = await asyncio.gather(
        fred_one("FEDFUNDS"), fred_one("DGS10"), fred_one("DTWEXBGS")
    )
    out = {
        "fed_funds": fed,
        "ust_10y": dgs10,
        "dollar_index": dxy,
        "label": "宏观背景（描述性，不合成买卖分）",
    }
    cache["macro"] = {"ts": ts(), "data": out}
    return out


def structure_state(closes: list[float], price: float) -> dict[str, Any]:
    if len(closes) < 120:
        return {"state": "数据不足", "invalidation": "历史日线不足，无法判断结构", "structure_broken": True,
                "rules_aligned": 0, "rules_total": 4}
    # Use live median price against daily MAs built from completed days
    sma60 = statistics.fmean(closes[-60:])
    sma120 = statistics.fmean(closes[-120:])
    r21 = pct(price, closes[-22]) if len(closes) >= 22 else None
    r63 = pct(price, closes[-64]) if len(closes) >= 64 else None
    above60 = price > sma60
    above120 = price > sma120
    r63_pos = (r63 or 0) > 0
    r21_pos = (r21 or 0) > 0

    if above60 and above120 and r63_pos:
        state = "中期结构偏多"
        invalidation = "若日线收盘跌破60日均线，本状态失效"
    elif (not above60) and (not above120) and not r63_pos:
        state = "中期结构偏空"
        invalidation = "若日线重新站上60日均线，偏空减弱"
    elif above60 and (r63 or 0) < -5:
        state = "仍在均线上方但动能转弱"
        invalidation = "跌破60日均线则确认结构破坏"
    elif (not above60) and (r63 or 0) > 5:
        state = "均线下方、动能回升（修复中）"
        invalidation = "需站上60日均线才确认结构修复"
    else:
        state = "震荡/过渡"
        invalidation = "等待价格与60/120日均线关系明确"

    rules_hit = sum([1 if above60 else 0, 1 if above120 else 0, 1 if r63_pos else 0, 1 if r21_pos else 0])
    return {
        "state": state,
        "invalidation": invalidation,
        "structure_broken": not above60,
        "rules_aligned": rules_hit,
        "rules_total": 4,
        "sma60": round(sma60, 4),
        "sma120": round(sma120, 4),
        "return_21d": round(r21, 2) if r21 is not None else None,
        "return_63d": round(r63, 2) if r63 is not None else None,
        "flags": {"above_sma60": above60, "above_sma120": above120, "r63_positive": r63_pos, "r21_positive": r21_pos},
    }


def risk_metrics(closes: list[float], price: float) -> dict[str, Any]:
    if len(closes) < 30:
        return {}
    series = closes + ([price] if closes[-1] != price else [])
    daily = [series[i] / series[i - 1] - 1 for i in range(1, len(series))]
    vol20 = statistics.pstdev(daily[-20:]) if len(daily) >= 20 else None
    mdd_90 = max_drawdown(series[-91:]) if len(series) >= 91 else max_drawdown(series)
    mdd_all = max_drawdown(series)
    return {
        "vol20_daily_pct": round(vol20 * 100, 2) if vol20 is not None else None,
        "max_drawdown_90d": round(mdd_90, 4),
        "max_drawdown_all": round(mdd_all, 4),
        "stress_prices": {
            "down_10pct": round(price * 0.90, 4),
            "down_20pct": round(price * 0.80, 4),
            "down_30pct": round(price * 0.70, 4),
        },
    }


def similar_drawdown_stats(closes: list[float], struct: dict[str, Any]) -> dict[str, Any]:
    if len(closes) < 200:
        return {"sample_count": 0, "reliability": "不足", "note": "历史不足，不展示条件统计"}
    above60 = struct.get("flags", {}).get("above_sma60", False)
    above120 = struct.get("flags", {}).get("above_sma120", False)
    r63_pos = struct.get("flags", {}).get("r63_positive", False)
    matches = []
    for i in range(120, len(closes) - 90):
        s60 = statistics.fmean(closes[i - 59 : i + 1])
        s120 = statistics.fmean(closes[i - 119 : i + 1])
        r63_h = closes[i] / closes[i - 63] - 1 if i >= 63 else 0.0
        if (closes[i] > s60) != above60:
            continue
        if (closes[i] > s120) != above120:
            continue
        if (r63_h > 0) != r63_pos:
            continue
        matches.append(i)
    records = []
    for i in matches:
        path = closes[i : i + 91]
        if len(path) < 91:
            continue
        records.append({"ret": path[-1] / path[0] - 1, "mdd": max_drawdown(path)})
    n = len(records)
    if n < 10:
        return {"sample_count": n, "reliability": reliability_level(n), "note": "样本不足，不展示比例", "horizon_90d": None}
    rets = [x["ret"] for x in records]
    mdds = [x["mdd"] for x in records]
    return {
        "sample_count": n,
        "reliability": reliability_level(n),
        "note": "历史相似结构下的90日路径统计；存在重叠；非未来保证。",
        "horizon_90d": {
            "up_rate": sum(v > 0 for v in rets) / n,
            "median_return": statistics.median(rets),
            "worst_return": min(rets),
            "median_path_mdd": statistics.median(mdds),
            "worst_path_mdd": min(mdds),
        },
    }


def build_asset(symbol: str, spot: dict, hist: dict) -> dict[str, Any]:
    if not spot.get("available"):
        return {"symbol": symbol, "status": "现货数据暂缺", "sources": spot.get("sources", {})}
    price = spot["price"]
    closes = hist.get("closes") or []
    struct = structure_state(closes, price)
    risk = risk_metrics(closes, price)
    similar = similar_drawdown_stats(closes, struct)
    broken = 1 if struct.get("structure_broken") else 0
    vol = risk.get("vol20_daily_pct") or 99
    return {
        "symbol": symbol,
        "status": "可分析",
        "price": price,
        "change24h": spot.get("change24h"),
        "sources": spot.get("sources", {}),
        "history_source": hist.get("source"),
        "history_n": hist.get("n"),
        "structure": struct,
        "risk": risk,
        "similar_history": similar,
        "scan_order_key": (broken, vol),
        "disclaimer": "结构与回撤描述仅供参考，不构成买卖建议。",
    }


async def full_scan():
    state["scan_status"] = "扫描中"
    async with httpx.AsyncClient(headers={"User-Agent": UA}) as c:
        try:
            macro = await macro_background(c)
        except Exception:
            macro = {}
        state["macro"] = macro

        async def one(sym: str):
            try:
                spot, hist = await asyncio.gather(spot_price(c, sym), daily_history(c, sym))
                return build_asset(sym, spot, hist)
            except Exception as e:
                return {"symbol": sym, "status": "扫描异常", "error": str(e)}

        assets = await asyncio.gather(*(one(s) for s in CRYPTO))
    good = [a for a in assets if a.get("status") == "可分析"]
    good.sort(key=lambda a: a.get("scan_order_key", (1, 99)))
    state["assets"] = {a["symbol"]: a for a in assets if "symbol" in a}
    state["rank"] = [a["symbol"] for a in good]
    state["sources"] = {
        "Binance": any(
            (state["assets"].get(s) or {}).get("sources", {}).get("binance", {}).get("available")
            for s in CRYPTO
        ),
        "OKX": any(
            (state["assets"].get(s) or {}).get("sources", {}).get("okx", {}).get("available")
            for s in CRYPTO
        ),
        "FRED": bool(macro.get("fed_funds") or macro.get("ust_10y")),
    }
    state["updated"] = ts()
    state["scan_status"] = "完成"
    state["app_version"] = APP_VERSION


async def loop():
    while True:
        try:
            await full_scan()
        except Exception as e:
            state["scan_status"] = f"异常: {e}"
        await asyncio.sleep(60)


app = FastAPI(title="Lighthouse Crypto", version=APP_VERSION)


@app.on_event("startup")
async def startup():
    asyncio.create_task(loop())


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "scan_status": state["scan_status"],
        "updated": state["updated"],
        "sources": state.get("sources"),
    }


@app.get("/api/state")
async def api_state():
    return JSONResponse(state)


@app.get("/api/search")
async def search(symbol: str = Query(..., min_length=1, max_length=30)):
    q = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    if not q.endswith("USDT"):
        q += "USDT"
    async with httpx.AsyncClient(headers={"User-Agent": UA}) as c:
        spot, hist = await asyncio.gather(spot_price(c, q), daily_history(c, q))
        asset = build_asset(q, spot, hist)
        try:
            asset["macro"] = await macro_background(c)
        except Exception:
            asset["macro"] = {}
    return asset


@app.get("/")
async def root():
    html = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>灯塔 Crypto v3</title>
<style>
body{font-family:system-ui,sans-serif;max-width:920px;margin:auto;padding:16px;background:#0f1419;color:#e7e9ea}
.card{background:#1a2332;border-radius:12px;padding:14px;margin:12px 0}
button,input{padding:10px;border-radius:8px;border:1px solid #333;background:#111;color:#eee}
button{background:#1d9bf0;border:none;cursor:pointer}
.muted{color:#8b98a5;font-size:13px}
.warn{color:#ffad1f;font-size:13px}
.tag{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;background:#1e3a5f;color:#8ecaff}
.tag.bad{background:#3f1d1d;color:#ff8f8f}
.disclaimer{background:#2a2110;border-left:4px solid #ffad1f;padding:10px;margin:12px 0;font-size:13px;color:#ffd78a}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px}
table{width:100%;font-size:13px;border-collapse:collapse}
td,th{padding:6px;border-bottom:1px solid #333;text-align:left}
</style></head><body>
<h1>🔦 灯塔 Crypto</h1>
<p class="muted">结构说明书 · 风险尺 · 不预测涨跌 · v3.0</p>
<div class="disclaimer">描述均线结构与历史回撤，<strong>不提供买卖建议</strong>。列表不是「最该买」排名。</div>
<div class="card"><input id="q" placeholder="BTC / ETH / SOL…"><button onclick="searchA()">搜索</button>
<p class="muted" id="status">—</p></div>
<div class="card"><b>数据源</b><div id="sources" class="muted"></div></div>
<div class="card"><h2>扫描快照</h2><div id="rank" class="grid"></div></div>
<div class="card"><h2>宏观背景</h2><div id="macro" class="muted"></div></div>
<div id="detail"></div>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function pct(x){return typeof x==='number'?(x*100).toFixed(1)+'%':'—'}
function row(k,v){return `<div>${esc(k)}：${esc(v)}</div>`}
function render(x){
  $('status').textContent='更新 '+(x.updated?new Date(x.updated).toLocaleString():'—')+' · '+esc(x.scan_status||'');
  $('sources').innerHTML=Object.entries(x.sources||{}).map(([k,v])=>row(k,v?'可用':'暂缺')).join('');
  const rank=x.rank||[];
  $('rank').innerHTML=rank.slice(0,6).map(s=>{
    const a=(x.assets||{})[s]||{}, st=a.structure||{}, rk=a.risk||{};
    const tag=st.structure_broken?'tag bad':'tag';
    return `<div class="card"><b>${esc(s.replace('USDT',''))}</b> $${Number(a.price).toLocaleString(undefined,{maximumFractionDigits:2})}
      <p><span class="${tag}">${esc(st.state)}</span> 对齐 ${st.rules_aligned}/${st.rules_total}</p>
      <p class="muted">21日 ${st.return_21d??'—'}% · 63日 ${st.return_63d??'—'}%</p>
      <p class="muted">90日回撤 ${pct(rk.max_drawdown_90d)}</p></div>`;
  }).join('')||'<p class="muted">扫描中…</p>';
  const m=x.macro||{};
  $('macro').innerHTML=[
    m.fed_funds?row('Fed Funds', m.fed_funds.value+'（'+m.fed_funds.date+'）'):'',
    m.ust_10y?row('美债10Y', m.ust_10y.value+'（'+m.ust_10y.date+'）'):'',
    m.dollar_index?row('美元指数', m.dollar_index.value):'',
    '<p class="muted">'+esc(m.label||'')+'</p>'
  ].join('')||'暂缺';
}
async function load(){try{render(await fetch('/api/state',{cache:'no-store'}).then(r=>r.json()))}catch(e){$('status').textContent='连接异常'}}
async function searchA(){
  const q=$('q').value.trim(); if(!q)return;
  const d=await fetch('/api/search?symbol='+encodeURIComponent(q)).then(r=>r.json());
  const st=d.structure||{}, rk=d.risk||{}, sh=d.similar_history||{};
  const tag=st.structure_broken?'tag bad':'tag';
  let sim=esc(sh.note||'');
  if(sh.horizon_90d){
    const h=sh.horizon_90d;
    sim+=`<table><tr><td>样本</td><td>${sh.sample_count}（${esc(sh.reliability)}）</td></tr>
      <tr><td>90日上涨占比</td><td>${pct(h.up_rate)}</td></tr>
      <tr><td>中位收益</td><td>${pct(h.median_return)}</td></tr>
      <tr><td>最差收益</td><td>${pct(h.worst_return)}</td></tr>
      <tr><td>路径中位回撤</td><td>${pct(h.median_path_mdd)}</td></tr>
      <tr><td>路径最差回撤</td><td>${pct(h.worst_path_mdd)}</td></tr></table>`;
  }
  $('detail').innerHTML=`<div class="card"><h2>${esc((d.symbol||q).replace('USDT',''))}</h2>
    <p class="big">$${Number(d.price).toLocaleString(undefined,{maximumFractionDigits:2})}</p>
    <p><span class="${tag}">${esc(st.state)}</span> 对齐 ${st.rules_aligned}/${st.rules_total}</p>
    <p class="warn">${esc(st.invalidation||'')}</p>
    <p>SMA60 ${st.sma60} · SMA120 ${st.sma120}</p>
    <p>21日 ${st.return_21d??'—'}% · 63日 ${st.return_63d??'—'}%</p>
    <h3>风险尺</h3>
    <p>90日回撤 ${pct(rk.max_drawdown_90d)} · 全历史 ${pct(rk.max_drawdown_all)}</p>
    <p>波动20 ${rk.vol20_daily_pct??'—'}%</p>
    <p class="muted">跌10/20/30%：${rk.stress_prices?.down_10pct} / ${rk.stress_prices?.down_20pct} / ${rk.stress_prices?.down_30pct}</p>
    <h3>相似结构历史</h3>${sim}
    <p class="muted">${esc(d.disclaimer||'')}</p></div>`;
}
load(); setInterval(load, 30000);
</script></body></html>"""
    return HTMLResponse(html)
