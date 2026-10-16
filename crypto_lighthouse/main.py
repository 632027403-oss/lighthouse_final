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
    "note": "列表按是否偏弱、晃动大小排列，不是推荐买卖",
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
        return {"state": "数据不够", "invalidation": "历史价格天数太少，先不判断", "structure_broken": True,
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
        state = "价位偏稳"
        invalidation = "如果日线收盘掉到近3个月平均价下面，就不再算偏稳"
    elif (not above60) and (not above120) and not r63_pos:
        state = "价位偏弱"
        invalidation = "如果日线重新回到近3个月平均价上面，偏弱会减轻"
    elif above60 and (r63 or 0) < -5:
        state = "还在平均价上方，但近况变弱"
        invalidation = "若掉到近3个月平均价下面，就算明显走弱"
    elif (not above60) and (r63 or 0) > 5:
        state = "还在平均价下方，但近况有回升"
        invalidation = "要重新站上近3个月平均价，才算真正好转"
    else:
        state = "上下拉锯，方向不清"
        invalidation = "等价格和近几个月平均价的关系更清楚再说"

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
        return {"sample_count": 0, "reliability": "不足", "note": "历史太少，这里不强凑数字"}
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
        return {"sample_count": n, "reliability": reliability_level(n), "note": "类似情况以前不多，不展示比例", "horizon_90d": None}
    rets = [x["ret"] for x in records]
    mdds = [x["mdd"] for x in records]
    return {
        "sample_count": n,
        "reliability": reliability_level(n),
        "note": "以前出现类似情况时大约3个月里发生过什么。不能当以后一定会怎样。",
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
        "disclaimer": "只帮助看清现在贵不贵、曾经跌过多少。不是买卖建议。",
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
<title>灯塔 Crypto</title>
<style>
body{font-family:system-ui,sans-serif;max-width:920px;margin:auto;padding:16px;background:#0f1419;color:#e7e9ea;line-height:1.55}
.card{background:#1a2332;border-radius:12px;padding:14px;margin:12px 0}
button,input{padding:10px;border-radius:8px;border:1px solid #333;background:#111;color:#eee;font-size:15px}
button{background:#1d9bf0;border:none;cursor:pointer}
.big{font-size:26px;font-weight:700}
.lead{font-size:17px;font-weight:600;margin:8px 0}
.muted{color:#8b98a5;font-size:13px}
.warn{color:#ffad1f;font-size:14px}
.error{color:#ff7a7a}
.tag{display:inline-block;padding:3px 10px;border-radius:999px;font-size:13px;background:#1e3a5f;color:#8ecaff}
.tag.bad{background:#3f1d1d;color:#ff8f8f}
.disclaimer{background:#2a2110;border-left:4px solid #ffad1f;padding:10px;margin:12px 0;font-size:13px;color:#ffd78a}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px}
details{margin-top:10px;border-top:1px solid #2a3544;padding-top:10px}
summary{cursor:pointer;color:#1d9bf0;font-size:14px}
table{width:100%;font-size:13px;border-collapse:collapse;margin-top:8px}
td,th{padding:6px;border-bottom:1px solid #333;text-align:left}
</style></head><body>
<h1>🔦 灯塔 · 加密</h1>
<p class="muted">用大白话说明：现在贵不贵、曾经跌过多少 · 不预测明天涨跌</p>
<div class="disclaimer">不是荐币工具。只帮你看清位置和风险。</div>
<div class="card">
<input id="q" placeholder="例如 BTC 或 ETH">
<button onclick="searchA()">看看这个</button>
<p class="muted" id="status">—</p>
</div>
<div class="card"><b>数据从哪来</b><div id="sources" class="muted"></div></div>
<div class="card"><h2>一批币现在处在什么位置</h2><div id="rank" class="grid"></div></div>
<div class="card"><h2>外面的大环境（仅供参考）</h2><div id="macro" class="muted"></div></div>
<div id="detail"></div>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function pct(x){return typeof x==='number'?(x*100).toFixed(1)+'%':'—'}
function plainState(st){
  if(!st||!st.state)return '暂时说不清现在处在什么位置。';
  if(st.state==='数据不够')return '历史价格天数太少，先不判断。';
  if(st.structure_broken) return '现价已经低于近3个月的平均价，偏弱。';
  if(st.state.indexOf('偏稳')>=0) return '现价还在近几个月平均价上面，整体偏稳。';
  if(st.state.indexOf('偏弱')>=0) return '现价低于近几个月平均价，整体偏弱。';
  if(st.state.indexOf('变弱')>=0) return '还在平均价上面，但最近涨势变弱，要当心掉下去。';
  if(st.state.indexOf('回升')>=0) return '还在平均价下面，不过最近有抬头，还没算真正翻过来。';
  return '上上下下拉锯，方向还不清楚。';
}
function plainRisk(rk){
  const d90=rk&&rk.max_drawdown_90d;
  if(typeof d90!=='number')return '最近跌幅数据暂时没有。';
  const a=Math.abs(d90*100).toFixed(0);
  if(Math.abs(d90)>=0.30) return `最近大约3个月里，从高点最多掉过约 ${a}%。晃得很猛，仓位要格外小心。`;
  if(Math.abs(d90)>=0.15) return `最近大约3个月里，从高点最多掉过约 ${a}%。在币圈算常见晃动。`;
  return `最近大约3个月里，从高点最多掉过约 ${a}%。相对不算猛（以后未必如此）。`;
}
function row(k,v){return `<div>${esc(k)}：${esc(v)}</div>`}
function render(x){
  $('status').textContent='更新 '+(x.updated?new Date(x.updated).toLocaleString():'—')+' · '+esc(x.scan_status||'');
  $('sources').innerHTML=Object.entries(x.sources||{}).map(([k,v])=>row(k,v?'可用':'暂缺')).join('');
  const rank=x.rank||[];
  $('rank').innerHTML=rank.slice(0,6).map(s=>{
    const a=(x.assets||{})[s]||{}, st=a.structure||{}, rk=a.risk||{};
    const tag=st.structure_broken?'tag bad':'tag';
    return `<div class="card">
      <div class="lead">${esc(s.replace('USDT',''))} · $${Number(a.price).toLocaleString(undefined,{maximumFractionDigits:2})}</div>
      <p><span class="${tag}">${esc(st.state||'—')}</span></p>
      <p>${esc(plainState(st))}</p>
      <p class="muted">${esc(plainRisk(rk))}</p>
      <details><summary>数字细节（可点开）</summary>
        <p class="muted">近1个月 ${st.return_21d??'—'}% · 近3个月 ${st.return_63d??'—'}%</p>
        <p class="muted">近3个月最多掉过 ${pct(rk.max_drawdown_90d)} · 晃动 ${rk.vol20_daily_pct??'—'}%</p>
      </details>
    </div>`;
  }).join('')||'<p class="muted">列表加载中…</p>';
  const m=x.macro||{};
  $('macro').innerHTML=[
    m.fed_funds?row('美国政策利率', m.fed_funds.value+'（'+m.fed_funds.date+'）'):'',
    m.ust_10y?row('美国10年期国债利率', m.ust_10y.value+'（'+m.ust_10y.date+'）'):'',
    m.dollar_index?row('美元强弱', m.dollar_index.value):'',
    '<p class="muted">只是外面大环境，不会自动变成买卖建议。</p>'
  ].join('')||'暂缺';
}
async function load(){try{render(await fetch('/api/state',{cache:'no-store'}).then(r=>r.json()))}catch(e){$('status').textContent='连接异常'}}
async function searchA(){
  const q=$('q').value.trim(); if(!q)return;
  const d=await fetch('/api/search?symbol='+encodeURIComponent(q)).then(r=>r.json());
  if(d.status&&d.status!=='可分析'&&!d.price){
    $('detail').innerHTML='<div class="card error">'+esc(d.status||d.error||'看不了')+'</div>';
    return;
  }
  const st=d.structure||{}, rk=d.risk||{}, sh=d.similar_history||{};
  const tag=st.structure_broken?'tag bad':'tag';
  let histPlain='以前很少出现和现在差不多的情况，不强行类比。';
  if(sh.horizon_90d&&sh.sample_count>=10){
    const h=sh.horizon_90d;
    histPlain=`以前大约有 ${sh.sample_count} 次类似情况。那之后约3个月里，中间从高点掉下来，一半时候大约 ${pct(h.median_path_mdd)}，最惨大约 ${pct(h.worst_path_mdd)}。过去不保证未来。`;
  }
  let histTable='';
  if(sh.horizon_90d){
    const h=sh.horizon_90d;
    histTable=`<table>
      <tr><td>以前类似次数</td><td>${sh.sample_count}（参考价值：${esc(sh.reliability)}）</td></tr>
      <tr><td>约3个月后上涨的比例</td><td>${pct(h.up_rate)}</td></tr>
      <tr><td>约3个月后涨跌中间水平</td><td>${pct(h.median_return)}</td></tr>
      <tr><td>最差一次约3个月结果</td><td>${pct(h.worst_return)}</td></tr>
      <tr><td>中间从高点掉下来（一半水平）</td><td>${pct(h.median_path_mdd)}</td></tr>
      <tr><td>中间从高点掉下来（最惨）</td><td>${pct(h.worst_path_mdd)}</td></tr>
    </table>`;
  }
  $('detail').innerHTML=`<div class="card">
    <h2>${esc((d.symbol||q).replace('USDT',''))}</h2>
    <p class="big">$${Number(d.price).toLocaleString(undefined,{maximumFractionDigits:2})}</p>
    <p><span class="${tag}">${esc(st.state)}</span></p>
    <p class="lead">${esc(plainState(st))}</p>
    <p class="warn">什么情况算变了：${esc(st.invalidation||'')}</p>
  </div>
  <div class="card">
    <h3>曾经跌过多少（先看这里）</h3>
    <p class="lead">${esc(plainRisk(rk))}</p>
    <p>能查到的历史里，从高点最多掉过：<b>${pct(rk.max_drawdown_all)}</b></p>
    <p class="muted">如果从现在再跌 10% / 20% / 30%，大约到
      ${rk.stress_prices?.down_10pct} / ${rk.stress_prices?.down_20pct} / ${rk.stress_prices?.down_30pct}</p>
  </div>
  <div class="card">
    <h3>以前遇到类似情况时</h3>
    <p>${esc(histPlain)}</p>
    <p class="muted">${esc(sh.note||'')}</p>
  </div>
  <div class="card">
    <details>
      <summary>数字细节（可点开）</summary>
      <p>近3个月平均价 ${st.sma60} · 近半年平均价 ${st.sma120}</p>
      <p>近1个月 ${st.return_21d??'—'}% · 近3个月 ${st.return_63d??'—'}%</p>
      <p>最近晃动程度 ${rk.vol20_daily_pct??'—'}% · 近3个月最多掉过 ${pct(rk.max_drawdown_90d)}</p>
      ${histTable}
    </details>
  </div>
  <p class="muted">${esc(d.disclaimer||'')}</p>`;
}
load(); setInterval(load, 30000);
</script></body></html>"""
    return HTMLResponse(html)
