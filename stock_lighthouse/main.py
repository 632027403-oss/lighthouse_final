from __future__ import annotations

"""
Lighthouse US Stocks v3 — structure + risk dashboard (no predictive score).
Principles: real data only · no fabricated probabilities · clear invalidation · risk first.
"""

import asyncio
import csv
import io
import math
import re
import statistics
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

APP_VERSION = "3.0-structure-risk"
BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
TICKER_RE = re.compile(r"^[A-Z0-9.\-^=]{1,15}$")

SCAN_UNIVERSE = [
    "NVDA", "AMD", "AVGO", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "GOOG", "TSLA",
    "NFLX", "ORCL", "CRM", "PLTR", "MU", "MRVL", "QCOM", "INTC", "TSM", "ARM",
    "SMCI", "AMAT", "LRCX", "ASML", "V", "MA", "JPM", "LLY", "XOM", "COST",
]
DEFAULTS = ["AMD", "MRVL", "NVDA"]
SCAN_INTERVAL_SECONDS = 90

_scan_cache: dict[str, Any] = {
    "status": "not_started",
    "started_at": None,
    "completed_at": None,
    "results": [],
    "errors": [],
    "universe_size": len(SCAN_UNIVERSE),
}
_scan_lock = asyncio.Lock()
_last_scan_task: asyncio.Task | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * p
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return vals[lo]
    return vals[lo] * (hi - pos) + vals[hi] * (pos - lo)


def max_drawdown(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    peak = values[0]
    worst = 0.0
    for value in values[1:]:
        peak = max(peak, value)
        if peak > 0:
            worst = min(worst, value / peak - 1)
    return worst


def reliability_level(n: int) -> str:
    if n >= 50:
        return "高"
    if n >= 20:
        return "中"
    if n >= 10:
        return "低"
    return "不足"


def clean_close(raw: list[Any]) -> list[float]:
    out: list[float] = []
    for x in raw:
        try:
            v = float(x)
            if math.isfinite(v) and v > 0:
                out.append(v)
        except (TypeError, ValueError):
            continue
    return out


async def yahoo_get(ticker: str, range_: str = "2y", interval: str = "1d") -> dict[str, Any]:
    ticker = ticker.upper().strip()
    if not TICKER_RE.fullmatch(ticker):
        raise ValueError("股票代码格式无效")
    headers = {"User-Agent": "Mozilla/5.0 Lighthouse/3.0"}
    timeout = httpx.Timeout(15.0, connect=8.0)
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=timeout) as client:
        response = await client.get(
            BASE + ticker,
            params={"range": range_, "interval": interval, "events": "div,splits"},
        )
        response.raise_for_status()
        data = response.json()
    result = data.get("chart", {}).get("result")
    if not isinstance(result, list) or not result or not isinstance(result[0], dict):
        raise ValueError("Yahoo 公开行情源未返回有效数据")
    return result[0]


async def history(ticker: str) -> list[float]:
    item = await yahoo_get(ticker, "2y", "1d")
    quote = item.get("indicators", {}).get("quote", [{}])[0]
    close = clean_close(quote.get("close", []))
    if len(close) < 220:
        raise ValueError("历史价格数据不足（至少约220个交易日）")
    return close


async def realtime(ticker: str) -> dict[str, Any]:
    item = await yahoo_get(ticker, "1d", "1m")
    meta = item.get("meta", {})
    price = meta.get("regularMarketPrice")
    ts = meta.get("regularMarketTime")
    if price is None or ts is None:
        raise ValueError("Yahoo 未提供可用的市场价格时间戳，拒绝用历史K线冒充实时价格")
    price = float(price)
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Yahoo 市场价格无效")
    return {
        "price": price,
        "timestamp": datetime.fromtimestamp(int(ts), timezone.utc).isoformat(),
        "currency": meta.get("currency") or "USD",
        "exchange": meta.get("exchangeName") or meta.get("fullExchangeName"),
        "market_state": meta.get("marketState"),
    }



FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_macro_cache: dict[str, Any] = {"ts": 0, "data": {}}


async def macro_background() -> dict[str, Any]:
    import time as _time
    now = _time.time()
    if _macro_cache.get("data") and now - _macro_cache.get("ts", 0) < 900:
        return _macro_cache["data"]

    async def fred_one(client: httpx.AsyncClient, series: str):
        try:
            r = await client.get(FRED, params={"id": series}, timeout=15.0)
            r.raise_for_status()
            rows = [row for row in csv.DictReader(io.StringIO(r.text)) if row.get(series) not in (None, "", ".")]
            if not rows:
                return None
            return {"value": float(rows[-1][series]), "date": rows[-1]["observation_date"]}
        except Exception:
            return None

    headers = {"User-Agent": "Mozilla/5.0 Lighthouse/3.0"}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        fed, dgs10, dxy = await asyncio.gather(
            fred_one(client, "FEDFUNDS"),
            fred_one(client, "DGS10"),
            fred_one(client, "DTWEXBGS"),
        )
    bits = []
    if fed and isinstance(fed.get("value"), (int, float)):
        v = fed["value"]
        if v >= 4.5:
            bits.append("政策利率仍偏高，借钱成本不低")
        elif v <= 2.0:
            bits.append("政策利率偏低，借钱相对便宜")
        else:
            bits.append("政策利率处于中间地带")
    if dgs10 and isinstance(dgs10.get("value"), (int, float)):
        v = dgs10["value"]
        if v >= 4.0:
            bits.append("长期利率偏高，对爱涨价的股票往往不太友好")
        elif v <= 2.5:
            bits.append("长期利率偏低，股票有时更容易被追捧")
        else:
            bits.append("长期利率不高不低")
    if dxy and isinstance(dxy.get("value"), (int, float)):
        v = dxy["value"]
        if v >= 105:
            bits.append("美元偏强时，全球风险资产常更吃力")
        elif v <= 95:
            bits.append("美元偏弱时，风险资产有时更容易抬头")
        else:
            bits.append("美元强弱大致中性")
    out = {
        "fed_funds": fed,
        "ust_10y": dgs10,
        "dollar_index": dxy,
        "plain_lines": bits,
        "how_it_works": (
            "大环境不直接决定某只股票明天涨跌，但会影响大家愿不愿意买股票。"
            "利率升高或美元变强时，股票常常更难涨、更容易晃；"
            "利率回落或美元变弱时，往往更容易有人愿意买。"
            "这只是常见倾向，不是定律，也不是买卖信号。"
        ),
        "label": "外面的大环境（只作背景，不合成买卖分）",
    }
    _macro_cache["ts"] = now
    _macro_cache["data"] = out
    return out


def structure_state(close: list[float]) -> dict[str, Any]:
    """Deterministic regime labels — descriptive only, not a forecast score."""
    latest = close[-1]
    sma60 = statistics.fmean(close[-60:])
    sma120 = statistics.fmean(close[-120:])
    sma200 = statistics.fmean(close[-200:]) if len(close) >= 200 else None

    r21 = latest / close[-22] - 1 if len(close) >= 22 else 0.0
    r63 = latest / close[-64] - 1 if len(close) >= 64 else 0.0

    above60 = latest > sma60
    above120 = latest > sma120
    above200 = (latest > sma200) if sma200 is not None else None

    if above60 and above120 and r63 > 0:
        state = "价位偏稳"
        invalidation = "如果收盘价掉到近3个月平均价下面，就不再算偏稳"
    elif (not above60) and (not above120) and r63 < 0:
        state = "价位偏弱"
        invalidation = "如果收盘价重新回到近3个月平均价上面，偏弱会减轻"
    elif above60 and r63 < -0.05:
        state = "还在平均价上方，但近况变弱"
        invalidation = "若掉到近3个月平均价下面，就算明显走弱"
    elif (not above60) and r63 > 0.05:
        state = "还在平均价下方，但近况有回升"
        invalidation = "要重新站上近3个月平均价，才算真正好转"
    else:
        state = "上下拉锯，方向不清"
        invalidation = "等价格和近几个月平均价的关系更清楚再说"

    structure_broken = (not above60)

    rules_hit = sum([
        1 if above60 else 0,
        1 if above120 else 0,
        1 if (above200 is True) else 0,
        1 if r63 > 0 else 0,
        1 if r21 > 0 else 0,
    ])
    rules_total = 5 if above200 is not None else 4

    return {
        "state": state,
        "invalidation": invalidation,
        "structure_broken": structure_broken,
        "rules_aligned": rules_hit,
        "rules_total": rules_total,
        "sma60": round(sma60, 2),
        "sma120": round(sma120, 2),
        "sma200": round(sma200, 2) if sma200 is not None else None,
        "return_21d": round(r21 * 100, 2),
        "return_63d": round(r63 * 100, 2),
        "flags": {
            "above_sma60": above60,
            "above_sma120": above120,
            "above_sma200": above200,
            "r63_positive": r63 > 0,
            "r21_positive": r21 > 0,
        },
    }


def window_peak_trough(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    return max(values), min(values)


def risk_metrics(close: list[float]) -> dict[str, Any]:
    latest = close[-1]
    daily = [close[i] / close[i - 1] - 1 for i in range(1, len(close))]
    vol20 = statistics.pstdev(daily[-20:]) if len(daily) >= 20 else None
    vol60 = statistics.pstdev(daily[-60:]) if len(daily) >= 60 else None
    w90 = close[-91:] if len(close) >= 91 else close[:]
    w252 = close[-253:] if len(close) >= 253 else close[:]
    mdd_90 = max_drawdown(w90)
    mdd_252 = max_drawdown(w252)
    peak90, trough90 = window_peak_trough(w90)
    peak252, trough252 = window_peak_trough(w252)

    stress = {
        "down_10pct": round(latest * 0.90, 2),
        "down_20pct": round(latest * 0.80, 2),
        "down_30pct": round(latest * 0.70, 2),
    }

    return {
        "vol20_daily_pct": round(vol20 * 100, 2) if vol20 is not None else None,
        "vol60_daily_pct": round(vol60 * 100, 2) if vol60 is not None else None,
        "max_drawdown_90d": round(mdd_90, 4),
        "max_drawdown_252d": round(mdd_252, 4),
        "peak_90d": round(peak90, 2) if peak90 is not None else None,
        "trough_90d": round(trough90, 2) if trough90 is not None else None,
        "peak_252d": round(peak252, 2) if peak252 is not None else None,
        "trough_252d": round(trough252, 2) if trough252 is not None else None,
        "stress_prices": stress,
    }


def similar_drawdown_stats(close: list[float], struct: dict[str, Any]) -> dict[str, Any]:
    """Historical path drawdowns under similar structure — risk focus, not win-rate hype."""
    above60 = struct["flags"]["above_sma60"]
    above120 = struct["flags"]["above_sma120"]
    r63_pos = struct["flags"]["r63_positive"]

    matches: list[int] = []
    for i in range(120, len(close) - 90):
        s60 = statistics.fmean(close[i - 59 : i + 1])
        s120 = statistics.fmean(close[i - 119 : i + 1])
        r63_h = close[i] / close[i - 63] - 1 if i >= 63 else 0.0
        if (close[i] > s60) != above60:
            continue
        if (close[i] > s120) != above120:
            continue
        if (r63_h > 0) != r63_pos:
            continue
        matches.append(i)

    records_90 = []
    for i in matches:
        path = close[i : i + 91]
        if len(path) < 91:
            continue
        ret90 = path[-1] / path[0] - 1
        records_90.append({"ret": ret90, "mdd": max_drawdown(path)})

    n = len(records_90)
    if n == 0:
        return {
            "sample_count": 0,
            "reliability": "不足",
            "note": "以前类似的情况太少，这里不硬凑数字",
        }

    rets = [x["ret"] for x in records_90]
    mdds = [x["mdd"] for x in records_90]
    return {
        "sample_count": n,
        "reliability": reliability_level(n),
        "note": (
            "下面是以前出现类似情况时，大约三个月里曾经发生过什么。次数有限，不能当以后一定会怎样。"
            if n >= 20
            else "类似情况以前出现得不多，只能当参考。"
        ),
        "horizon_90d": {
            "up_rate": sum(v > 0 for v in rets) / n,
            "median_return": statistics.median(rets),
            "p25_return": percentile(rets, 0.25),
            "p75_return": percentile(rets, 0.75),
            "worst_return": min(rets),
            "median_path_mdd": statistics.median(mdds),
            "worst_path_mdd": min(mdds),
        } if n >= 10 else None,
    }


def analyze(close: list[float]) -> dict[str, Any]:
    struct = structure_state(close)
    risk = risk_metrics(close)
    similar = similar_drawdown_stats(close, struct)
    return {
        "structure": struct,
        "risk": risk,
        "similar_history": similar,
        "disclaimer": "只帮助看清现在贵不贵、曾经跌过多少。不是买卖建议，也不保证以后怎么走。",
    }


async def asset(ticker: str) -> dict[str, Any]:
    ticker = ticker.upper().strip()
    close = await history(ticker)
    rt = await realtime(ticker)
    body = analyze(close)
    return {
        "ticker": ticker,
        "realtime": rt,
        "history_last_close": close[-1],
        "updated_at": now_iso(),
        "app_version": APP_VERSION,
        **body,
    }


async def scan_one(ticker: str) -> dict[str, Any]:
    try:
        item = await asset(ticker)
        # Rank scan list by realized risk context, not a fake alpha score:
        # prefer clearer structure + show drawdown; sort key = structure_broken then vol
        broken = 1 if item["structure"]["structure_broken"] else 0
        vol = item["risk"].get("vol20_daily_pct") or 99
        item["scan_order_key"] = (broken, vol)
        return item
    except Exception as exc:
        return {"ticker": ticker, "error": str(exc)}


async def run_scan() -> None:
    global _scan_cache
    async with _scan_lock:
        started = datetime.now(timezone.utc)
        _scan_cache = {
            **_scan_cache,
            "status": "scanning",
            "started_at": started.isoformat(),
            "errors": [],
        }
        sem = asyncio.Semaphore(8)

        async def limited(t: str) -> dict[str, Any]:
            async with sem:
                return await scan_one(t)

        results = await asyncio.gather(*(limited(t) for t in SCAN_UNIVERSE))
        good = [x for x in results if "error" not in x]
        # List intact structures first, then by lower short vol (calmer)
        good.sort(key=lambda x: x.get("scan_order_key", (1, 99)))
        errors = [x for x in results if "error" in x]
        finished = datetime.now(timezone.utc)
        _scan_cache = {
            "status": "ready",
            "started_at": started.isoformat(),
            "completed_at": finished.isoformat(),
            "duration_seconds": round((finished - started).total_seconds(), 2),
            "results": good,
            "errors": errors,
            "universe_size": len(SCAN_UNIVERSE),
            "successful_count": len(good),
            "failed_count": len(errors),
            "app_version": APP_VERSION,
            "note": "列表按「是否偏弱、晃动大小」排列，不是推荐买卖",
        }


async def scan_loop() -> None:
    while True:
        try:
            await run_scan()
        except Exception as exc:
            _scan_cache["status"] = "error"
            _scan_cache["errors"] = [{"ticker": "__scan__", "error": str(exc)}]
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scan_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Lighthouse US Stocks", version=APP_VERSION, lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION, "scan": _scan_cache["status"], "updated_at": now_iso()}


@app.get("/api/asset")
async def api_asset(ticker: str = Query(..., min_length=1, max_length=15)):
    try:
        return await asset(ticker)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})
    except httpx.HTTPError:
        return JSONResponse(status_code=502, content={"detail": "公开行情源暂时无法访问"})
    except Exception:
        return JSONResponse(status_code=500, content={"detail": "分析过程中发生内部错误"})


@app.get("/api/scan")
async def api_scan(refresh: bool = False):
    global _last_scan_task
    if refresh and (_last_scan_task is None or _last_scan_task.done()):
        _last_scan_task = asyncio.create_task(run_scan())
    return _scan_cache


@app.get("/api/macro")
async def api_macro():
    try:
        return await macro_background()
    except Exception:
        return JSONResponse(status_code=502, content={"detail": "宏观数据暂时读不到"})


@app.get("/api/universe")
async def api_universe():
    return {"count": len(SCAN_UNIVERSE), "tickers": SCAN_UNIVERSE}


@app.get("/")
async def home():
    html = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lighthouse 美股</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;max-width:920px;margin:auto;padding:16px;background:#f0f2f5;color:#1a1a1a;line-height:1.55}
.card{background:#fff;border-radius:12px;padding:14px 16px;margin:12px 0;box-shadow:0 1px 6px rgba(0,0,0,.07)}
button,input{padding:10px 12px;border-radius:8px;border:1px solid #ccc;font-size:15px}
button{cursor:pointer;background:#1a56db;color:#fff;border:none}
button.secondary{background:#e5e7eb;color:#111}
.big{font-size:26px;font-weight:700}
.lead{font-size:17px;font-weight:600;margin:8px 0}
.muted{color:#6b7280;font-size:13px}
.warn{color:#b45309;font-size:14px}
.error{color:#b00020}
.tag{display:inline-block;padding:3px 10px;border-radius:999px;font-size:13px;background:#e0e7ff;color:#3730a3}
.tag.bad{background:#fee2e2;color:#991b1b}
.disclaimer{background:#fff7ed;border-left:4px solid #f59e0b;padding:10px 12px;margin:12px 0;font-size:13px;color:#92400e}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px}
details{margin-top:10px;border-top:1px solid #eee;padding-top:10px}
summary{cursor:pointer;color:#1a56db;font-size:14px;user-select:none}
table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}
th,td{padding:6px;border-bottom:1px solid #eee;text-align:left}
</style></head><body>
<h1>🔦 灯塔 · 美股</h1>
<p class="muted">用大白话说明：现在贵不贵、曾经跌过多少 · 不预测明天涨跌</p>
<div class="disclaimer">这不是荐股工具，也没有「胜率」。只帮你少一点雾里看花。</div>
<div class="card">
<input id="ticker" value="AMD" maxlength="15" placeholder="输入代码，如 AMD">
<button onclick="loadStock()">看看这只</button>
<button class="secondary" onclick="loadScan(true)">刷新列表</button>
</div>
<div id="macro" class="card">大环境加载中…</div>
<div id="scan" class="card">列表加载中…</div>
<div id="result"></div>
<script>
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function pct(x){return typeof x==='number'?(x*100).toFixed(1)+'%':'—'}
function plainState(st){
  if(!st||!st.state)return '暂时说不清现在处在什么位置。';
  if(st.structure_broken) return '现价已经低于近3个月的平均价，偏弱。';
  if(st.state.indexOf('偏稳')>=0) return '现价还在近几个月平均价上面，整体偏稳。';
  if(st.state.indexOf('偏弱')>=0) return '现价低于近几个月平均价，整体偏弱。';
  if(st.state.indexOf('变弱')>=0) return '还在平均价上面，但最近两三个月涨得不如从前，要当心掉下去。';
  if(st.state.indexOf('回升')>=0) return '还在平均价下面，不过最近有抬头，还没算真正翻过来。';
  return '上上下下拉锯，方向还不清楚。';
}
function money(x){return (typeof x==='number')?('$'+Number(x).toFixed(2)):'—'}
function plainRisk(rk){
  const d90=rk&&rk.max_drawdown_90d;
  if(typeof d90!=='number')return '最近跌幅数据暂时没有。';
  const a=Math.abs(d90*100).toFixed(1);
  const peak=rk.peak_90d, trough=rk.trough_90d;
  let s=`最近大约3个月里，高点大约在 ${money(peak)}，最低大约到 ${money(trough)}，等于从高点最多掉过约 ${a}%。`;
  if(Math.abs(d90)>=0.25) s+=' 晃得比较大，仓位要自己掂量。';
  else if(Math.abs(d90)>=0.12) s+=' 属于常见晃动。';
  else s+=' 相对不算猛（以后未必如此）。';
  return s;
}
async function loadMacro(){
  try{
    const m=await fetch('/api/macro').then(r=>r.json());
    const lines=(m.plain_lines||[]).map(t=>'<p>· '+esc(t)+'</p>').join('');
    document.getElementById('macro').innerHTML=
      `<h2>外面的大环境</h2>
       <p class="lead">${esc(m.how_it_works||'')}</p>
       ${lines||'<p class="muted">句子生成中…</p>'}
       <details><summary>具体数字（可点开）</summary>
         ${m.fed_funds?`<p>美国政策利率：${m.fed_funds.value}（${esc(m.fed_funds.date)}）</p>`:''}
         ${m.ust_10y?`<p>美国10年期国债利率：${m.ust_10y.value}（${esc(m.ust_10y.date)}）</p>`:''}
         ${m.dollar_index?`<p>美元指数：${m.dollar_index.value}</p>`:''}
       </details>
       <p class="muted">${esc(m.label||'')}</p>`;
  }catch(e){
    document.getElementById('macro').innerHTML='<span class="error">大环境暂时读不到</span>';
  }
}
async function loadScan(refresh=false){
  try{
    const r=await fetch('/api/scan'+(refresh?'?refresh=true':''));
    const d=await r.json();
    const rows=(d.results||[]).slice(0,8).map(x=>{
      const st=x.structure||{}, rk=x.risk||{};
      const tag=st.structure_broken?'tag bad':'tag';
      return `<div class="card">
        <div class="lead">${esc(x.ticker)} · $${Number(x.realtime&&x.realtime.price).toFixed(2)}</div>
        <p><span class="${tag}">${esc(st.state||'—')}</span></p>
        <p>${esc(plainState(st))}</p>
        <p class="muted">${esc(plainRisk(rk))}</p>
        <details><summary>数字细节（可点开）</summary>
          <p class="muted">近3个月平均价 $${st.sma60} · 近半年平均价 $${st.sma120}</p>
          <p class="muted">近1个月涨跌 ${st.return_21d}% · 近3个月 ${st.return_63d}%</p>
          <p class="muted">近3个月高点 ${money(rk.peak_90d)} → 最低 ${money(rk.trough_90d)}（约掉 ${pct(rk.max_drawdown_90d)}）</p>
        </details>
      </div>`;
    }).join('');
    document.getElementById('scan').innerHTML=
      `<h2>一批股票现在处在什么位置</h2>
       <p class="muted">${esc(d.note||'')} · 成功 ${d.successful_count??0}/${d.universe_size??0}</p>
       <div class="grid">${rows||'<p class="muted">还没有结果</p>'}</div>
       <p class="muted">更新时间：${esc(d.completed_at||'—')}</p>`;
  }catch(e){
    document.getElementById('scan').innerHTML='<span class="error">列表暂时打不开</span>';
  }
}
async function loadStock(){
  const t=document.getElementById('ticker').value.trim().toUpperCase();
  if(!t)return;
  const box=document.getElementById('result');
  box.innerHTML='<div class="card">加载中…</div>';
  try{
    const r=await fetch('/api/asset?ticker='+encodeURIComponent(t));
    const d=await r.json();
    if(!r.ok)throw Error(d.detail||'失败');
    const st=d.structure||{}, rk=d.risk||{}, sh=d.similar_history||{}, rt=d.realtime||{};
    const h=sh.horizon_90d;
    const tag=st.structure_broken?'tag bad':'tag';
    let histPlain='以前很少出现和现在差不多的情况，这里不强行类比。';
    if(h&&sh.sample_count>=10){
      histPlain=`以前出现过大约 ${sh.sample_count} 次类似情况。那之后约3个月里，中间从高点掉下来的幅度，一半时候大约在 ${pct(h.median_path_mdd)}，最惨大约 ${pct(h.worst_path_mdd)}。这是过去的事，不保证以后一样。`;
    }
    let histTable='';
    if(h){
      histTable=`<table><tr><th>说什么</th><th>数字</th></tr>
        <tr><td>以前类似次数</td><td>${sh.sample_count}（参考价值：${esc(sh.reliability)}）</td></tr>
        <tr><td>约3个月后上涨的比例</td><td>${pct(h.up_rate)}</td></tr>
        <tr><td>约3个月后涨跌的中间水平</td><td>${pct(h.median_return)}</td></tr>
        <tr><td>常见大致区间</td><td>${pct(h.p25_return)} ~ ${pct(h.p75_return)}</td></tr>
        <tr><td>最差一次约3个月结果</td><td>${pct(h.worst_return)}</td></tr>
        <tr><td>中间从高点掉下来（一半水平）</td><td>${pct(h.median_path_mdd)}</td></tr>
        <tr><td>中间从高点掉下来（最惨）</td><td>${pct(h.worst_path_mdd)}</td></tr>
      </table>`;
    }
    box.innerHTML=`
      <div class="card">
        <h2>${esc(d.ticker)}</h2>
        <div class="big">$${Number(rt.price).toFixed(2)}</div>
        <p class="muted">${esc(rt.market_state||'')} · ${esc(rt.timestamp||'')}</p>
        <p><span class="${tag}">${esc(st.state)}</span></p>
        <p class="lead">${esc(plainState(st))}</p>
        <p>近3个月平均价 <b>$${st.sma60}</b> · 近半年 <b>$${st.sma120}</b> · 近一年 <b>$${st.sma200??'—'}</b></p>
        <p class="warn">什么情况算变了：${esc(st.invalidation||'')}</p>
      </div>
      <div class="card">
        <h3>曾经跌过多少（先看这里）</h3>
        <p class="lead">${esc(plainRisk(rk))}</p>
        <p>最近大约一年里：高点约 <b>${money(rk.peak_252d)}</b>，最低约 <b>${money(rk.trough_252d)}</b>，从高点最多掉过 <b>${pct(rk.max_drawdown_252d)}</b>。</p>
        <p class="muted">如果从现在的 $${Number(rt.price).toFixed(2)} 再跌：<br>
          跌10% → 约 $${rk.stress_prices?.down_10pct}<br>
          跌20% → 约 $${rk.stress_prices?.down_20pct}<br>
          跌30% → 约 $${rk.stress_prices?.down_30pct}</p>
      </div>
      <div class="card">
        <h3>以前遇到类似情况时</h3>
        <p>${esc(histPlain)}</p>
        <p class="muted">${esc(sh.note||'')}</p>
      </div>
      <div class="card">
        <details>
          <summary>数字细节（可点开）</summary>
          <p>近3个月平均价：<b>$${st.sma60}</b> · 近半年平均价：<b>$${st.sma120}</b> · 近一年平均价：<b>$${st.sma200??'—'}</b></p>
          <p>近1个月涨跌 ${st.return_21d}% · 近3个月 ${st.return_63d}%</p>
          <p>近3个月：高点 ${money(rk.peak_90d)} → 最低 ${money(rk.trough_90d)}（从高点约掉 ${pct(rk.max_drawdown_90d)}）</p>
          <p>最近晃动程度（短/稍长）：${rk.vol20_daily_pct??'—'}% / ${rk.vol60_daily_pct??'—'}%</p>
          ${histTable}
        </details>
      </div>
      <p class="muted">${esc(d.disclaimer)}</p>`;
  }catch(e){
    box.innerHTML='<div class="card error">'+esc(e.message)+'</div>';
  }
}
loadMacro();loadScan();loadStock();
setInterval(()=>{loadMacro();loadScan(false);},30000);
</script></body></html>"""
    return HTMLResponse(html)
