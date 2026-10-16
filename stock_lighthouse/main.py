from __future__ import annotations

"""
Lighthouse US Stocks v3 — structure + risk dashboard (no predictive score).
Principles: real data only · no fabricated probabilities · clear invalidation · risk first.
"""

import asyncio
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
        state = "中期结构偏多"
        invalidation = "若收盘跌破60日均线，本状态失效，视为结构破坏"
    elif (not above60) and (not above120) and r63 < 0:
        state = "中期结构偏空"
        invalidation = "若收盘重新站上60日均线，偏空结构减弱"
    elif above60 and r63 < -0.05:
        state = "仍在均线上方但动能转弱"
        invalidation = "跌破60日均线则确认结构破坏"
    elif (not above60) and r63 > 0.05:
        state = "均线下方、动能回升（修复中）"
        invalidation = "需站上60日均线才确认结构修复"
    else:
        state = "震荡/过渡"
        invalidation = "等待价格与60/120日均线关系明确"

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


def risk_metrics(close: list[float]) -> dict[str, Any]:
    latest = close[-1]
    daily = [close[i] / close[i - 1] - 1 for i in range(1, len(close))]
    vol20 = statistics.pstdev(daily[-20:]) if len(daily) >= 20 else None
    vol60 = statistics.pstdev(daily[-60:]) if len(daily) >= 60 else None
    mdd_90 = max_drawdown(close[-91:]) if len(close) >= 91 else None
    mdd_252 = max_drawdown(close[-253:]) if len(close) >= 253 else max_drawdown(close)

    # Simple "if falls further" levels from current price
    stress = {
        "down_10pct": round(latest * 0.90, 2),
        "down_20pct": round(latest * 0.80, 2),
        "down_30pct": round(latest * 0.70, 2),
    }

    return {
        "vol20_daily_pct": round(vol20 * 100, 2) if vol20 is not None else None,
        "vol60_daily_pct": round(vol60 * 100, 2) if vol60 is not None else None,
        "max_drawdown_90d": round(mdd_90, 4) if mdd_90 is not None else None,
        "max_drawdown_252d": round(mdd_252, 4) if mdd_252 is not None else None,
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
            "note": "没有足够的历史相似样本，不展示条件统计",
        }

    rets = [x["ret"] for x in records_90]
    mdds = [x["mdd"] for x in records_90]
    return {
        "sample_count": n,
        "reliability": reliability_level(n),
        "note": (
            "以下为历史相似结构下的90日路径统计，含重叠窗口；有效信息量小于名义样本。非未来保证。"
            if n >= 20
            else "样本偏少，仅作辅助参考。"
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
        "disclaimer": "结构描述与历史回撤统计仅供参考，不构成买卖建议；非预测胜率。",
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
            "note": "列表按「结构是否破坏、短期波动」排列，不是预测排名",
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


@app.get("/api/universe")
async def api_universe():
    return {"count": len(SCAN_UNIVERSE), "tickers": SCAN_UNIVERSE}


@app.get("/")
async def home():
    html = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lighthouse 美股 v3</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;max-width:920px;margin:auto;padding:16px;background:#f0f2f5;color:#1a1a1a}
.card{background:#fff;border-radius:12px;padding:14px 16px;margin:12px 0;box-shadow:0 1px 6px rgba(0,0,0,.07)}
button,input{padding:10px 12px;border-radius:8px;border:1px solid #ccc;font-size:15px}
button{cursor:pointer;background:#1a56db;color:#fff;border:none}
button.secondary{background:#e5e7eb;color:#111}
.big{font-size:26px;font-weight:700}
.muted{color:#6b7280;font-size:13px}
.warn{color:#b45309;font-size:13px}
.error{color:#b00020}
.tag{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;background:#e0e7ff;color:#3730a3}
.tag.bad{background:#fee2e2;color:#991b1b}
.disclaimer{background:#fff7ed;border-left:4px solid #f59e0b;padding:10px 12px;margin:12px 0;font-size:13px;color:#92400e}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px;border-bottom:1px solid #eee;text-align:left}
</style></head><body>
<h1>🔦 Lighthouse 美股</h1>
<p class="muted">结构说明书 · 风险尺 · 不预测涨跌 · v3.0</p>
<div class="disclaimer">本工具描述均线结构与历史回撤，<strong>不提供买卖建议</strong>，不输出预测胜率。列表排序不是「最该买」。</div>
<div class="card">
<input id="ticker" value="AMD" maxlength="15" placeholder="股票代码">
<button onclick="loadStock()">分析</button>
<button class="secondary" onclick="loadScan(true)">刷新扫描</button>
</div>
<div id="scan" class="card">扫描读取中…</div>
<div id="result"></div>
<script>
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function pct(x){return typeof x==='number'?(x*100).toFixed(1)+'%':'—'}
async function loadScan(refresh=false){
  try{
    const r=await fetch('/api/scan'+(refresh?'?refresh=true':''));
    const d=await r.json();
    const rows=(d.results||[]).slice(0,8).map(x=>{
      const st=x.structure||{}, rk=x.risk||{};
      const tag=st.structure_broken?'tag bad':'tag';
      return `<div class="card">
        <b>${esc(x.ticker)}</b> · $${Number(x.realtime&&x.realtime.price).toFixed(2)}
        <p><span class="${tag}">${esc(st.state)}</span>
        对齐 ${st.rules_aligned}/${st.rules_total}</p>
        <p class="muted">21日 ${st.return_21d}% · 63日 ${st.return_63d}%</p>
        <p class="muted">90日回撤 ${pct(rk.max_drawdown_90d)} · 波动20 ${rk.vol20_daily_pct??'—'}%</p>
      </div>`;
    }).join('');
    document.getElementById('scan').innerHTML=
      `<h2>扫描池快照</h2>
       <p class="muted">${esc(d.note||'')} · 成功 ${d.successful_count??0}/${d.universe_size??0}</p>
       <div class="grid">${rows||'<p class="muted">尚无结果</p>'}</div>
       <p class="muted">完成：${esc(d.completed_at||'—')}</p>`;
  }catch(e){
    document.getElementById('scan').innerHTML='<span class="error">扫描暂不可用</span>';
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
    let sim='<p class="muted">'+esc(sh.note||'')+'</p>';
    if(h){
      sim+=`<table><tr><th>项目</th><th>数值</th></tr>
        <tr><td>样本数</td><td>${sh.sample_count}（可靠度 ${esc(sh.reliability)}）</td></tr>
        <tr><td>90日上涨占比</td><td>${pct(h.up_rate)}</td></tr>
        <tr><td>90日中位收益</td><td>${pct(h.median_return)}</td></tr>
        <tr><td>常见区间</td><td>${pct(h.p25_return)} ~ ${pct(h.p75_return)}</td></tr>
        <tr><td>最差收益</td><td>${pct(h.worst_return)}</td></tr>
        <tr><td>路径中位回撤</td><td>${pct(h.median_path_mdd)}</td></tr>
        <tr><td>路径最差回撤</td><td>${pct(h.worst_path_mdd)}</td></tr>
      </table>`;
    }
    box.innerHTML=`
      <div class="card">
        <h2>${esc(d.ticker)}</h2>
        <div class="big">$${Number(rt.price).toFixed(2)}</div>
        <p class="muted">${esc(rt.market_state||'')} · ${esc(rt.timestamp||'')}</p>
        <p><span class="${tag}">${esc(st.state)}</span>
        规则对齐 ${st.rules_aligned}/${st.rules_total}</p>
        <p class="warn">${esc(st.invalidation)}</p>
        <p>SMA60 $${st.sma60} · SMA120 $${st.sma120} · SMA200 $${st.sma200??'—'}</p>
        <p>21日 ${st.return_21d}% · 63日 ${st.return_63d}%</p>
      </div>
      <div class="card">
        <h3>风险尺</h3>
        <p>近90日最大回撤：<b>${pct(rk.max_drawdown_90d)}</b></p>
        <p>近一年最大回撤：<b>${pct(rk.max_drawdown_252d)}</b></p>
        <p>波动(20日日)：${rk.vol20_daily_pct??'—'}% · (60日)：${rk.vol60_daily_pct??'—'}%</p>
        <p class="muted">若再跌10%/20%/30%：
          $${rk.stress_prices?.down_10pct} /
          $${rk.stress_prices?.down_20pct} /
          $${rk.stress_prices?.down_30pct}</p>
      </div>
      <div class="card">
        <h3>历史相似结构下的90日路径</h3>
        ${sim}
      </div>
      <p class="muted">${esc(d.disclaimer)}</p>`;
  }catch(e){
    box.innerHTML='<div class="card error">'+esc(e.message)+'</div>';
  }
}
loadScan();loadStock();
setInterval(()=>loadScan(false),30000);
</script></body></html>"""
    return HTMLResponse(html)
