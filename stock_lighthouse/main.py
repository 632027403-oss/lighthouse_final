from __future__ import annotations

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

APP_VERSION = "2.0-final"
BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_BASE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Fixed, transparent 30-stock scan universe. It is a scan universe, not a claim that
# these are the only stocks worth analyzing. Any valid Yahoo ticker can still be queried.
SCAN_UNIVERSE = [
    "NVDA", "AMD", "AVGO", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "GOOG", "TSLA",
    "NFLX", "ORCL", "CRM", "PLTR", "MU", "MRVL", "QCOM", "INTC", "TSM", "ARM",
    "SMCI", "AMAT", "LRCX", "ASML", "V", "MA", "JPM", "LLY", "XOM", "COST",
]
DEFAULTS = ["AMD", "MRVL", "NVDA"]
TICKER_RE = re.compile(r"^[A-Z0-9.\-^=]{1,15}$")
SCAN_INTERVAL_SECONDS = 60
FRONTEND_REFRESH_SECONDS = 30

_scan_cache: dict[str, Any] = {
    "status": "not_started",
    "started_at": None,
    "completed_at": None,
    "duration_seconds": None,
    "results": [],
    "errors": [],
    "top3": [],
    "universe_size": len(SCAN_UNIVERSE),
}
_scan_lock = asyncio.Lock()
_last_scan_task: asyncio.Task | None = None
_sec_ticker_map: dict[str, str] | None = None
_sec_lock = asyncio.Lock()


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


async def yahoo_get(ticker: str, range_: str = "1y", interval: str = "1d") -> dict[str, Any]:
    ticker = ticker.upper().strip()
    if not TICKER_RE.fullmatch(ticker):
        raise ValueError("股票代码格式无效")
    headers = {"User-Agent": "Mozilla/5.0 Lighthouse/2.0"}
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


async def history(ticker: str) -> tuple[list[float], dict[str, Any]]:
    item = await yahoo_get(ticker, "1y", "1d")
    quote = item.get("indicators", {}).get("quote", [{}])[0]
    close = clean_close(quote.get("close", []))
    if len(close) < 180:
        raise ValueError("历史价格数据不足，无法进行可靠统计")
    return close, item.get("meta", {})


async def realtime(ticker: str) -> dict[str, Any]:
    # Deliberately separate from historical daily closes. We use Yahoo's market metadata
    # rather than the last historical candle, so a stale daily close cannot masquerade as live price.
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


def stats(close: list[float]) -> dict[str, Any]:
    if len(close) < 180:
        raise ValueError("历史数据不足，至少需要约180个交易日")
    latest = close[-1]
    sma20 = statistics.fmean(close[-20:])
    sma60 = statistics.fmean(close[-60:])
    r7 = latest / close[-8] - 1
    r30 = latest / close[-31] - 1
    daily_returns = [close[i] / close[i - 1] - 1 for i in range(len(close) - 20, len(close))]
    vol20 = statistics.pstdev(daily_returns)
    price_vs_s20 = latest / sma20 - 1
    s20_vs_s60 = sma20 / sma60 - 1
    score = 50 + 15 * math.tanh(price_vs_s20 / 0.03) + 15 * math.tanh(s20_vs_s60 / 0.04) + 12 * math.tanh(r30 / 0.10) + 8 * math.tanh(r7 / 0.05)
    score = max(0.0, min(100.0, score))
    stage = "上升趋势" if score >= 65 else "下降趋势" if score <= 35 else "震荡/过渡"

    matches: list[int] = []
    # Exclude the most recent 90 observations from candidate anchors so every reported
    # 90-day outcome has a complete forward path and no look-ahead leakage.
    for i in range(60, len(close) - 90):
        s20 = statistics.fmean(close[i - 19:i + 1])
        s60 = statistics.fmean(close[i - 59:i + 1])
        r30_hist = close[i] / close[i - 30] - 1
        rv = statistics.pstdev([close[j] / close[j - 1] - 1 for j in range(i - 19, i + 1)])
        if (
            abs(rv - vol20) / max(vol20, 1e-9) < 0.35
            and (close[i] > s20) == (latest > sma20)
            and (s20 > s60) == (sma20 > sma60)
            and (r30_hist > 0) == (r30 > 0)
        ):
            matches.append(i)

    horizons: dict[str, Any] = {}
    for h in (1, 3, 7, 30, 90):
        records = []
        for i in matches:
            if i + h >= len(close):
                continue
            start = close[i]
            if start <= 0:
                continue
            path = close[i:i + h + 1]
            ret = close[i + h] / start - 1
            records.append({"return": ret, "max_drawdown": max_drawdown(path)})
        if records:
            returns = [x["return"] for x in records]
            dds = [x["max_drawdown"] for x in records]
            n = len(returns)
            horizons[str(h)] = {
                "n": n,
                "up": sum(v > 0 for v in returns) / n,
                "down": sum(v < 0 for v in returns) / n,
                "flat": sum(v == 0 for v in returns) / n,
                "median": statistics.median(returns),
                "p25": percentile(returns, 0.25),
                "p75": percentile(returns, 0.75),
                "min": min(returns),
                "max": max(returns),
                "max_drawdown": min(dds),
                "reliability": reliability_level(n),
            }
        else:
            horizons[str(h)] = {"n": 0, "reliability": "不足", "note": "没有足够的历史相似样本"}

    n = len(matches)
    return {
        "trend_score": round(score, 2),
        "stage": stage,
        "sma20": round(sma20, 2),
        "sma60": round(sma60, 2),
        "return_7d": round(r7 * 100, 2),
        "return_30d": round(r30 * 100, 2),
        "sample_count": n,
        "reliability": reliability_level(n),
        "reliability_note": (
            "历史相似样本较充足，统计结果参考价值较高。" if n >= 50 else
            "历史相似样本中等，统计结果可以参考，但仍存在较大误差。" if n >= 20 else
            "历史相似样本偏少，统计结果仅作辅助参考。" if n >= 10 else
            "历史相似样本过少，不宜把统计结果作为主要判断依据。"
        ),
        "historical_stats": horizons,
    }


async def asset(ticker: str, include_sec: bool = False) -> dict[str, Any]:
    ticker = ticker.upper().strip()
    close, meta = await history(ticker)
    realtime_data = await realtime(ticker)
    result = {"ticker": ticker, **stats(close), "realtime": realtime_data, "history_last_close": close[-1], "updated_at": now_iso()}
    if include_sec:
        result["sec"] = await sec_latest(ticker)
    return result


async def scan_one(ticker: str) -> dict[str, Any]:
    try:
        item = await asset(ticker, include_sec=False)
        # Sample size is a reliability descriptor, not a score bonus. This avoids
        # converting a small/noisy sample into an artificial probability advantage.
        advantage = item["trend_score"]
        item["scan_rank_score"] = round(advantage, 2)
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
            "universe_size": len(SCAN_UNIVERSE),
        }
        semaphore = asyncio.Semaphore(8)

        async def limited(ticker: str) -> dict[str, Any]:
            async with semaphore:
                return await scan_one(ticker)

        results = await asyncio.gather(*(limited(t) for t in SCAN_UNIVERSE))
        good = [x for x in results if "error" not in x]
        good.sort(key=lambda x: (x["scan_rank_score"], x["sample_count"]), reverse=True)
        errors = [x for x in results if "error" in x]
        finished = datetime.now(timezone.utc)
        _scan_cache = {
            "status": "ready",
            "started_at": started.isoformat(),
            "completed_at": finished.isoformat(),
            "duration_seconds": round((finished - started).total_seconds(), 2),
            "results": good,
            "errors": errors,
            "top3": good[:3],
            "universe_size": len(SCAN_UNIVERSE),
            "successful_count": len(good),
            "failed_count": len(errors),
        }


async def scan_loop() -> None:
    while True:
        try:
            await run_scan()
        except Exception as exc:
            _scan_cache["status"] = "error"
            _scan_cache["errors"] = [{"ticker": "__scan__", "error": str(exc)}]
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def get_sec_ticker_map() -> dict[str, str]:
    global _sec_ticker_map
    if _sec_ticker_map is not None:
        return _sec_ticker_map
    async with _sec_lock:
        if _sec_ticker_map is not None:
            return _sec_ticker_map
        headers = {"User-Agent": "Lighthouse stock research contact@example.com"}
        async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
            r = await client.get(SEC_TICKERS_URL)
            r.raise_for_status()
            data = r.json()
        mapping: dict[str, str] = {}
        for row in data.values():
            if isinstance(row, dict) and row.get("ticker") and row.get("cik_str") is not None:
                mapping[str(row["ticker"]).upper()] = str(int(row["cik_str"])).zfill(10)
        _sec_ticker_map = mapping
        return mapping


async def sec_latest(ticker: str) -> dict[str, Any]:
    mapping = await get_sec_ticker_map()
    cik = mapping.get(ticker.upper())
    if not cik:
        return {"available": False, "note": "SEC 未找到该股票的 CIK 映射"}
    headers = {"User-Agent": "Lighthouse stock research contact@example.com"}
    async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
        r = await client.get(SEC_FACTS_BASE.format(cik=cik))
        r.raise_for_status()
        data = r.json()
    # Never take the last array item blindly. SEC facts can contain amendments,
    # different forms and multiple periods. Sort candidates by filing date first.
    candidates: list[dict[str, Any]] = []
    facts = data.get("facts", {})
    for taxonomy in facts.values():
        if not isinstance(taxonomy, dict):
            continue
        for concept, payload in taxonomy.items():
            units = payload.get("units", {}) if isinstance(payload, dict) else {}
            for unit, rows in units.items():
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, dict) and row.get("filed") and "val" in row:
                        candidates.append({
                            "concept": concept,
                            "unit": unit,
                            "val": row.get("val"),
                            "form": row.get("form"),
                            "filed": row.get("filed"),
                            "end": row.get("end"),
                            "accn": row.get("accn"),
                        })
    if not candidates:
        return {"available": False, "cik": cik, "note": "SEC 没有可用财务事实"}
    candidates.sort(key=lambda x: (x.get("filed", ""), x.get("end", ""), x.get("accn", "")), reverse=True)
    latest = candidates[0]
    return {"available": True, "cik": cik, "latest_filed_fact": latest, "source": "SEC EDGAR companyfacts"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _last_scan_task
    _last_scan_task = asyncio.create_task(scan_loop())
    yield
    if _last_scan_task:
        _last_scan_task.cancel()
        try:
            await _last_scan_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Lighthouse US Stocks", version=APP_VERSION, lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION, "scan": _scan_cache["status"], "updated_at": now_iso()}


@app.get("/api/asset")
async def api_asset(ticker: str = Query(..., min_length=1, max_length=15), sec: bool = False):
    try:
        return await asset(ticker, include_sec=sec)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})
    except httpx.HTTPError:
        return JSONResponse(status_code=502, content={"detail": "公开行情源暂时无法访问"})
    except Exception:
        return JSONResponse(status_code=500, content={"detail": "股票分析过程中发生内部错误"})


@app.get("/api/defaults")
async def api_defaults():
    results = []
    for ticker in DEFAULTS:
        results.append(await scan_one(ticker))
    return {"assets": results, "updated_at": now_iso()}


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
    return HTMLResponse(r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lighthouse US Stocks</title>
<style>
body{font-family:Arial,sans-serif;max-width:900px;margin:auto;padding:16px;background:#f5f7fa;color:#222}.card{background:#fff;border-radius:12px;padding:15px;margin:12px 0;box-shadow:0 2px 8px rgba(0,0,0,.08)}button,input{padding:10px;border-radius:8px;border:1px solid #ccc}button{cursor:pointer}.big{font-size:28px;font-weight:700}.muted{color:#666;font-size:13px}.error{color:#b00020}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}.rank{font-size:18px;font-weight:700}table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:7px;border-bottom:1px solid #eee;text-align:center}th:first-child,td:first-child{text-align:left}.scroll{overflow:auto}
</style></head><body>
<h1>🔦 Lighthouse US Stocks</h1><p>证据先行 · 中期为主 · 统计验证 · 不造数据</p>
<div class="card"><input id="ticker" value="AMD" maxlength="15" placeholder="输入股票代码"><button onclick="loadStock()">分析</button><button onclick="loadScan(true)">立即扫描</button></div>
<div id="scan" class="card">正在读取30只股票扫描结果……</div><div id="result"></div>
<script>
const esc=s=>String(s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function pct(x){return typeof x==='number'?(x*100).toFixed(1)+'%':'—'}
async function loadScan(refresh=false){try{let r=await fetch('/api/scan'+(refresh?'?refresh=true':''));let d=await r.json();let top=d.top3||[];document.getElementById('scan').innerHTML='<h2>Top 3 扫描结果</h2><p class="muted">扫描池 '+d.universe_size+' 只 · 成功 '+(d.successful_count??0)+' · 失败 '+(d.failed_count??0)+' · 后端每60秒完整扫描一次</p><div class="grid">'+top.map((x,i)=>`<div class="card"><div class="rank">#${i+1} ${esc(x.ticker)}</div><div class="big">${Number(x.realtime.price).toFixed(2)} USD</div><p>趋势：<b>${esc(x.stage)}</b></p><p>趋势评分：<b>${x.trend_score}</b>/100</p><p>7日 ${x.return_7d}% · 30日 ${x.return_30d}%</p><p>历史相似样本：${x.sample_count} · 可靠度：${esc(x.reliability)}</p></div>`).join('')+'</div><p class="muted">最后完成：'+(d.completed_at||'尚未完成')+'</p>'}catch(e){document.getElementById('scan').innerHTML='<span class="error">扫描结果暂时不可用</span>'}}
async function loadStock(){const t=document.getElementById('ticker').value.trim().toUpperCase();if(!t)return;const box=document.getElementById('result');box.innerHTML='<div class="card">正在获取历史统计与独立市场价格……</div>';try{let r=await fetch('/api/asset?ticker='+encodeURIComponent(t));let d=await r.json();if(!r.ok)throw Error(d.detail||'分析失败');let rt=d.realtime;let rows=Object.entries(d.historical_stats||{}).map(([h,s])=>`<tr><td>${h}日</td><td>${s.n??0}</td><td>${pct(s.up)}</td><td>${s.median==null?'—':pct(s.median)}</td><td>${s.p25==null?'—':pct(s.p25)} ～ ${s.p75==null?'—':pct(s.p75)}</td><td>${s.min==null?'—':pct(s.min)}</td><td>${s.max_drawdown==null?'—':pct(s.max_drawdown)}</td></tr>`).join('');box.innerHTML=`<div class="card"><h2>${esc(d.ticker)}</h2><div class="big">$${Number(rt.price).toFixed(2)}</div><p>市场状态：${esc(rt.market_state||'未知')} · 行情时间：${esc(rt.timestamp)}</p><p>趋势：<b>${esc(d.stage)}</b> · 趋势评分：<b>${d.trend_score}</b>/100</p><p>20日均线：$${d.sma20.toFixed(2)} · 60日均线：$${d.sma60.toFixed(2)}</p><p>7日收益：${d.return_7d}% · 30日收益：${d.return_30d}%</p><p>历史相似样本：<b>${d.sample_count}</b> · 统计可靠度：<b>${esc(d.reliability)}</b></p><p class="muted">${esc(d.reliability_note)}</p></div><div class="card"><h3>历史相似条件统计</h3><div class="scroll"><table><thead><tr><th>周期</th><th>样本</th><th>上涨</th><th>中位收益</th><th>常见区间</th><th>最差</th><th>最大回撤</th></tr></thead><tbody>${rows}</tbody></table></div><p class="muted">统计来自真实历史价格；样本不足时不会伪造概率。</p></div>`}catch(e){box.innerHTML='<div class="card error">'+esc(e.message)+'</div>'}}
loadScan();loadStock();setInterval(()=>loadScan(false),30000);
</script></body></html>''')
