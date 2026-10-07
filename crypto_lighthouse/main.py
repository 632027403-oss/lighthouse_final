from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import asyncio, csv, io, math, re, statistics, time
from datetime import datetime, timezone
import httpx

app = FastAPI(title="Lighthouse Crypto", version="1.0-final")

CRYPTO = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","DOGEUSDT",
          "ADAUSDT","AVAXUSDT","LINKUSDT","SUIUSDT","LTCUSDT","BCHUSDT",
          "DOTUSDT","TRXUSDT","TONUSDT"]

BINANCE_SPOT = ["https://data-api.binance.vision","https://api-gcp.binance.com",
                "https://api1.binance.com","https://api2.binance.com",
                "https://api3.binance.com","https://api4.binance.com","https://api.binance.com"]
BINANCE_FUT = ["https://fapi.binance.com","https://fapi1.binance.com","https://fapi2.binance.com",
               "https://fapi3.binance.com","https://fapi4.binance.com"]
OKX = "https://www.okx.com"
COINBASE = "https://api.exchange.coinbase.com"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"
TREASURY = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
FARSIDE = "https://farside.co.uk/btc/"
UA = "LighthouseCrypto/1.0"

state = {"updated": 0, "assets": {}, "crypto_rank": [], "sources": {}, "macro": {},
         "etf": {}, "notes": [], "scan_status": "启动中"}
cache = {}


def ts(): return int(time.time() * 1000)

def iso(ms): return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()

def pct(a, b): return (a - b) / b * 100 if b else None

def safe_mean(xs): return statistics.mean(xs) if xs else None

async def get(c, url, params=None, timeout=12):
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


def max_drawdown(closes):
    peak = -float("inf"); worst = 0.0
    for x in closes:
        if x > peak: peak = x
        if peak > 0: worst = min(worst, (x - peak) / peak * 100)
    return round(worst, 2)

def window_max_drawdown(closes, window=90):
    if len(closes) < window + 1:
        return None
    return max_drawdown(closes[-(window + 1):])


def similar_stats(rows):
    if len(rows) < 180:
        return {"sample_count": 0, "status": "历史数据不足，无法建立相似条件样本"}
    closes = [r["close"] for r in rows]

    def feat(i):
        r7 = pct(closes[i], closes[i-7]) if i >= 7 else 0
        r30 = pct(closes[i], closes[i-30]) if i >= 30 else 0
        vol = rows[i]["volume"]
        prev = [r["volume"] for r in rows[max(0, i-20):i]]
        med = statistics.median(prev) if prev else vol
        return (1 if r7 > 2 else -1 if r7 < -2 else 0,
                1 if r30 > 8 else -1 if r30 < -8 else 0,
                1 if vol > med * 1.5 else 0)

    cur = feat(len(rows) - 1)
    sims = []
    forward_drawdowns = []
    # Leave 90 future days available; no look-ahead leakage.
    for i in range(30, len(rows) - 90):
        if sum(abs(a - b) for a, b in zip(cur, feat(i))) <= 1:
            base = closes[i]
            sims.append([(closes[i+h] - base) / base * 100 for h in (1, 3, 7, 30, 90)])
            forward_drawdowns.append(max_drawdown(closes[i:i+91]))
    out = {"sample_count": len(sims), "condition": list(cur)}
    for j, h in enumerate((1, 3, 7, 30, 90)):
        x = [s[j] for s in sims]
        if x:
            out[f"{h}d"] = {
                "up_rate": round(sum(v > 0 for v in x) / len(x) * 100, 1),
                "down_rate": round(sum(v < 0 for v in x) / len(x) * 100, 1),
                "median": round(statistics.median(x), 2),
                "avg": round(statistics.mean(x), 2),
                "worst": round(min(x), 2),
                "best": round(max(x), 2),
            }
    out["forward_90d_max_drawdown"] = round(min(forward_drawdowns), 2) if forward_drawdowns else None
    out["reliability"] = ("高" if len(sims) >= 50 else "中" if len(sims) >= 20 else "不足")
    out["status"] = "有效" if sims else "历史相似样本不足"
    return out


def historical_probability(stats):
    # Never show a probability unless there are at least 20 real historical matches.
    if not isinstance(stats, dict) or stats.get("sample_count", 0) < 20:
        return None
    weights = {"1d": .10, "3d": .10, "7d": .20, "30d": .30, "90d": .30}
    vals, ws = [], []
    for k, w in weights.items():
        x = stats.get(k)
        if isinstance(x, dict) and x.get("up_rate") is not None:
            vals.append(float(x["up_rate"])); ws.append(w)
    return round(sum(v*w for v, w in zip(vals, ws)) / sum(ws), 1) if vals else None


async def spot_binance(c, s):
    ticker, base, te = await first_json(c, BINANCE_SPOT, "/api/v3/ticker/24hr", {"symbol": s})
    depth, _, de = await first_json(c, BINANCE_SPOT, "/api/v3/depth", {"symbol": s, "limit": 50})
    trades, _, tre = await first_json(c, BINANCE_SPOT, "/api/v3/aggTrades", {"symbol": s, "limit": 1000})
    if not ticker:
        return {"available": False, "error": te or "ticker unavailable", "updated": ts()}
    out = {"available": True, "price": float(ticker["lastPrice"]),
           "change24h": float(ticker["priceChangePercent"]), "volume": float(ticker["quoteVolume"]),
           "source": "Binance spot", "endpoint": base, "updated": ts()}
    if depth:
        bid = sum(float(x[0])*float(x[1]) for x in depth.get("bids", []))
        ask = sum(float(x[0])*float(x[1]) for x in depth.get("asks", []))
        out["book_buy"] = bid/(bid+ask) if bid+ask else .5
    if trades:
        buy = sum(float(x.get("q", 0)) for x in trades if not x.get("m"))
        sell = sum(float(x.get("q", 0)) for x in trades if x.get("m"))
        out["aggressive_buy"] = buy/(buy+sell) if buy+sell else .5
    errors = [x for x in (de, tre) if x]
    if errors: out["partial_errors"] = errors
    return out

async def spot_okx(c, s):
    inst = s.replace("USDT", "-USDT")
    t = (await get(c, f"{OKX}/api/v5/market/ticker", {"instId": inst})).json()["data"][0]
    d = (await get(c, f"{OKX}/api/v5/market/books", {"instId": inst, "sz": "50"})).json()["data"][0]
    bid = sum(float(x[0])*float(x[1]) for x in d["bids"])
    ask = sum(float(x[0])*float(x[1]) for x in d["asks"])
    p, o = float(t["last"]), float(t["open24h"])
    return {"available": True, "price": p, "change24h": pct(p, o),
            "volume": float(t.get("volCcy24h", 0)), "book_buy": bid/(bid+ask) if bid+ask else .5,
            "source": "OKX spot", "updated": ts()}

async def spot_coinbase(c, s):
    product = s.replace("USDT", "-USD")
    t = (await get(c, f"{COINBASE}/products/{product}/ticker")).json()
    st = (await get(c, f"{COINBASE}/products/{product}/stats")).json()
    p, o = float(t["price"]), float(st["open"])
    return {"available": True, "price": p, "change24h": pct(p, o),
            "volume": float(st.get("volume", 0)), "source": "Coinbase spot", "updated": ts()}

async def derivatives(c, s):
    oi, ob, oe = await first_json(c, BINANCE_FUT, "/fapi/v1/openInterest", {"symbol": s})
    fu, fb, fe = await first_json(c, BINANCE_FUT, "/fapi/v1/premiumIndex", {"symbol": s})
    # Recent forced orders are an evidence layer, not a fake dollar-total if endpoint fails.
    li, _, le = await first_json(c, BINANCE_FUT, "/fapi/v1/allForceOrders", {"symbol": s, "limit": 100})
    out = {"available": bool(oi or fu), "source": "Binance Futures"}
    if oi: out["oi"] = float(oi["openInterest"])
    if fu:
        out["funding"] = float(fu["lastFundingRate"])
        out["mark"] = float(fu["markPrice"])
    if isinstance(li, list): out["liquidation_events"] = len(li)
    if not out["available"]: out["error"] = oe or fe or le or "derivatives unavailable"
    return out

async def historical(c, s):
    key = "hist:" + s
    cached = cache.get(key)
    if cached and ts() - cached["ts"] < 6 * 3600 * 1000:
        return cached["data"]
    k, base, err = await first_json(c, BINANCE_SPOT, "/api/v3/klines", {"symbol": s, "interval": "1d", "limit": 1000})
    source = base
    if not k:
        try:
            inst = s.replace("USDT", "-USDT")
            x = (await get(c, f"{OKX}/api/v5/market/candles", {"instId": inst, "bar": "1D", "limit": "300"})).json()
            data = list(reversed(x.get("data", [])))
            k = [[int(r[0]), 0, 0, 0, float(r[4]), float(r[5])] for r in data]
            source = "OKX"
        except Exception:
            data = {"rows": [], "stats": {"sample_count": 0, "status": "历史数据暂缺"}, "source": None, "max_drawdown_90d": None}
            cache[key] = {"ts": ts(), "data": data}
            return data
    rows = [{"ts": int(x[0]), "close": float(x[4]), "volume": float(x[5])} for x in k if len(x) >= 6]
    # Statistics use completed UTC daily candles only; the current live price stays in the real-time layer.
    day_start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    rows = [r for r in rows if r["ts"] < day_start]
    closes = [r["close"] for r in rows]
    data = {"rows": rows, "stats": similar_stats(rows),
            "max_drawdown_90d": window_max_drawdown(closes, 90),
            "max_drawdown_all": max_drawdown(closes), "source": source,
            "last_date": iso(rows[-1]["ts"]) if rows else None}
    cache[key] = {"ts": ts(), "data": data}
    return data


async def macro(c):
    cached = cache.get("macro")
    if cached and ts() - cached["ts"] < 10 * 60 * 1000:
        return cached["data"]
    async def fred_one(series):
        try:
            txt = (await get(c, FRED, {"id": series}, timeout=15)).text
            rows = [r for r in csv.DictReader(io.StringIO(txt)) if r.get(series) not in (None, "", ".")]
            if not rows: return None
            return {"value": float(rows[-1][series]), "date": rows[-1]["observation_date"], "source": "FRED"}
        except Exception: return None
    vals = await asyncio.gather(*(fred_one(x) for x in ("FEDFUNDS", "DGS10", "DTWEXBGS")))
    out = {}
    for k, v in zip(("Fed Funds", "10Y", "Dollar"), vals):
        if v: out[k] = v
    cache["macro"] = {"ts": ts(), "data": out}
    return out

async def etf(c):
    cached = cache.get("etf")
    if cached and ts() - cached["ts"] < 30 * 60 * 1000:
        return cached["data"]
    # Optional evidence. If the public page format changes, system reports unavailable rather than inventing a flow.
    try:
        html = (await get(c, FARSIDE, timeout=20)).text
        rows = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
            cells = [re.sub(r"<[^>]+>", "", z).strip() for z in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)]
            if len(cells) >= 3 and re.match(r"\d{2} \w{3} \d{4}", cells[0]):
                raw = cells[-1].replace(",", "").replace("(", "-").replace(")", "")
                try: rows.append({"date": cells[0], "total": float(raw)})
                except ValueError: pass
        out = {"available": bool(rows), "latest": rows[-1] if rows else None, "source": "Farside"}
        cache["etf"] = {"ts": ts(), "data": out}
        return out
    except Exception as e:
        out = {"available": False, "status": "暂缺", "error": str(e), "source": "Farside"}
        cache["etf"] = {"ts": ts(), "data": out}
        return out


def build_evidence(src, der, macro_data, hist, etf_data, symbol):
    pro, counter = [], []
    vals = [v for v in src.values() if v and v.get("available")]
    if vals:
        ps = [v["price"] for v in vals]
        dispersion = (max(ps)-min(ps))/statistics.median(ps)*100 if ps else 0
        pro.append({"name": "跨交易所价格一致性", "detail": f"价格离散 {dispersion:.3f}%",
                    "direction": "一致" if dispersion < .15 else "分歧"})
        buys = []
        for v in vals:
            for key in ("book_buy", "aggressive_buy"):
                if key in v: buys.append(v[key])
        bp = safe_mean(buys)
        if bp is not None:
            item = {"name": "现货买卖力量", "detail": f"买方占比 {bp*100:.1f}%",
                    "direction": "支持买方" if bp > .52 else "支持卖方" if bp < .48 else "中性"}
            (pro if bp >= .5 else counter).append(item)
    funding = der.get("funding")
    if funding is not None:
        item = {"name": "Funding Rate", "detail": f"{funding*100:.4f}%",
                "direction": "杠杆多头拥挤风险" if funding > .0005 else "杠杆偏空/反向燃料" if funding < -.0005 else "中性"}
        (counter if funding > .0005 else pro if funding < -.0005 else pro).append(item)
    if der.get("oi") is not None:
        pro.append({"name": "OI", "detail": f"{der['oi']:.4g}", "direction": "已接入，需与价格共同解释"})
    if der.get("liquidation_events") is not None:
        pro.append({"name": "强平事件", "detail": f"最近公开窗口事件数 {der['liquidation_events']}", "direction": "辅助风险证据"})
    if macro_data.get("10Y"):
        pro.append({"name": "美国10Y国债收益率", "detail": str(macro_data["10Y"]["value"]), "direction": "宏观背景"})
    if symbol == "BTCUSDT" and etf_data.get("available") and etf_data.get("latest"):
        x = etf_data["latest"]["total"]
        item = {"name": "BTC ETF净流量", "detail": f"{x:+.1f} US$m", "direction": "支持需求" if x > 0 else "需求转弱" if x < 0 else "中性"}
        (pro if x >= 0 else counter).append(item)
    return {"pro": pro, "counter": counter}


def make_asset(symbol, src, der, hist, macro_data, etf_data):
    vals = [v for v in src.values() if v and v.get("available")]
    if not vals: return {"symbol": symbol, "status": "公开现货数据暂缺", "sources": src}
    price = statistics.median([v["price"] for v in vals])
    changes = [v["change24h"] for v in vals if v.get("change24h") is not None]
    buy = []
    for v in vals:
        for key in ("book_buy", "aggressive_buy"):
            if key in v: buy.append(v[key])
    buy_power = safe_mean(buy) or .5
    rows = hist.get("rows", [])
    ret7 = pct(price, rows[-8]["close"]) if len(rows) >= 8 else safe_mean(changes)
    ret30 = pct(price, rows[-31]["close"]) if len(rows) >= 31 else ret7
    funding = der.get("funding")
    score = 50.0
    score += max(-18, min(18, (ret7 or 0) * 1.2))
    score += max(-24, min(24, (ret30 or 0) * .8))
    score += max(-8, min(8, (buy_power - .5) * 80))
    if funding is not None:
        score += -5 if funding > .0005 else 3 if funding < -.0005 else 0
    score = round(max(0, min(100, score)), 1)
    mid = "中期偏多" if score >= 62 else "中期偏空" if score <= 38 else "中期震荡/观察"
    short = "短期偏强" if (safe_mean(changes) or 0) > .5 else "短期偏弱" if (safe_mean(changes) or 0) < -.5 else "短期震荡"
    conflict = (ret7 or 0) * (ret30 or 0) < 0
    stats = hist.get("stats", {})
    prob = historical_probability(stats)
    return {
        "symbol": symbol, "status": "可分析", "sources": src,
        "price": price, "price_source": "多交易所现货中位数", "updated": ts(),
        "completeness": len(vals), "change24h": round(safe_mean(changes) or 0, 2),
        "buy_power": round(buy_power*100, 1), "ret7": round(ret7, 2) if ret7 is not None else None,
        "ret30": round(ret30, 2) if ret30 is not None else None,
        "trend": {"mid": mid, "short": short, "stage": "趋势形成/延续" if abs(ret30 or 0) >= 8 else "整理/待确认",
                  "score": score, "reversal_risk": "较高" if conflict else "中等" if abs(ret7 or 0) > 4 else "中低"},
        "derivatives": {k:v for k,v in der.items() if k != "source"},
        "history": {"stats": stats, "max_drawdown_90d": hist.get("max_drawdown_90d"),
                    "max_drawdown_all": hist.get("max_drawdown_all"), "source": hist.get("source"),
                    "last_date": hist.get("last_date")},
        "stat_probability": prob,
        "probability_basis": "历史相似条件加权上涨率（样本≥20）" if prob is not None else "样本不足，不显示统计概率",
        "evidence": build_evidence(src, der, macro_data, hist, etf_data, symbol),
    }

async def scan_one(c, symbol, macro_data, etf_data):
    async def safe(name, fn):
        try: return name, await fn(c, symbol)
        except Exception as e: return name, {"available": False, "error": str(e), "updated": ts()}
    pairs = await asyncio.gather(
        safe("binance", spot_binance), safe("okx", spot_okx), safe("coinbase", spot_coinbase),
        safe("derivatives", derivatives), safe("historical", historical)
    )
    src = {k: v for k, v in pairs[:3]}
    der = pairs[3][1]
    hist = pairs[4][1]
    return make_asset(symbol, src, der, hist, macro_data, etf_data)

async def full_scan():
    state["scan_status"] = "扫描中"
    async with httpx.AsyncClient(headers={"User-Agent": UA}) as c:
        macro_data, etf_data = await asyncio.gather(macro(c), etf(c), return_exceptions=True)
        if not isinstance(macro_data, dict): macro_data = {}
        if not isinstance(etf_data, dict): etf_data = {"available": False, "status": "暂缺"}
        sem = asyncio.Semaphore(5)
        async def one(s):
            async with sem:
                try: return await scan_one(c, s, macro_data, etf_data)
                except Exception as e: return {"symbol": s, "status": "扫描异常", "error": str(e)}
        assets = await asyncio.gather(*(one(s) for s in CRYPTO))
    assets = [a for a in assets if a.get("price") is not None]
    assets.sort(key=lambda a: a.get("trend", {}).get("score", -1), reverse=True)
    state["assets"] = {a["symbol"]: a for a in assets}
    state["crypto_rank"] = [a["symbol"] for a in assets]
    state["macro"] = macro_data
    state["etf"] = etf_data
    state["sources"] = {
        "Binance": any(a.get("sources", {}).get("binance", {}).get("available") for a in assets),
        "OKX": any(a.get("sources", {}).get("okx", {}).get("available") for a in assets),
        "Coinbase": any(a.get("sources", {}).get("coinbase", {}).get("available") for a in assets),
        "Binance Futures": any(a.get("derivatives", {}).get("available") for a in assets),
        "FRED": bool(macro_data), "BTC ETF / Farside": bool(etf_data.get("available")),
    }
    state["updated"] = ts(); state["scan_status"] = "完成"

async def loop():
    while True:
        try: await full_scan()
        except Exception as e:
            state["notes"].append({"time": ts(), "error": str(e)})
            state["scan_status"] = "扫描异常，保留上一轮有效结果"
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup(): asyncio.create_task(loop())

@app.get("/health")
async def health(): return {"status": "ok", "updated": state["updated"], "scan_status": state["scan_status"], "sources": state["sources"]}

@app.get("/api/state")
async def api_state(): return JSONResponse(state)

@app.get("/api/search")
async def search(symbol: str = Query(..., min_length=1, max_length=30)):
    q = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    if not q.endswith("USDT"): q += "USDT"
    if q in state["assets"]: return state["assets"][q]
    if q not in CRYPTO and not re.fullmatch(r"[A-Z0-9]{2,18}USDT", q):
        return {"status": "资产代码格式不受支持；请输入类似 BTC、ETH、SOL 或 BTCUSDT", "requested": symbol.upper()}
    async with httpx.AsyncClient(headers={"User-Agent": UA}) as c:
        try:
            macro_data, etf_data = await asyncio.gather(macro(c), etf(c), return_exceptions=True)
            if not isinstance(macro_data, dict): macro_data = {}
            if not isinstance(etf_data, dict): etf_data = {"available": False}
            result = await scan_one(c, q, macro_data, etf_data)
            if result.get("price") is None:
                return {"symbol": q, "status": "该资产当前没有足够的公开现货数据；不会假装已经分析"}
            return result
        except Exception as e:
            return {"symbol": q, "status": "查询失败", "error": str(e)}

HTML = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>🔦 灯塔 Crypto</title><style>
:root{--bg:#07111f;--card:#0d1a2b;--line:#223650;--txt:#edf5ff;--muted:#91a6bf}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:system-ui,-apple-system,"Segoe UI","Noto Sans SC",sans-serif}main{max-width:920px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;gap:10px;align-items:center}.brand{font-size:23px;font-weight:850}.sub,.small{font-size:11px;color:var(--muted);line-height:1.5}.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px;margin-top:10px}.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.rank{padding:10px 0;border-bottom:1px solid #20324a}.rank:last-child{border:0}.row{display:flex;justify-content:space-between;gap:12px;padding:7px 0;border-bottom:1px solid #20324a;font-size:12px}.row:last-child{border:0}.price{font-size:25px;font-weight:850}.tag{font-size:11px;background:#172941;border-radius:8px;padding:3px 6px;color:#bfd1e8}input,button{border:1px solid var(--line);background:#10243b;color:var(--txt);border-radius:11px;padding:9px}input{width:70%}@media(max-width:650px){.grid{grid-template-columns:1fr}}
</style></head><body><main><div class="top"><div><div class="brand">🔦 灯塔 Crypto</div><div class="sub">证据先行 · 中期为主 · 统计验证 · 不造数据</div></div><div id="status" class="small">启动扫描</div></div><div class="card"><input id="q" placeholder="输入 BTC / ETH / SOL…"><button onclick="searchA()">搜索</button><div class="small">只返回已接入公开数据；未覆盖资产不会假装已经分析。</div></div><div class="card"><b>数据完整度</b><div id="sources"></div><div class="small">多交易所、衍生品、宏观与ETF分别显示；缺失层不会被填成假数据。</div></div><div class="grid"><section class="card"><h2>🪙 Crypto Top 3</h2><div id="rank"></div></section><section class="card"><h2>宏观 / BTC机构需求</h2><div id="macro"></div><div id="etf"></div></section></div><div id="detail"></div><div class="card"><h2>统计规则</h2><div class="small">历史相似条件来自真实日线；输出1/3/7/30/90日上涨/下跌频率、中位收益、区间、最差收益与历史路径最大回撤。相似样本少于20时不显示统计概率。概率不是模型拍脑袋预测。</div></div></main><script>
const $=id=>document.getElementById(id),esc=s=>String(s??'—').replace(/[&<>\"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[c]));
function row(a,b){return `<div class="row"><span>${esc(a)}</span><b>${esc(b)}</b></div>`}function stat(s){if(!s||!s.sample_count)return '<div class="small">历史相似样本不足</div>';return ['1d','3d','7d','30d','90d'].map(k=>s[k]?row(k.toUpperCase(),`↑ ${s[k].up_rate}% / ↓ ${s[k].down_rate}% · 中位 ${s[k].median}% · 区间 ${s[k].worst}% ~ ${s[k].best}%`):'').join('')}
function render(x){$('status').textContent='最近更新 '+(x.updated?new Date(x.updated).toLocaleTimeString():'—')+' · '+x.scan_status;$('sources').innerHTML=Object.entries(x.sources||{}).map(([k,v])=>row(k,v?'可用':'暂缺')).join('');$('rank').innerHTML=(x.crypto_rank||[]).slice(0,3).map((s,i)=>{let a=x.assets[s],t=a.trend||{};return `<div class="rank"><b>${i+1}. ${esc(s.replace('USDT',''))}</b> <span class="tag">${esc(t.mid)}</span>${row('价格','$'+Number(a.price).toLocaleString(undefined,{maximumFractionDigits:2}))}${row('中期',t.mid)}${row('短期',t.short)}${row('趋势评分',t.score)}${row('7日',a.ret7+'%')}${row('30日',a.ret30+'%')}${row('统计概率',a.stat_probability==null?'样本不足':a.stat_probability+'%')}${row('历史样本',a.history?.stats?.sample_count||0)}${row('90日最大回撤',a.history?.max_drawdown_90d==null?'样本不足':a.history.max_drawdown_90d+'%')}</div>`}).join('');let m=x.macro||{};$('macro').innerHTML=Object.entries(m).map(([k,v])=>row(k,v?.value??'暂缺')).join('');let e=x.etf||{};$('etf').innerHTML=e.available?row('BTC ETF最近日',`${e.latest.date} · ${e.latest.total>0?'+':''}${e.latest.total} US$m`):row('BTC ETF','暂缺');}
async function load(){try{render(await fetch('/api/state',{cache:'no-store'}).then(r=>r.json()))}catch(e){$('status').textContent='连接异常'}}async function searchA(){let q=$('q').value.trim();if(!q)return;let d=await fetch('/api/search?symbol='+encodeURIComponent(q)).then(r=>r.json());$('detail').innerHTML='<div class="card"><h2>'+esc(q.toUpperCase())+'</h2>'+row('状态',d.status||'可分析')+(d.trend?row('中期',d.trend.mid)+row('短期',d.trend.short)+row('趋势评分',d.trend.score)+row('反转风险',d.trend.reversal_risk)+row('数据完整度',d.completeness+'/3')+row('统计概率',d.stat_probability==null?'样本不足':d.stat_probability+'%')+row('90日最大回撤',d.history.max_drawdown_90d==null?'样本不足':d.history.max_drawdown_90d+'%')+row('全历史最大回撤',d.history.max_drawdown_all==null?'样本不足':d.history.max_drawdown_all+'%')+stat(d.history.stats)+'<h3>正方证据</h3>'+d.evidence.pro.map(z=>'<div class="small">'+esc(z.name)+'：'+esc(z.detail)+'</div>').join('')+'<h3>反方证据</h3>'+d.evidence.counter.map(z=>'<div class="small">'+esc(z.name)+'：'+esc(z.detail)+'</div>').join(''):'')+'</div>'}load();setInterval(load,30000);
</script></body></html>'''

@app.get("/", response_class=HTMLResponse)
async def root(): return HTMLResponse(HTML)
