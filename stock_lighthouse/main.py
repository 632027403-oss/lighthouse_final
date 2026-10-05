from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import statistics
import math
import re
from datetime import datetime, timezone
import httpx

app = FastAPI(title="Lighthouse US Stocks", version="1.1-final")

DEFAULTS = ["AMD", "MRVL", "NVDA"]
BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"


async def chart(ticker, range_="1y", interval="1d"):
    ticker = ticker.upper().strip()

    if not re.fullmatch(r"[A-Z0-9.\-^=]{1,15}", ticker):
        raise ValueError("股票代码格式无效")

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0"},
        follow_redirects=True
    ) as c:
        r = await c.get(
            BASE + ticker,
            params={
                "range": range_,
                "interval": interval,
                "events": "div,splits"
            },
            timeout=12
        )
        r.raise_for_status()

        data = r.json()
        chart_data = data.get("chart", {})
        result = chart_data.get("result")

        if not result or not isinstance(result, list):
            raise ValueError("公开行情源未返回有效数据")

        item = result[0]

        if not isinstance(item, dict):
            raise ValueError("公开行情数据结构异常")

        if "indicators" not in item:
            raise ValueError("公开行情数据缺少价格字段")

        return item


def percentile(values, p):
    if not values:
        return None

    values = sorted(values)

    if len(values) == 1:
        return values[0]

    pos = (len(values) - 1) * p
    lower = math.floor(pos)
    upper = math.ceil(pos)

    if lower == upper:
        return values[lower]

    weight = pos - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def max_drawdown(values):
    """
    计算一段价格路径中的最大回撤。
    返回负数，例如 -0.12 表示最大回撤约12%。
    """
    if len(values) < 2:
        return 0.0

    peak = values[0]
    worst = 0.0

    for value in values[1:]:
        if value > peak:
            peak = value

        if peak > 0:
            drawdown = value / peak - 1
            if drawdown < worst:
                worst = drawdown

    return worst


def reliability_level(n):
    if n >= 50:
        return "高"
    if n >= 20:
        return "中"
    if n >= 10:
        return "低"
    return "不足"


def stats(close):
    if len(close) < 180:
        raise ValueError("历史数据不足，至少需要约180个交易日")

    latest = close[-1]

    sma20 = statistics.fmean(close[-20:])
    sma60 = statistics.fmean(close[-60:])

    # 真正的7个交易日间隔
    r7 = latest / close[-8] - 1

    r30 = latest / close[-31] - 1

    daily_returns = [
        close[i] / close[i - 1] - 1
        for i in range(len(close) - 20, len(close))
    ]

    vol20 = statistics.pstdev(daily_returns)

    # ---------------------------------------------------------
    # 连续型趋势评分
    # 不再使用大量固定 +25/-25，避免分数长期挤在0或100。
    # ---------------------------------------------------------
    price_vs_s20 = latest / sma20 - 1
    s20_vs_s60 = sma20 / sma60 - 1

    score = (
        50
        + 15 * math.tanh(price_vs_s20 / 0.03)
        + 15 * math.tanh(s20_vs_s60 / 0.04)
        + 12 * math.tanh(r30 / 0.10)
        + 8 * math.tanh(r7 / 0.05)
    )

    score = max(0.0, min(100.0, score))

    if score >= 65:
        stage = "上升趋势"
    elif score <= 35:
        stage = "下降趋势"
    else:
        stage = "震荡/过渡"

    # ---------------------------------------------------------
    # 历史相似条件
    #
    # 条件：
    # 1. 波动率相近
    # 2. 价格与20日均线关系相近
    # 3. 20日/60日均线结构相近
    # 4. 30日收益方向相近
    # ---------------------------------------------------------
    matches = []

    for i in range(60, len(close) - 90):
        s20 = statistics.fmean(close[i - 19:i + 1])
        s60 = statistics.fmean(close[i - 59:i + 1])

        r30_hist = close[i] / close[i - 30] - 1

        rv = statistics.pstdev([
            close[j] / close[j - 1] - 1
            for j in range(i - 19, i + 1)
        ])

        volatility_match = (
            abs(rv - vol20) / max(vol20, 1e-9) < 0.35
        )

        price_structure_match = (
            (close[i] > s20) == (latest > sma20)
        )

        moving_average_structure_match = (
            (s20 > s60) == (sma20 > sma60)
        )

        momentum_match = (
            (r30_hist > 0) == (r30 > 0)
        )

        if (
            volatility_match
            and price_structure_match
            and moving_average_structure_match
            and momentum_match
        ):
            matches.append(i)

    horizons = {}

    for h in (1, 3, 7, 30, 90):
        records = []

        for i in matches:
            if i + h >= len(close):
                continue

            start_price = close[i]
            end_price = close[i + h]

            if start_price <= 0:
                continue

            ret = end_price / start_price - 1

            # 从相似条件出现之后，直到h日的价格路径
            path = close[i:i + h + 1]

            records.append({
                "return": ret,
                "max_drawdown": max_drawdown(path)
            })

        if records:
            returns = [x["return"] for x in records]
            drawdowns = [x["max_drawdown"] for x in records]

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

                "max_drawdown": min(drawdowns),

                "reliability": reliability_level(n)
            }

    sample_count = len(matches)

    if sample_count >= 50:
        reliability = "高"
        reliability_note = "历史相似样本较充足，统计结果参考价值较高。"
    elif sample_count >= 20:
        reliability = "中"
        reliability_note = "历史相似样本中等，统计结果可以参考，但仍存在较大误差。"
    elif sample_count >= 10:
        reliability = "低"
        reliability_note = "历史相似样本偏少，统计结果仅作辅助参考。"
    else:
        reliability = "不足"
        reliability_note = "历史相似样本过少，不宜把统计结果作为主要判断依据。"

    return (
        score,
        stage,
        sma20,
        sma60,
        r7,
        r30,
        horizons,
        sample_count,
        reliability,
        reliability_note
    )


async def asset(ticker):
    ticker = ticker.upper().strip()

    j = await chart(ticker)

    try:
        q = j["indicators"]["quote"][0]
        raw_close = q.get("close", [])
    except (KeyError, IndexError, TypeError):
        raise ValueError("公开行情数据结构异常")

    close = [
        float(x)
        for x in raw_close
        if x is not None
    ]

    if len(close) < 180:
        raise ValueError("历史价格数据不足，无法进行可靠统计")

    (
        score,
        stage,
        s20,
        s60,
        r7,
        r30,
        horizons,
        sample_count,
        reliability,
        reliability_note
        ) = stats(close)
    latest = close[-1]

    return {
        "ticker": ticker,
        "price": latest,
        "trend_score": round(score, 2),
        "stage": stage,
        "sma20": round(s20, 2),
        "sma60": round(s60, 2),
        "return_7d": round(r7 * 100, 2),
        "return_30d": round(r30 * 100, 2),
        "sample_count": sample_count,
        "reliability": reliability,
        "reliability_note": reliability_note,
        "historical_stats": horizons,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }


@app.get("/")
async def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lighthouse US Stocks</title>
<style>
body {
    font-family: Arial, sans-serif;
    max-width: 760px;
    margin: 0 auto;
    padding: 20px;
    background: #f5f7fa;
    color: #222;
}
h1 { margin-bottom: 8px; }
.card {
    background: white;
    border-radius: 12px;
    padding: 16px;
    margin: 12px 0;
    box-shadow: 0 2px 8px rgba(0,0,0,.08);
}
button {
    padding: 10px 16px;
    border: 0;
    border-radius: 8px;
    cursor: pointer;
}
input {
    padding: 10px;
    width: 180px;
    border: 1px solid #ccc;
    border-radius: 8px;
}
.error { color: #b00020; }
.big { font-size: 28px; font-weight: bold; }
</style>
</head>

<body>

<h1>🔦 Lighthouse US Stocks</h1>
<p>证据先行 · 中期为主 · 统计验证 · 不造数据</p>

<div class="card">
    <input id="ticker" value="AMD" placeholder="输入股票代码">
    <button onclick="loadStock()">分析</button>
</div>

<div id="result"></div>

<script>
async function loadStock() {
    const ticker = document.getElementById("ticker").value.trim().toUpperCase();

    if (!ticker) return;

    const box = document.getElementById("result");
    box.innerHTML = "<div class='card'>正在获取公开行情并计算统计……</div>";

    try {
        const response = await fetch(
            "/api/asset?ticker=" + encodeURIComponent(ticker)
        );

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || "分析失败");
        }

        box.innerHTML = `
        <div class="card">
            <h2>${data.ticker}</h2>
            <div class="big">$${data.price.toFixed(2)}</div>

            <p>趋势：<b>${data.stage}</b></p>
            <p>趋势评分：<b>${data.trend_score}</b> / 100</p>

            <p>20日均线：$${data.sma20.toFixed(2)}</p>
            <p>60日均线：$${data.sma60.toFixed(2)}</p>

            <p>7日收益：${data.return_7d}%</p>
            <p>30日收益：${data.return_30d}%</p>

            <hr>

            <p>历史相似样本：<b>${data.sample_count}</b></p>
            <p>统计可靠度：<b>${data.reliability}</b></p>
            <p>${data.reliability_note}</p>
        </div>

        <div class="card">
    <h3>历史相似条件统计</h3>
    <div style="overflow-x:auto;">
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
                <tr>
                    <th style="padding:8px;text-align:left;">周期</th>
                    <th style="padding:8px;">样本</th>
                    <th style="padding:8px;">上涨概率</th>
                    <th style="padding:8px;">中位收益</th>
                    <th style="padding:8px;">常见区间</th>
                    <th style="padding:8px;">最差收益</th>
                    <th style="padding:8px;">最大回撤</th>
                </tr>
            </thead>
            <tbody>
                ${Object.entries(data.historical_stats || {}).map(([h, s]) => `
                    <tr>
                        <td style="padding:8px;">${h}日</td>
                        <td style="padding:8px;text-align:center;">${s.n}</td>
                        <td style="padding:8px;text-align:center;">${(s.up * 100).toFixed(1)}%</td>
                        <td style="padding:8px;text-align:center;">${(s.median * 100).toFixed(1)}%</td>
                        <td style="padding:8px;text-align:center;">
                            ${(s.p25 * 100).toFixed(1)}% ～ ${(s.p75 * 100).toFixed(1)}%
                        </td>
                        <td style="padding:8px;text-align:center;">${(s.min * 100).toFixed(1)}%</td>
                        <td style="padding:8px;text-align:center;">${(s.max_drawdown * 100).toFixed(1)}%</td>
                    </tr>
                `).join("")}
            </tbody>
        </table>
    </div>

    <p style="font-size:13px;color:#777;margin-top:12px;">
        以上为历史上与当前条件相似时的统计结果，仅用于辅助判断，不代表未来必然结果。
    </p>
</div>
        `;

    } catch (error) {
        box.innerHTML =
            "<div class='card error'>" +
            error.message +
            "</div>";
    }
}

async function loadDefault(ticker) {
    document.getElementById("ticker").value = ticker;
    await loadStock();
}

loadDefault("AMD");
</script>

</body>
</html>
""")


@app.get("/api/asset")
async def api_asset(
    ticker: str = Query(..., min_length=1, max_length=15)
):
    try:
        return await asset(ticker)
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"detail": str(e)}
        )
    except httpx.HTTPError:
        return JSONResponse(
            status_code=502,
            content={"detail": "公开行情源暂时无法访问"}
        )
    except Exception:
        return JSONResponse(
            status_code=500,
            content={"detail": "股票分析过程中发生内部错误"}
        )


@app.get("/api/defaults")
async def api_defaults():
    results = []

    for ticker in DEFAULTS:
        try:
            results.append(await asset(ticker))
        except Exception as e:
            results.append({
                "ticker": ticker,
                "error": str(e)
            })

    return {
        "assets": results,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
