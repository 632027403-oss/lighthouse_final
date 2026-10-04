# Lighthouse 最终收口版

本包故意拆成两个可独立部署的服务：

- `crypto_lighthouse`：加密货币，公开 Binance / OKX 数据，默认 BTC/ETH/SOL，并提供任意交易对搜索。
- `stock_lighthouse`：美股，使用公开 Yahoo Finance Chart 数据，默认 AMD/MRVL/NVDA，并提供任意股票代码搜索。

共同原则：真实数据优先；数据不足直接报错；不伪造概率；历史统计来自实际历史价格；中期趋势为主、短期动量辅助。

## Render 启动命令
加密：`uvicorn main:app --host 0.0.0.0 --port $PORT`
美股：`uvicorn main:app --host 0.0.0.0 --port $PORT`

分别把对应目录作为一个服务的代码根目录即可。

## API
`/api/asset?symbol=BTCUSDT`
`/api/scan`

## 重要说明
这一版优先解决“真实数据→历史统计→判断→手机展示”的核心链路，没有把 ETF、链上、宏观、新闻等容易造成接口不稳定的证据源硬塞进第一版。后续只有在核心服务稳定后才增加证据层。
