# Lighthouse US Stocks

独立美股灯塔服务。

## 目录

本目录作为 `lighthouse_final/stock_lighthouse` 独立部署。

## 核心功能

- Yahoo 历史日线与市场价格分离；不使用历史 K 线冒充实时价格
- 任意美股代码查询
- 透明的 30 只股票扫描池
- 后端每 60 秒完整扫描一次
- 手机页面每 30 秒读取扫描结果
- Top 3 按趋势评分排序，不虚构概率
- 历史相似条件统计：1/3/7/30/90 日、样本数、上涨/下跌比例、中位收益、区间、最差收益、最大回撤
- 历史样本不足时明确标记，不造数据
- SEC EDGAR companyfacts 可选查询：按 filed/end/accession 排序后取最新记录，不盲取数组最后一条

## Render

若使用 Render 的 Root Directory 部署本目录，设置为 `stock_lighthouse`；构建：

`pip install -r requirements.txt`

启动：

`uvicorn main:app --host 0.0.0.0 --port $PORT`
