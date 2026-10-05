from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import asyncio, math, statistics, time
from datetime import datetime, timezone
import httpx

app = FastAPI(title='Lighthouse Crypto', version='1.0-final')

BINANCE='https://data-api.binance.vision'
OKX='https://www.okx.com'
COINBASE='https://api.exchange.coinbase.com'
DEFAULTS=['BTCUSDT','ETHUSDT','SOLUSDT']

async def get_json(client,url,params=None):
    r=await client.get(url,params=params,timeout=10)
    r.raise_for_status(); return r.json()

async def binance_klines(client,symbol,interval='1d',limit=180):
    return await get_json(client,f'{BINANCE}/api/v3/klines',{'symbol':symbol,'interval':interval,'limit':limit})

async def market(symbol):
    async with httpx.AsyncClient(headers={'User-Agent':'Lighthouse/1.0'}) as c:
        # Binance first, OKX fallback
        try:
            ticker=await get_json(c,f'{BINANCE}/api/v3/ticker/24hr',{'symbol':symbol})
            ks=await binance_klines(c,symbol,'1d',180)
            source='Binance'
        except Exception:
            inst=symbol.replace('USDT','-USDT')
            ticker=await get_json(c,f'{OKX}/api/v5/market/ticker',{'instId':inst})
            t=ticker['data'][0]
            ticker={'lastPrice':t['last'],'priceChangePercent':(float(t['last'])/float(t['open24h'])-1)*100,'volume':t.get('vol24h',0)}
            raw=await get_json(c,f'{OKX}/api/v5/market/candles',{'instId':inst,'bar':'1D','limit':'180'})
            ks=list(reversed(raw['data']))
            ks=[[x[0],x[1],x[2],x[3],x[4],x[5]] for x in ks]
            source='OKX'
        closes=[float(x[4]) for x in ks]
        highs=[float(x[2]) for x in ks]; lows=[float(x[3]) for x in ks]
        if len(closes)<40: raise ValueError('历史数据不足')
        ret1=closes[-1]/closes[-2]-1
        ret7=closes[-1]/closes[-8]-1
        ret30=closes[-1]/closes[-31]-1
        sma20=statistics.fmean(closes[-20:]); sma60=statistics.fmean(closes[-60:])
        vol20=statistics.pstdev([closes[i]/closes[i-1]-1 for i in range(len(closes)-20,len(closes))])
        drawdown=(closes[-1]/max(closes[-60:])-1)
        score=0
        score += 25 if closes[-1]>sma20 else -25
        score += 25 if sma20>sma60 else -25
        score += 20 if ret30>0 else -20
        score += 15 if ret7>0 else -15
        score += 15 if drawdown>-0.10 else -15
        score=max(0,min(100,50+score/2))
        stage='上升趋势' if score>=65 else ('下降趋势' if score<=35 else '震荡/过渡')
        # Similar-condition historical test: same direction of MA20/60, 30d return sign and volatility bucket.
        features=[]
        for i in range(60,len(closes)-1):
            c=closes[:i+1]; s20=statistics.fmean(c[-20:]); s60=statistics.fmean(c[-60:])
            r30=c[-1]/c[-31]-1
            rv=statistics.pstdev([c[j]/c[j-1]-1 for j in range(max(1,i-19),i+1)])
            features.append((i, c[-1]>s20, s20>s60, r30>0, rv))
        current_vol=vol20
        matches=[]
        for i,a,b,d,v in features:
            if a+90>=len(closes): continue
            if a+1>=len(closes): continue
            if (abs(v-current_vol)/max(current_vol,1e-9)<0.35 and a>=60 and (closes[a]>statistics.fmean(closes[a-19:a+1]))== (closes[-1]>sma20) and (statistics.fmean(closes[a-19:a+1])>statistics.fmean(closes[a-59:a+1]))==(sma20>sma60) and (closes[a]/closes[a-30]>1)==(ret30>0)):
                matches.append(a)
        horizons={}
        for h in (1,3,7,30,90):
            vals=[]
            for i in matches:
                if i+h < len(closes): vals.append(closes[i+h]/closes[i]-1)
            if vals:
                horizons[str(h)]={'n':len(vals),'up':sum(v>0 for v in vals)/len(vals),'down':sum(v<0 for v in vals)/len(vals),'median':statistics.median(vals)}
        return {'symbol':symbol,'source':source,'price':float(ticker['lastPrice']),'change24h':float(ticker['priceChangePercent']),'volume24h':float(ticker.get('volume',0)),'score':round(score,1),'stage':stage,'evidence':['价格高于20日均线' if closes[-1]>sma20 else '价格低于20日均线','20/60日均线关系支持'+('上行' if sma20>sma60 else '下行'),'近30日收益为'+('正' if ret30>0 else '负'),'近7日动量为'+('正' if ret7>0 else '负')],'counter':['近60日回撤超过10%' if drawdown<=-0.10 else '近60日回撤未超过10%'],'stats':{'sample_count':len(matches),'horizons':horizons},'updated':datetime.now(timezone.utc).isoformat()}

@app.get('/api/asset')
async def asset(symbol: str=Query('BTCUSDT')):
    try:
        return await market(symbol.upper())
    except Exception as e:
        return JSONResponse(status_code=503, content={'symbol':symbol.upper(),'status':'data_unavailable','message':'公开行情源暂时不可达，未生成判断','error':str(e)})

@app.get('/api/scan')
async def scan():
    out=[]
    for s in DEFAULTS:
        try: out.append(await market(s))
        except Exception as e: out.append({'symbol':s,'error':str(e)})
    return sorted(out,key=lambda x:x.get('score',0),reverse=True)

@app.get('/',response_class=HTMLResponse)
async def home():
    return HTMLResponse('''<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>灯塔·加密</title><style>body{font-family:system-ui;margin:0;background:#f5f6f8;color:#111}.wrap{max-width:760px;margin:auto;padding:18px}.card{background:white;border-radius:16px;padding:16px;margin:12px 0;box-shadow:0 2px 10px #0001}button{padding:10px 14px;border:0;border-radius:10px}input{padding:10px;width:65%}.muted{color:#666;font-size:13px}</style><div class="wrap"><h2>🔦 灯塔·加密</h2><div><input id=s value="BTCUSDT"><button onclick=load()>分析</button></div><div id=o class=card>等待真实数据…</div><script>async function load(){o.innerHTML='正在读取真实公开数据…';try{let x=await (await fetch('/api/asset?symbol='+s.value)).json();o.innerHTML='<h3>'+x.symbol+' · '+x.stage+'</h3><b>$'+x.price.toLocaleString()+'</b>　24h '+x.change24h.toFixed(2)+'%<p>判断分 '+x.score+'</p><b>主要证据</b><ul>'+x.evidence.map(a=>'<li>'+a+'</li>').join('')+'</ul><b>反证</b><ul>'+x.counter.map(a=>'<li>'+a+'</li>').join('')+'</ul><p>历史相似样本：'+x.stats.sample_count+'</p><p class=muted>来源：'+x.source+'；'+x.updated+'</p>'}catch(e){o.innerHTML='数据获取失败：'+e}}</script></div>''')
