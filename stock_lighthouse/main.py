from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import statistics
from datetime import datetime, timezone
import httpx

app=FastAPI(title='Lighthouse US Stocks',version='1.0-final')
DEFAULTS=['AMD','MRVL','NVDA']
BASE='https://query1.finance.yahoo.com/v8/finance/chart/'

async def chart(ticker,range_='1y',interval='1d'):
    async with httpx.AsyncClient(headers={'User-Agent':'Mozilla/5.0'}) as c:
        r=await c.get(BASE+ticker,params={'range':range_,'interval':interval,'events':'div,splits'},timeout=12); r.raise_for_status(); return r.json()['chart']['result'][0]

def stats(close):
    if len(close)<90: raise ValueError('历史数据不足90个交易日')
    sma20=statistics.fmean(close[-20:]); sma60=statistics.fmean(close[-60:]); r7=close[-1]/close[-6]-1; r30=close[-1]/close[-31]-1
    vol20=statistics.pstdev([close[i]/close[i-1]-1 for i in range(len(close)-20,len(close))])
    score=50+(25 if close[-1]>sma20 else -25)+(25 if sma20>sma60 else -25)+(15 if r30>0 else -15)+(10 if r7>0 else -10)
    score=max(0,min(100,score))
    stage='上升趋势' if score>=65 else ('下降趋势' if score<=35 else '震荡/过渡')
    matches=[]
    for i in range(60,len(close)-90):
        s20=statistics.fmean(close[i-19:i+1]); s60=statistics.fmean(close[i-59:i+1]); r=close[i]/close[i-30]-1
        rv=statistics.pstdev([close[j]/close[j-1]-1 for j in range(i-19,i+1)])
        if abs(rv-vol20)/max(vol20,1e-9)<0.35 and (close[i]>s20)==(close[-1]>sma20) and (s20>s60)==(sma20>sma60) and (r>0)==(r30>0): matches.append(i)
    horizons={}
    for h in (1,3,7,30,90):
        vals=[close[i+h]/close[i]-1 for i in matches if i+h<len(close)]
        if vals: horizons[str(h)]={'n':len(vals),'up':sum(v>0 for v in vals)/len(vals),'down':sum(v<0 for v in vals)/len(vals),'median':statistics.median(vals)}
    return score,stage,sma20,sma60,r7,r30,horizons

async def asset(ticker):
    j=await chart(ticker.upper())
    q=j['indicators']['quote'][0]; close=[float(x) for x in q['close'] if x is not None]
    score,stage,s20,s60,r7,r30,h=stats(close)
    return {'symbol':ticker.upper(),'price':close[-1],'score':round(score,1),'stage':stage,'evidence':['价格高于20日均线' if close[-1]>s20 else '价格低于20日均线','20日均线位于60日均线之'+('上' if s20>s60 else '下'),'近30日收益为'+('正' if r30>0 else '负'),'近7日动量为'+('正' if r7>0 else '负')],'counter':['趋势与20日均线不一致' if (close[-1]>s20)!=(s20>s60) else '暂无明显结构性反证'],'stats':{'sample_count':(h.get('1',{}).get('n',0) if h else 0),'horizons':h},'updated':datetime.now(timezone.utc).isoformat(),'source':'Yahoo Finance chart API'}

@app.get('/api/asset')
async def api_asset(symbol:str=Query('AMD')):
    try:
        return await asset(symbol)
    except Exception as e:
        return JSONResponse(status_code=503, content={'symbol':symbol.upper(),'status':'data_unavailable','message':'公开行情源暂时不可达，未生成判断','error':str(e)})

@app.get('/api/scan')
async def scan():
    out=[]
    for s in DEFAULTS:
        try: out.append(await asset(s))
        except Exception as e: out.append({'symbol':s,'error':str(e)})
    return sorted(out,key=lambda x:x.get('score',0),reverse=True)

@app.get('/',response_class=HTMLResponse)
async def home():
    return HTMLResponse('''<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>灯塔·美股</title><style>body{font-family:system-ui;margin:0;background:#f5f6f8;color:#111}.wrap{max-width:760px;margin:auto;padding:18px}.card{background:white;border-radius:16px;padding:16px;margin:12px 0;box-shadow:0 2px 10px #0001}button{padding:10px 14px;border:0;border-radius:10px}input{padding:10px;width:65%}.muted{color:#666;font-size:13px}</style><div class="wrap"><h2>🔦 灯塔·美股</h2><div><input id=s value="AMD"><button onclick=load()>分析</button></div><div id=o class=card>等待真实数据…</div><script>async function load(){o.innerHTML='正在读取真实公开数据…';try{let x=await (await fetch('/api/asset?symbol='+s.value)).json();o.innerHTML='<h3>'+x.symbol+' · '+x.stage+'</h3><b>$'+x.price.toFixed(2)+'</b><p>判断分 '+x.score+'</p><b>主要证据</b><ul>'+x.evidence.map(a=>'<li>'+a+'</li>').join('')+'</ul><b>反证</b><ul>'+x.counter.map(a=>'<li>'+a+'</li>').join('')+'</ul><p>历史相似样本：'+x.stats.sample_count+'</p><p class=muted>来源：'+x.source+'；'+x.updated+'</p>'}catch(e){o.innerHTML='数据获取失败：'+e}}</script></div>''')
