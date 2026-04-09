#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""台股主動ETF爬蟲（GitHub Actions雲端版，無需Selenium）"""

import re, json, time, logging, csv, io
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE = Path(__file__).parent
DATA = BASE / "data"
HIST = DATA / "history"
DATA.mkdir(exist_ok=True)
HIST.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(BASE/"etf_crawler.log",encoding="utf-8"),logging.StreamHandler()])
log = logging.getLogger(__name__)

TW = timezone(timedelta(hours=8))
HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36","Accept-Language":"zh-TW,zh;q=0.9"}

ETF_CONFIG = [
    {"id":"00982A","name":"群益台灣強棒","manager":"群益投信","color":"#6b1a3a"},
    {"id":"00981A","name":"統一台股增長","manager":"統一投信","color":"#1a3a6b"},
    {"id":"00985A","name":"野村臺灣增強50","manager":"野村投信","color":"#1a5a6b"},
    {"id":"00980A","name":"野村臺灣優選","manager":"野村投信","color":"#1a6b3a"},
    {"id":"00984A","name":"安聯台灣高息","manager":"安聯投信","color":"#6b4a1a"},
]

def tofloat(s):
    try: return float(re.sub(r"[,，%\s]","",str(s)))
    except: return 0.0
def toint(s):
    try: return int(re.sub(r"[,，\s]","",str(s)))
    except: return 0
def is_code(s): return bool(re.match(r"^\d{4,6}[A-Za-z]?$",str(s).strip()))

def fetch(url,sess,timeout=25):
    for i in range(3):
        try:
            r=sess.get(url,headers=HEADERS,timeout=timeout)
            r.raise_for_status()
            r.encoding=r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            log.warning(f"  [{i+1}/3] {e}"); time.sleep(4*(i+1))
    return None

def parse_html(html):
    soup=BeautifulSoup(html,"lxml")
    best,bn=None,0
    for tbl in soup.find_all("table"):
        n=sum(1 for tr in tbl.find_all("tr") for td in tr.find_all(["td","th"])[:1] if is_code(td.get_text(strip=True)))
        if n>bn: bn,best=n,tbl
    if not best or bn<3: return []
    result=[]
    for tr in best.find_all("tr"):
        cells=[c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if not cells or not is_code(cells[0]): continue
        code=cells[0]; name=""; shares=weight=None
        for c in cells[1:]:
            c2=re.sub(r"[,，%\s*＊]","",c)
            if not name and not re.match(r"^[\d.]+$",c2): name=c; continue
            v=tofloat(c2)
            if v>500 and shares is None: shares=v; continue
            if 0<v<=100 and weight is None: weight=v; continue
        result.append({"code":code,"name":name or "—","shares":int(shares) if shares else 0,"weight":round(weight or 0,4)})
    return result

def parse_csv(text):
    result=[]; ci=ni=si=wi=-1; hdr=False
    for row in csv.reader(io.StringIO(text)):
        if not any(row): continue
        if not hdr:
            for i,c in enumerate(row):
                if re.search(r"代號|代碼",c): ci=i
                if re.search(r"名稱",c): ni=i
                if re.search(r"股數|張數",c): si=i
                if re.search(r"比重|權重|%",c): wi=i
            if ci>=0: hdr=True
            continue
        if ci<0 or ci>=len(row): continue
        code=row[ci].strip()
        if not is_code(code): continue
        result.append({"code":code,"name":row[ni].strip() if ni>=0 and ni<len(row) else "—",
            "shares":toint(row[si]) if si>=0 and si<len(row) else 0,
            "weight":tofloat(row[wi]) if wi>=0 and wi<len(row) else 0})
    return result

def crawl_00982A(sess):
    log.info("  [00982A] 群益官網...")
    html=fetch("https://www.capitalfund.com.tw/etf/product/detail/399/portfolio",sess)
    if not html: return []
    r=parse_html(html); log.info(f"  [00982A] {'OK' if r else 'FAIL'} {len(r)}檔"); return r

def crawl_00981A(sess):
    for url in ["https://www.ezmoney.com.tw/ETF/Transaction/PCFDownload?fundCode=49YTW"]:
        log.info(f"  [00981A] {url[:50]}...")
        raw=fetch(url,sess)
        if not raw: continue
        if raw.strip().startswith("<"):
            soup=BeautifulSoup(raw,"lxml")
            for a in soup.find_all("a",href=True):
                href=a["href"]
                if any(k in href.lower() for k in ["pcf","download",".csv"]):
                    dl=href if href.startswith("http") else "https://www.ezmoney.com.tw"+href
                    ct=fetch(dl,sess)
                    if ct:
                        r=parse_csv(ct)
                        if r: log.info(f"  [00981A] OK CSV {len(r)}檔"); return r
            r=parse_html(raw)
            if r: log.info(f"  [00981A] OK HTML {len(r)}檔"); return r
        else:
            r=parse_csv(raw)
            if r: log.info(f"  [00981A] OK CSV {len(r)}檔"); return r
    return crawl_twse(sess,"00981A")

def crawl_twse(sess,eid):
    today=datetime.now(TW).strftime("%Y%m%d")
    for url in [f"https://www.twse.com.tw/ETFortune/etfPCF?date={today}&stockNo={eid}&response=json",
                f"https://www.twse.com.tw/ETFortune/etfPCF?stockNo={eid}&response=json"]:
        log.info(f"  [{eid}] TWSE API...")
        raw=fetch(url,sess)
        if not raw: continue
        try:
            d=json.loads(raw)
            if d.get("stat")=="OK" and d.get("data"):
                fields=d.get("fields",[])
                ci=next((i for i,f in enumerate(fields) if "代號" in f or "代碼" in f),0)
                ni=next((i for i,f in enumerate(fields) if "名稱" in f),1)
                si=next((i for i,f in enumerate(fields) if "股數" in f or "張數" in f),2)
                wi=next((i for i,f in enumerate(fields) if "比重" in f or "%" in f),-1)
                result=[]
                for row in d["data"]:
                    code=str(row[ci]).strip()
                    if not is_code(code): continue
                    result.append({"code":code,"name":row[ni] if ni<len(row) else "—",
                        "shares":toint(row[si]) if si<len(row) else 0,
                        "weight":tofloat(row[wi]) if wi>=0 and wi<len(row) else 0})
                if result: log.info(f"  [{eid}] OK TWSE {len(result)}檔"); return result
        except: pass
    log.warning(f"  [{eid}] FAIL"); return []

CRAWLERS={"00982A":crawl_00982A,"00981A":crawl_00981A,
    "00985A":lambda s:crawl_twse(s,"00985A"),"00980A":lambda s:crawl_twse(s,"00980A"),
    "00984A":lambda s:crawl_twse(s,"00984A")}

def compare(today,yesterday):
    t={s["code"]:s for s in today}; y={s["code"]:s for s in yesterday}
    new_in,out_of,up,dn,same=[],[],[],[],[]
    for code,s in t.items():
        if code not in y: new_in.append({**s,"change_type":"new_in"})
        else:
            diff=round(s["weight"]-y[code]["weight"],4)
            e={**s,"prev_weight":y[code]["weight"],"weight_diff":diff}
            if diff>0.05: up.append(e)
            elif diff<-0.05: dn.append(e)
            else: same.append(e)
    for code,s in y.items():
        if code not in t: out_of.append({**s,"change_type":"out_of"})
    return {"new_in":sorted(new_in,key=lambda x:x["weight"],reverse=True),
        "out_of":out_of,
        "weight_up":sorted(up,key=lambda x:x["weight_diff"],reverse=True),
        "weight_dn":sorted(dn,key=lambda x:x["weight_diff"]),
        "same":sorted(same,key=lambda x:x["weight"],reverse=True)}

def load_yesterday(eid):
    snaps=sorted(HIST.glob(f"{eid}_*.json"))
    if not snaps: return []
    try: return json.loads(snaps[-1].read_text(encoding="utf-8")).get("holdings",[])
    except: return []

def save_snap(eid,holdings,date_str):
    p=HIST/f"{eid}_{date_str}.json"
    p.write_text(json.dumps({"date":date_str,"holdings":holdings},ensure_ascii=False,indent=2),encoding="utf-8")

def run():
    now=datetime.now(TW)
    log.info("="*50)
    log.info(f"台股主動ETF爬蟲（GHA版） {now:%Y-%m-%d %H:%M} TW")
    log.info("="*50)
    if now.weekday()>=5: log.info("非交易日，跳過。"); return
    date_str=now.strftime("%Y-%m-%d")
    sess=requests.Session()
    output={"generated_at":now.isoformat(),"date":date_str,"etf_list":ETF_CONFIG,"etfs":{}}
    for cfg in ETF_CONFIG:
        eid=cfg["id"]
        log.info(f"\n[{eid}] {cfg['name']}")
        today_h=[]
        try: today_h=CRAWLERS[eid](sess)
        except Exception as e: log.error(f"  [{eid}] {e}")
        yest_h=load_yesterday(eid)
        changes=compare(today_h,yest_h) if today_h and yest_h else {}
        if today_h: save_snap(eid,today_h,date_str)
        output["etfs"][eid]={"meta":cfg,"date":date_str,"holdings":today_h,"yesterday":yest_h,
            "changes":changes,"count":len(today_h),"success":bool(today_h)}
        time.sleep(2)
    (DATA/"etf_data.json").write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding="utf-8")
    (DATA/"last_update.txt").write_text(now.isoformat())
    ok=[k for k,v in output["etfs"].items() if v["success"]]
    err=[k for k,v in output["etfs"].items() if not v["success"]]
    log.info(f"\n完成！成功:{len(ok)}{ok} 失敗:{len(err)}{err}")

if __name__=="__main__":
    run()
