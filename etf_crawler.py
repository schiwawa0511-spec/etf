#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股主動ETF 每日持股爬蟲 v3（GitHub Actions版）
改用更可靠的資料來源：
  00982A → 群益官網（加強 headers）
  00981A → 統一投信官網持股頁面
  其餘   → 各投信官網持股頁面 + TWSE備援
"""

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(BASE / "etf_crawler.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
TW = timezone(timedelta(hours=8))

# 模擬真實瀏覽器的完整 headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

ETF_CONFIG = [
    {"id": "00982A", "name": "群益台灣強棒",   "manager": "群益投信", "color": "#6b1a3a"},
    {"id": "00981A", "name": "統一台股增長",   "manager": "統一投信", "color": "#1a3a6b"},
    {"id": "00985A", "name": "野村臺灣增強50", "manager": "野村投信", "color": "#1a5a6b"},
    {"id": "00980A", "name": "野村臺灣優選",   "manager": "野村投信", "color": "#1a6b3a"},
    {"id": "00984A", "name": "安聯台灣高息",   "manager": "安聯投信", "color": "#6b4a1a"},
]

def tofloat(s):
    try: return float(re.sub(r"[,，%\s]", "", str(s)))
    except: return 0.0

def toint(s):
    try: return int(re.sub(r"[,，\s]", "", str(s)))
    except: return 0

def is_code(s):
    return bool(re.match(r"^\d{4,6}[A-Za-z]?$", str(s).strip()))

def fetch(url, sess, timeout=30, referer=None):
    h = dict(HEADERS)
    if referer:
        h["Referer"] = referer
    for i in range(3):
        try:
            r = sess.get(url, headers=h, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            log.info(f"    GET {url[:80]} → {r.status_code}")
            return r.text
        except Exception as e:
            log.warning(f"    [{i+1}/3] {e}")
            time.sleep(5 * (i + 1))
    return None

def parse_html(html):
    """從 HTML 找最像持股的表格"""
    soup = BeautifulSoup(html, "lxml")
    best, bn = None, 0
    for tbl in soup.find_all("table"):
        n = sum(
            1 for tr in tbl.find_all("tr")
            for td in tr.find_all(["td","th"])[:1]
            if is_code(td.get_text(strip=True))
        )
        if n > bn:
            bn, best = n, tbl
    if not best or bn < 3:
        return []
    result = []
    for tr in best.find_all("tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if not cells or not is_code(cells[0]): continue
        code = cells[0]
        name = ""
        shares = weight = None
        for c in cells[1:]:
            c2 = re.sub(r"[,，%\s*＊]", "", c)
            if not name and not re.match(r"^[\d.]+$", c2): name = c; continue
            v = tofloat(c2)
            if v > 500 and shares is None: shares = v; continue
            if 0 < v <= 100 and weight is None: weight = v; continue
        result.append({
            "code": code, "name": name or "—",
            "shares": int(shares) if shares else 0,
            "weight": round(weight or 0, 4),
        })
    return result

def parse_csv(text):
    result = []
    ci = ni = si = wi = -1
    hdr = False
    for row in csv.reader(io.StringIO(text)):
        if not any(row): continue
        if not hdr:
            for i, c in enumerate(row):
                if re.search(r"代號|代碼", c): ci = i
                if re.search(r"名稱", c): ni = i
                if re.search(r"股數|張數", c): si = i
                if re.search(r"比重|權重|%", c): wi = i
            if ci >= 0: hdr = True
            continue
        if ci < 0 or ci >= len(row): continue
        code = row[ci].strip()
        if not is_code(code): continue
        result.append({
            "code": code,
            "name": row[ni].strip() if ni >= 0 and ni < len(row) else "—",
            "shares": toint(row[si]) if si >= 0 and si < len(row) else 0,
            "weight": tofloat(row[wi]) if wi >= 0 and wi < len(row) else 0,
        })
    return result

# ═══════════════════════════════════════════════════════
# 各 ETF 爬蟲
# ═══════════════════════════════════════════════════════

def crawl_00982A(sess):
    """群益 — 加 Referer header"""
    eid = "00982A"
    url = "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    log.info(f"  [{eid}] 群益官網...")
    # 先訪問首頁建立 session cookie
    fetch("https://www.capitalfund.com.tw/etf", sess)
    time.sleep(2)
    html = fetch(url, sess, referer="https://www.capitalfund.com.tw/etf/product/overview")
    if not html:
        return []
    r = parse_html(html)
    log.info(f"  [{eid}] {'OK' if r else 'FAIL'} {len(r)}檔")
    return r

def crawl_00981A(sess):
    """統一 — 直接抓持股頁"""
    eid = "00981A"
    # 統一投信持股頁面（不同於 PCF 下載）
    urls = [
        "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW",
        "https://www.ezmoney.com.tw/ETF/Fund/Portfolio?fundCode=49YTW",
    ]
    for url in urls:
        log.info(f"  [{eid}] {url[:60]}...")
        fetch("https://www.ezmoney.com.tw/ETF/", sess)
        time.sleep(2)
        html = fetch(url, sess, referer="https://www.ezmoney.com.tw/ETF/")
        if not html: continue
        r = parse_html(html)
        if r:
            log.info(f"  [{eid}] OK {len(r)}檔")
            return r
    # 備援：基金資訊觀測站
    return crawl_fundclear(sess, eid, "49YTW")

def crawl_00985A(sess):
    return crawl_nomura(sess, "00985A")

def crawl_00980A(sess):
    return crawl_nomura(sess, "00980A")

def crawl_nomura(sess, eid):
    """野村 — 抓官網持股頁"""
    urls = [
        f"https://www.nomurafunds.com.tw/ETFWEB/api/fund/portfolio?fundNo={eid}",
        f"https://www.nomurafunds.com.tw/ETFWEB/product-description?fundNo={eid}&tab=Shareholding",
        f"https://money.nomurafunds.com.tw/etf/{eid}/holding",
    ]
    fetch("https://www.nomurafunds.com.tw/", sess)
    time.sleep(2)
    for url in urls:
        log.info(f"  [{eid}] {url[:70]}...")
        html = fetch(url, sess, referer="https://www.nomurafunds.com.tw/ETFWEB/")
        if not html: continue
        # 先試 JSON
        try:
            d = json.loads(html)
            r = parse_nomura_json(d, eid)
            if r: return r
        except: pass
        # 再試 HTML 表格
        r = parse_html(html)
        if r:
            log.info(f"  [{eid}] OK {len(r)}檔")
            return r
    return crawl_fundclear(sess, eid)

def parse_nomura_json(d, eid):
    """解析野村 API JSON 格式"""
    result = []
    # 可能的欄位名稱
    items = d.get("data") or d.get("list") or d.get("holdings") or []
    if not isinstance(items, list): return []
    for item in items:
        if not isinstance(item, dict): continue
        code = str(item.get("stockCode") or item.get("code") or item.get("securityCode") or "").strip()
        if not is_code(code): continue
        result.append({
            "code": code,
            "name": item.get("stockName") or item.get("name") or "—",
            "shares": toint(item.get("shares") or item.get("quantity") or 0),
            "weight": tofloat(item.get("weight") or item.get("ratio") or item.get("proportion") or 0),
        })
    if result:
        log.info(f"  [{eid}] OK JSON {len(result)}檔")
    return result

def crawl_00984A(sess):
    """安聯 — 嘗試多個 URL"""
    eid = "00984A"
    urls = [
        "https://www.allianzgi.com.tw/zh-tw/individual/funds-etf/active-etf/00984A/portfolio",
        "https://www.allianzgi.com.tw/zh-tw/funds/etf/00984A",
        "https://www.allianzgi.com.tw/zh-tw/individual/funds-etf/active-etf/00984A",
    ]
    fetch("https://www.allianzgi.com.tw/zh-tw/", sess)
    time.sleep(2)
    for url in urls:
        log.info(f"  [{eid}] {url[:70]}...")
        html = fetch(url, sess, referer="https://www.allianzgi.com.tw/zh-tw/")
        if not html: continue
        r = parse_html(html)
        if r:
            log.info(f"  [{eid}] OK {len(r)}檔")
            return r
    return crawl_fundclear(sess, eid)

def crawl_fundclear(sess, eid, fund_code=None):
    """
    基金資訊觀測站備援
    主動式ETF依法每日申報持股，可從此查詢
    """
    fc = fund_code or eid
    log.info(f"  [{eid}] 基金資訊觀測站備援...")
    urls = [
        f"https://announce.fundclear.com.tw/MOPSonshoreFundWeb/A01_02.jsp?fundId={fc}",
        f"https://www.fundclear.com.tw/etf/product?fundId={eid}",
    ]
    for url in urls:
        html = fetch(url, sess, referer="https://www.fundclear.com.tw/")
        if not html: continue
        r = parse_html(html)
        if r:
            log.info(f"  [{eid}] OK fundclear {len(r)}檔")
            return r
    log.warning(f"  [{eid}] 全部失敗")
    return []

# ═══════════════════════════════════════════════════════
# 比對 & 快照
# ═══════════════════════════════════════════════════════

def compare(today, yesterday):
    t = {s["code"]: s for s in today}
    y = {s["code"]: s for s in yesterday}
    new_in, out_of, up, dn, same = [], [], [], [], []
    for code, s in t.items():
        if code not in y:
            new_in.append({**s, "change_type": "new_in"})
        else:
            diff = round(s["weight"] - y[code]["weight"], 4)
            e = {**s, "prev_weight": y[code]["weight"], "weight_diff": diff}
            if diff > 0.05: up.append(e)
            elif diff < -0.05: dn.append(e)
            else: same.append(e)
    for code, s in y.items():
        if code not in t:
            out_of.append({**s, "change_type": "out_of"})
    return {
        "new_in":    sorted(new_in, key=lambda x: x["weight"], reverse=True),
        "out_of":    out_of,
        "weight_up": sorted(up, key=lambda x: x["weight_diff"], reverse=True),
        "weight_dn": sorted(dn, key=lambda x: x["weight_diff"]),
        "same":      sorted(same, key=lambda x: x["weight"], reverse=True),
    }

def load_yesterday(eid):
    snaps = sorted(HIST.glob(f"{eid}_*.json"))
    if not snaps: return []
    try: return json.loads(snaps[-1].read_text(encoding="utf-8")).get("holdings", [])
    except: return []

def save_snap(eid, holdings, date_str):
    p = HIST / f"{eid}_{date_str}.json"
    p.write_text(
        json.dumps({"date": date_str, "holdings": holdings}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

# ═══════════════════════════════════════════════════════
# 主程式
# ═══════════════════════════════════════════════════════

CRAWLERS = {
    "00982A": crawl_00982A,
    "00981A": crawl_00981A,
    "00985A": crawl_00985A,
    "00980A": crawl_00980A,
    "00984A": crawl_00984A,
}

def run():
    now = datetime.now(TW)
    log.info("=" * 55)
    log.info(f"台股主動ETF爬蟲 v3  {now:%Y-%m-%d %H:%M} TW")
    log.info("=" * 55)
    if now.weekday() >= 5:
        log.info("非交易日，跳過。"); return

    date_str = now.strftime("%Y-%m-%d")
    sess = requests.Session()
    output = {
        "generated_at": now.isoformat(),
        "date": date_str,
        "etf_list": ETF_CONFIG,
        "etfs": {},
    }

    for cfg in ETF_CONFIG:
        eid = cfg["id"]
        log.info(f"\n{'─'*40}\n[{eid}] {cfg['name']}")
        today_h = []
        try:
            today_h = CRAWLERS[eid](sess)
        except Exception as e:
            log.error(f"  [{eid}] 例外: {e}")

        yest_h = load_yesterday(eid)
        changes = compare(today_h, yest_h) if today_h and yest_h else {}
        if today_h:
            save_snap(eid, today_h, date_str)

        output["etfs"][eid] = {
            "meta": cfg, "date": date_str,
            "holdings": today_h, "yesterday": yest_h,
            "changes": changes, "count": len(today_h),
            "success": bool(today_h),
        }
        log.info(
            f"  [{eid}] {'✓' if today_h else '✗'} {len(today_h)}檔  "
            f"新:{len(changes.get('new_in',[]))} 出:{len(changes.get('out_of',[]))} "
            f"加:{len(changes.get('weight_up',[]))} 減:{len(changes.get('weight_dn',[]))}"
        )
        time.sleep(3)

    (DATA / "etf_data.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA / "last_update.txt").write_text(now.isoformat())

    ok  = [k for k, v in output["etfs"].items() if v["success"]]
    err = [k for k, v in output["etfs"].items() if not v["success"]]
    log.info(f"\n{'='*55}")
    log.info(f"完成！成功:{len(ok)} {ok}")
    if err: log.warning(f"失敗:{len(err)} {err}")
    log.info("=" * 55)

if __name__ == "__main__":
    run()
