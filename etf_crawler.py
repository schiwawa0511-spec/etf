#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股主動ETF 每日持股爬蟲 v4（GitHub Actions版）

核心策略：改用公開資訊觀測站（mops.twse.com.tw）
各投信依法每日必須向公開資訊觀測站申報持股，
此為政府網站，不會封鎖 GitHub Actions。

備援：群益官網（靜態HTML，較穩定）
"""

import re, json, time, logging
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

ETF_CONFIG = [
    {"id": "00982A", "name": "群益台灣強棒",   "manager": "群益投信", "color": "#6b1a3a"},
    {"id": "00981A", "name": "統一台股增長",   "manager": "統一投信", "color": "#1a3a6b"},
    {"id": "00985A", "name": "野村臺灣增強50", "manager": "野村投信", "color": "#1a5a6b"},
    {"id": "00980A", "name": "野村臺灣優選",   "manager": "野村投信", "color": "#1a6b3a"},
    {"id": "00984A", "name": "安聯台灣高息",   "manager": "安聯投信", "color": "#6b4a1a"},
]

# 各ETF在公開資訊觀測站的基金代號
# 查詢方式：mops.twse.com.tw → 基金 → 主動式ETF → 每日投資組合
MOPS_FUND_ID = {
    "00982A": "00982A",
    "00981A": "00981A",
    "00985A": "00985A",
    "00980A": "00980A",
    "00984A": "00984A",
}

def tofloat(s):
    try: return float(re.sub(r"[,，%\s]", "", str(s)))
    except: return 0.0

def toint(s):
    try: return int(re.sub(r"[,，\s]", "", str(s)))
    except: return 0

def is_code(s):
    return bool(re.match(r"^\d{4,6}[A-Za-z]?$", str(s).strip()))

def fetch(url, sess, timeout=30, extra_headers=None):
    h = dict(HEADERS)
    if extra_headers:
        h.update(extra_headers)
    for i in range(3):
        try:
            r = sess.get(url, headers=h, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            log.info(f"    {r.status_code} {url[:80]}")
            return r.text
        except Exception as e:
            log.warning(f"    [{i+1}/3] {e}")
            time.sleep(4 * (i + 1))
    return None

def parse_html(html):
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


# ═══════════════════════════════════════════════════════
# 主要來源：公開資訊觀測站 mops.twse.com.tw
# ═══════════════════════════════════════════════════════

def crawl_mops(sess, eid):
    """
    公開資訊觀測站查詢主動式ETF每日投資組合。
    URL: https://mops.twse.com.tw/mops/web/t147sb01
    POST 參數帶入基金代號，取得當日持股。
    """
    log.info(f"  [{eid}] 公開資訊觀測站...")

    # 方法一：MOPS 主動式ETF每日投資組合查詢（POST）
    url = "https://mops.twse.com.tw/mops/web/t147sb01"
    today = datetime.now(TW)
    # 民國年
    roc_year = today.year - 1911
    date_str = f"{roc_year}/{today.month:02d}/{today.day:02d}"

    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "keyword4": "",
        "code1": "",
        "TYPEK": "all",
        "co_id": eid,
        "date": date_str,
    }

    try:
        r = sess.post(url, data=payload, headers=HEADERS, timeout=30)
        r.encoding = r.apparent_encoding or "utf-8"
        log.info(f"    POST {url} → {r.status_code}")
        result = parse_html(r.text)
        if result:
            log.info(f"  [{eid}] OK MOPS {len(result)}檔")
            return result
    except Exception as e:
        log.warning(f"    MOPS POST 失敗: {e}")

    # 方法二：MOPS 另一個端點（GET）
    urls_get = [
        f"https://mops.twse.com.tw/mops/web/ajax_t147sb01?co_id={eid}",
        f"https://mopsplus.twse.com.tw/mops/web/t147sb01?co_id={eid}",
    ]
    for u in urls_get:
        html = fetch(u, sess)
        if html:
            result = parse_html(html)
            if result:
                log.info(f"  [{eid}] OK MOPS GET {len(result)}檔")
                return result

    return []


# ═══════════════════════════════════════════════════════
# 備援：群益官網（靜態HTML，較容易抓）
# ═══════════════════════════════════════════════════════

def crawl_00982A_direct(sess):
    """群益官網直接抓（靜態HTML）"""
    url = "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    log.info(f"  [00982A] 群益官網直接...")
    # 先取首頁建立 cookie
    sess.get("https://www.capitalfund.com.tw/etf", headers=HEADERS, timeout=15)
    time.sleep(2)
    html = fetch(url, sess, extra_headers={
        "Referer": "https://www.capitalfund.com.tw/etf/product/overview"
    })
    if html:
        r = parse_html(html)
        if r:
            log.info(f"  [00982A] OK 群益直接 {len(r)}檔")
            return r
    return []


# ═══════════════════════════════════════════════════════
# 各ETF爬蟲入口
# ═══════════════════════════════════════════════════════

def crawl(eid, sess):
    # 先試公開資訊觀測站
    result = crawl_mops(sess, eid)
    if result:
        return result

    # 群益有靜態頁面，額外備援
    if eid == "00982A":
        result = crawl_00982A_direct(sess)
        if result:
            return result

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

def run():
    now = datetime.now(TW)
    log.info("=" * 55)
    log.info(f"台股主動ETF爬蟲 v4  {now:%Y-%m-%d %H:%M} TW")
    log.info("主要來源：公開資訊觀測站 mops.twse.com.tw")
    log.info("=" * 55)

    # if now.weekday() >= 5:
    #    log.info("非交易日，跳過。"); return

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
            today_h = crawl(eid, sess)
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
        time.sleep(2)

    (DATA / "etf_data.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DATA / "last_update.txt").write_text(now.isoformat())

    ok  = [k for k, v in output["etfs"].items() if v["success"]]
    err = [k for k, v in output["etfs"].items() if not v["success"]]
    log.info(f"\n{'='*55}")
    log.info(f"完成！成功:{len(ok)} {ok}")
    if err:
        log.warning(f"失敗:{len(err)} {err}")
        log.warning("→ 請用網頁儀表板的手動匯入功能補齊")
    log.info("=" * 55)

if __name__ == "__main__":
    run()
