#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股主動ETF 每日持股爬蟲 v5
- 每天執行，交易日抓新資料
- 自動保留過去 60 天歷史快照
- 輸出 etf_data.json（今日）+ history_index.json（歷史清單）
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

KEEP_DAYS = 60  # 保留幾天歷史

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

def crawl_mops(sess, eid):
    log.info(f"  [{eid}] 公開資訊觀測站...")
    url = "https://mops.twse.com.tw/mops/web/t147sb01"
    today = datetime.now(TW)
    roc_year = today.year - 1911
    date_str = f"{roc_year}/{today.month:02d}/{today.day:02d}"
    payload = {
        "encodeURIComponent": "1", "step": "1", "firstin": "1",
        "off": "1", "keyword4": "", "code1": "", "TYPEK": "all",
        "co_id": eid, "date": date_str,
    }
    try:
        r = sess.post(url, data=payload, headers=HEADERS, timeout=30)
        r.encoding = r.apparent_encoding or "utf-8"
        result = parse_html(r.text)
        if result:
            log.info(f"  [{eid}] OK MOPS POST {len(result)}檔")
            return result
    except Exception as e:
        log.warning(f"    MOPS POST: {e}")
    for u in [
        f"https://mops.twse.com.tw/mops/web/ajax_t147sb01?co_id={eid}",
        f"https://mopsplus.twse.com.tw/mops/web/t147sb01?co_id={eid}",
    ]:
        html = fetch(u, sess)
        if html:
            result = parse_html(html)
            if result:
                log.info(f"  [{eid}] OK MOPS GET {len(result)}檔")
                return result
    return []

def crawl_00982A_direct(sess):
    url = "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    sess.get("https://www.capitalfund.com.tw/etf", headers=HEADERS, timeout=15)
    time.sleep(2)
    html = fetch(url, sess, extra_headers={"Referer": "https://www.capitalfund.com.tw/etf/product/overview"})
    if html:
        r = parse_html(html)
        if r:
            log.info(f"  [00982A] OK 群益直接 {len(r)}檔")
            return r
    return []

def crawl(eid, sess):
    result = crawl_mops(sess, eid)
    if result: return result
    if eid == "00982A":
        result = crawl_00982A_direct(sess)
        if result: return result
    log.warning(f"  [{eid}] 全部失敗")
    return []

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

def load_snap(eid, date_str):
    p = HIST / f"{eid}_{date_str}.json"
    if not p.exists(): return []
    try: return json.loads(p.read_text(encoding="utf-8")).get("holdings", [])
    except: return []

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

def cleanup_old_snaps():
    """刪除超過 KEEP_DAYS 天的舊快照"""
    cutoff = datetime.now(TW) - timedelta(days=KEEP_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    for p in HIST.glob("*.json"):
        # 檔名格式: 00982A_2026-04-10.json
        parts = p.stem.split("_")
        if len(parts) >= 2:
            date_part = parts[-1]
            if date_part < cutoff_str:
                p.unlink()
                log.info(f"  清除舊快照: {p.name}")

def build_history_index():
    """
    掃描 history/ 資料夾，建立歷史索引。
    輸出 data/history_index.json，格式：
    {
      "dates": ["2026-04-10", "2026-04-09", ...],  # 有資料的日期（降序）
      "etfs": {
        "00982A": {
          "2026-04-10": { "count": 50, "success": true },
          ...
        }
      }
    }
    """
    index = {"dates": set(), "etfs": {e["id"]: {} for e in ETF_CONFIG}}

    for p in HIST.glob("*.json"):
        parts = p.stem.split("_")
        if len(parts) < 2: continue
        date_part = "_".join(parts[1:])   # 2026-04-10
        eid_part = parts[0]               # 00982A
        if eid_part not in index["etfs"]: continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            holdings = data.get("holdings", [])
            index["etfs"][eid_part][date_part] = {
                "count": len(holdings),
                "success": len(holdings) > 0,
            }
            if holdings:
                index["dates"].add(date_part)
        except: pass

    index["dates"] = sorted(index["dates"], reverse=True)
    return index

def build_daily_json(date_str, sess=None):
    """
    建立某一天的完整 JSON（含比對前一天的異動）
    回傳 output dict
    """
    now = datetime.now(TW)
    output = {
        "generated_at": now.isoformat(),
        "date": date_str,
        "etf_list": ETF_CONFIG,
        "etfs": {},
    }
    for cfg in ETF_CONFIG:
        eid = cfg["id"]
        today_h = load_snap(eid, date_str)

        # 找前一個交易日的快照
        snaps = sorted(HIST.glob(f"{eid}_*.json"))
        yest_h = []
        for p in reversed(snaps):
            d = "_".join(p.stem.split("_")[1:])
            if d < date_str:
                try:
                    yest_h = json.loads(p.read_text(encoding="utf-8")).get("holdings", [])
                    break
                except: pass

        changes = compare(today_h, yest_h) if today_h and yest_h else {}
        output["etfs"][eid] = {
            "meta": cfg, "date": date_str,
            "holdings": today_h, "yesterday": yest_h,
            "changes": changes, "count": len(today_h),
            "success": bool(today_h),
        }
    return output

def run():
    now = datetime.now(TW)
    log.info("=" * 55)
    log.info(f"台股主動ETF爬蟲 v5  {now:%Y-%m-%d %H:%M} TW")
    log.info("=" * 55)

    date_str = now.strftime("%Y-%m-%d")
    is_trading_day = now.weekday() < 5  # 週一到週五

    if is_trading_day:
        log.info("交易日 → 抓取今日持股")
        sess = requests.Session()
        for cfg in ETF_CONFIG:
            eid = cfg["id"]
            log.info(f"\n{'─'*40}\n[{eid}] {cfg['name']}")
            today_h = []
            try:
                today_h = crawl(eid, sess)
            except Exception as e:
                log.error(f"  [{eid}] 例外: {e}")
            if today_h:
                save_snap(eid, today_h, date_str)
            log.info(f"  [{eid}] {'✓' if today_h else '✗'} {len(today_h)}檔")
            time.sleep(2)
        cleanup_old_snaps()
    else:
        log.info("非交易日（週六/日）→ 跳過抓取，僅更新索引")

    # 無論交易日或假日，都重建今日 JSON 和歷史索引
    output = build_daily_json(date_str)
    (DATA / "etf_data.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 歷史索引（讓網頁知道哪些日期有資料）
    hist_index = build_history_index()
    (DATA / "history_index.json").write_text(
        json.dumps(hist_index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    (DATA / "last_update.txt").write_text(now.isoformat())

    ok  = [k for k, v in output["etfs"].items() if v["success"]]
    err = [k for k, v in output["etfs"].items() if not v["success"]]
    log.info(f"\n{'='*55}")
    log.info(f"完成！今日成功:{len(ok)} {ok}")
    log.info(f"歷史日期數:{len(hist_index['dates'])} 筆")
    if err and is_trading_day:
        log.warning(f"失敗:{len(err)} {err}")
    log.info("=" * 55)

if __name__ == "__main__":
    run()
