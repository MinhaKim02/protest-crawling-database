#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPATIC 집회·통제정보 크롤러 (Selenium 목록 기반 mgrSeq 선택 + VWorld 지오코딩, 맥락 스코어링+BBOX 강화)
- 결과 CSV 스키마(문자열; 리스트는 JSON 문자열):
  ['년','월','일','start_time','end_time','장소','인원','위도','경도','비고']

의존:
  - requests, beautifulsoup4, selenium

사용 예:
  python spatic_crawler.py --out 집회_정보.csv --debug --vworld-key YOUR_VWORLD_KEY
  # BBOX 가중치(종로+중구 느슨): 기본값
  python spatic_crawler.py --bbox jj_loose --bbox-mode boost
  # BBOX 엄격 필터(종로+중구 타이트):
  python spatic_crawler.py --bbox jj_tight --bbox-mode strict
"""

import re
import csv
import json
import html
import time
import argparse
from typing import List, Dict, Tuple, Optional
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString
from bs4 import FeatureNotFound

# --- Selenium(목록 선택 전용) -----------------------------------------
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

DETAIL_URL_FMT = "https://www.spatic.go.kr/spatic/assem/getInfoView.do?mgrSeq={mgrSeq}"
LIST_URL       = "https://www.spatic.go.kr/spatic/main/assem.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# ─────────────── VWorld 설정 ───────────────
DEFAULT_VWORLD_KEY = "46AEEE06-EE1D-3C1F-A4A4-E38D578695E8"
VWORLD_SEARCH_URL = "https://api.vworld.kr/req/search"
VWORLD_ADDR_URL   = "https://api.vworld.kr/req/address"

# ─────────────── BBOX(좌표 범위) 설정 ───────────────
# (lat_min, lat_max, lon_min, lon_max)
BBOXES: Dict[str, Tuple[float, float, float, float]] = {
    "seoul":    (37.38, 37.72, 126.76, 127.18),  # 서울 대략값
    "jj_loose": (37.53, 37.61, 126.95, 127.03),  # 종로+중구 느슨
    "jj_tight": (37.55, 37.60, 126.96, 127.02),  # 종로+중구 타이트
}
def _in_bbox(lat: float, lon: float, bbox: Tuple[float, float, float, float]) -> bool:
    try:
        lat_min, lat_max, lon_min, lon_max = bbox
        return (lat_min <= float(lat) <= lat_max) and (lon_min <= float(lon) <= lon_max)
    except Exception:
        return False

# ─────────────── 공통 유틸 ───────────────
def normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t\u00A0]+", " ", s).strip()

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = s.replace("\u00ad", "")  # soft hyphen
    s = s.replace("\u00A0", " ") # &nbsp;
    s = s.replace("∼", "~").replace("–", "-").replace("—", "-")
    s = normalize_spaces(s)
    return s

def soup_preprocess(soup: BeautifulSoup):
    for br in soup.find_all("br"):
        br.replace_with(NavigableString("\n"))

# ---------------- 요청(세션/쿠키/헤더) ----------------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(LIST_URL, timeout=15)  # 쿠키 확보
    except requests.RequestException:
        pass
    return s

def fetch_html(url: str, session: Optional[requests.Session] = None) -> str:
    sess = session or build_session()
    headers = HEADERS | {"Referer": LIST_URL}
    r = sess.get(url, headers=headers, timeout=20, allow_redirects=True)
    r.raise_for_status()
    return r.text

# ---------------- 목록 → 게시글 추출 (Selenium 전용) ----------------
def _is_event_post(title: str) -> bool:
    if not title:
        return False
    t = re.sub(r"\s+", " ", title).strip()
    if "행사 및 집회" in t:
        return True
    return ("행사" in t) and ("집회" in t)

def _to_int_or_none(s: str) -> Optional[int]:
    if s is None: return None
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else None

def convert_json_to_posts(json_data: List[dict]) -> List[Dict]:
    posts = []
    for item in json_data:
        if not isinstance(item, dict): continue
        post = {
            "number": str(item.get("mgrSeq", item.get("id", item.get("seq", "")))),
            "title": item.get("title", item.get("subject", "")),
            "date":  item.get("regDt", item.get("regDate", item.get("date", ""))),
            "views": str(item.get("hitCnt", item.get("viewCnt", item.get("views", "0")))),
            "is_new": item.get("newYn", "N") == "Y",
        }
        if post["number"]:
            posts.append(post)
    return posts

def setup_selenium_driver_for_list(headless: bool=True):
    if not SELENIUM_AVAILABLE:
        return None
    try:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=" + HEADERS["User-Agent"])
        if headless:
            options.add_argument("--headless=new")
        service = ChromeService()  # Selenium Manager
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception:
        return None

def get_posts_with_selenium_from_list(debug: bool=False) -> List[Dict]:
    driver = setup_selenium_driver_for_list(headless=True)
    if not driver:
        return []
    try:
        driver.get(LIST_URL)
        posts: List[Dict] = []
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.assem_content tr"))
            )
            tbody = driver.find_element(By.CSS_SELECTOR, "tbody.assem_content")
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                tds = row.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 4:
                    number = row.get_attribute("key") or tds[0].text.strip()
                    title  = tds[1].text.strip()
                    date_  = tds[2].text.strip()
                    views  = tds[3].text.strip()
                    if number:
                        posts.append({"number": number, "title": title, "date": date_, "views": views, "is_new": False})
        except Exception:
            pass

        # 테이블 실패 시 <script> JSON 추출
        if not posts:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            scripts = soup.find_all("script")
            for script in scripts:
                if not script.string: continue
                for pat in [
                    r'var\s+\w*[Ll]ist\w*\s*=\s*(\[.*?\]);',
                    r'\w*[Dd]ata\w*\s*=\s*(\[.*?\]);',
                    r'resultList["\']?\s*:\s*(\[.*?\])',
                    r'(\[{[^}]*["\']mgrSeq["\'][^}]*}[^]]*\])'
                ]:
                    m = re.search(pat, script.string, re.DOTALL)
                    if m:
                        try:
                            data = json.loads(m.group(1))
                            posts = convert_json_to_posts(data)
                            break
                        except Exception:
                            continue
                if posts: break

        if debug:
            print(f"[디버그] Selenium 목록 추출: {len(posts)}개")
        return posts
    finally:
        try: driver.quit()
        except Exception: pass

def get_list_posts(debug: bool=False) -> List[Dict]:
    return get_posts_with_selenium_from_list(debug=debug)

def filter_event_posts(posts: List[Dict], limit: int = 10) -> List[Dict]:
    head = posts[:limit] if posts else []
    return [p for p in head if _is_event_post(p.get("title", ""))]

def select_highest_post(posts: List[Dict]) -> Optional[Dict]:
    best = None; best_val = None
    for p in posts:
        iv = _to_int_or_none(p.get("number"))
        if iv is None: continue
        if best is None or iv > best_val:
            best, best_val = p, iv
    return best

# ---------------- 목록 날짜 → 년/월/일 파싱 ----------------
def parse_list_date_to_ymd(date_text: str) -> Tuple[str, str, str]:
    if not date_text: return "", "", ""
    s = clean_text(date_text)
    m = re.search(r'(\d{4})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})', s)
    if not m: return "", "", ""
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return y, f"{int(mo):02d}", f"{int(d):02d}"

# ---------------- 상세 파싱 ----------------
TIME_PAT_CELL = re.compile(
    r"(?<!\d)(\d{1,2})\s*:\s*(\d{2})(?:\s*[~∼\-]\s*(\d{1,2})\s*:\s*(\d{2}))?"
)

def best_table_by_content(soup: BeautifulSoup, debug: bool=False) -> Optional[BeautifulSoup]:
    best = None; best_score = (-1, -1)
    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 2: continue
        time_hits = 0; long_hits = 0
        sample_rows = trs[1: min(len(trs), 12)]
        for tr in sample_rows:
            cells = tr.find_all(["td", "th"]);
            if not cells: continue
            row_txts = [clean_text(c.get_text("\n")) for c in cells]
            if any(TIME_PAT_CELL.search(t) for t in row_txts): time_hits += 1
            if any(len(t) >= 10 for t in row_txts): long_hits += 1
        score = (time_hits, long_hits)
        if score > best_score:
            best_score = score; best = table
    if debug and best:
        print(f"[디버그] best_table_by_content 점수: time_hits={best_score[0]}, long_hits={best_score[1]}")
    return best

def detect_columns(table: BeautifulSoup, debug: bool=False) -> Tuple[int,int,Optional[int],int]:
    trs = table.find_all("tr")
    header_rows_used = 1
    for hdr_rows in [1, 2]:
        if len(trs) < hdr_rows + 1: break
        data_cells = trs[hdr_rows].find_all(["td", "th"])
        ncols = len(data_cells) if data_cells else 0
        if ncols == 0: continue
        col_texts = [[] for _ in range(ncols)]
        sample_rows = trs[hdr_rows: min(len(trs), hdr_rows + 15)]
        for tr in sample_rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) != ncols: continue
            for j, c in enumerate(cells):
                col_texts[j].append(clean_text(c.get_text("\n")))
        time_counts = [sum(1 for t in col_texts[j] if TIME_PAT_CELL.search(t)) for j in range(ncols)]
        avg_len     = [ (sum(len(t) for t in col_texts[j]) / max(1, len(col_texts[j]))) for j in range(ncols)]
        col_time  = max(range(ncols), key=lambda j: time_counts[j]) if ncols else 1
        candidates = [j for j in range(ncols) if j != col_time]
        col_place = (max(candidates, key=lambda j: avg_len[j]) if candidates else (0 if col_time != 0 else 1))
        col_route = None
        header_rows_used = hdr_rows
        if debug:
            print(f"[디버그] 헤더/내용 탐지: ncols={ncols}, time_counts={time_counts}, avg_len={[round(x,1) for x in avg_len]}")
            print(f"[디버그] col_time={col_time}, col_place={col_place}, col_route={col_route}")
        return col_time, col_place, col_route, header_rows_used

    data_start = 1 if len(trs) > 1 else 0
    if len(trs) <= data_start:
        return 1, 2, None, data_start
    ncols = len(trs[data_start].find_all(["td", "th"]))
    col_texts = [[] for _ in range(ncols)]
    sample_rows = trs[data_start: min(len(trs), data_start + 15)]
    for tr in sample_rows:
        cells = tr.find_all(["td", "th"])
        if len(cells) != ncols: continue
        for j, c in enumerate(cells):
            col_texts[j].append(clean_text(c.get_text("\n")))
    time_counts = [sum(1 for t in col_texts[j] if TIME_PAT_CELL.search(t)) for j in range(ncols)]
    avg_len     = [ (sum(len(t) for t in col_texts[j]) / max(1, len(col_texts[j]))) for j in range(ncols)]
    col_time  = max(range(ncols), key=lambda j: time_counts[j]) if ncols else 1
    candidates = [j for j in range(ncols) if j != col_time]
    col_place = (max(candidates, key=lambda j: avg_len[j]) if candidates else (0 if col_time != 0 else 1))
    col_route = None
    if debug:
        print(f"[디버그] 내용기반 탐지: ncols={ncols}, time_counts={time_counts}, avg_len={[round(x,1) for x in avg_len]}")
        print(f"[디버그] col_time={col_time}, col_place={col_place}")
    return col_time, col_place, col_route, data_start

def split_location_and_route(cell_text: str) -> Tuple[str, str]:
    txt = clean_text(cell_text)
    if not txt: return "", ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    if not lines: return txt, ""
    place_lines: List[str] = []; route_lines: List[str] = []
    for ln in lines:
        if ln.startswith("※") and ("행진" in ln or "이동" in ln):
            route_lines.append(re.sub(r"^※\s*(행진|이동)\s*:\s*", "", ln))
        else:
            place_lines.append(ln)
    tmp_place = []
    for ln in place_lines:
        if any(ch in ln for ch in ["→","↔","⇄"]):
            route_lines.append(ln)
        else:
            tmp_place.append(ln)
    place = normalize_spaces(" ".join(tmp_place)) if tmp_place else (lines[0] if lines else txt)
    route = normalize_spaces(" ".join(route_lines))
    return place, route

def parse_any(soup: BeautifulSoup, debug: bool=False) -> List[Dict]:
    soup_preprocess(soup)
    table = best_table_by_content(soup, debug=debug)
    if table:
        rows = parse_table_rows(table, debug=debug)
        if rows: return rows
    container = soup.select_one("#hwpEditorBoardContent, .detail_contents, .notice_contents, #contents, #container, .content")
    text = container.get_text("\n") if container else soup.get_text("\n")
    text = clean_text(text)
    rows: List[Dict] = []
    for m in TIME_PAT_CELL.finditer(text):
        pos = m.start()
        end = next((n.start() for n in TIME_PAT_CELL.finditer(text, pos+1)), len(text))
        body = text[pos:end]
        lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
        rem = [ln for ln in lines if not TIME_PAT_CELL.search(ln)]
        place, route = split_location_and_route("\n".join(rem)) if rem else ("","")
        rows.append({"시간": clean_text(m.group()), "장소": place, "행진경로": route})
    return rows

def parse_table_rows(table: BeautifulSoup, debug: bool=False) -> List[Dict]:
    trs = table.find_all("tr")
    if len(trs) < 2: return []
    col_time, col_place, _col_route, header_rows_used = detect_columns(table, debug=debug)
    out: List[Dict] = []
    for tr in trs[header_rows_used:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= max(col_time, col_place): continue
        t_raw = clean_text(cells[col_time].get_text("\n"))
        p_raw = clean_text(cells[col_place].get_text("\n"))
        place, route = split_location_and_route(p_raw)
        if not (t_raw or place or route): continue
        out.append({"시간": t_raw, "장소": place, "행진경로": route})
    return out

def crawl(url: str, debug: bool=False, debug_html_path: Optional[Path]=None) -> List[Dict]:
    try:
        html_text = fetch_html(url)
    except requests.RequestException as e:
        if debug: print(f"[디버그] HTTP 오류: {e}")
        return []
    if debug_html_path:
        try:
            debug_html_path.parent.mkdir(parents=True, exist_ok=True)
            debug_html_path.write_text(html_text, encoding="utf-8")
            if debug: print(f"[디버그] 원본 HTML 저장: {debug_html_path}")
        except Exception as e:
            if debug: print(f"[디버그] HTML 저장 실패: {e}")

    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html_text, parser)
            rows = parse_any(soup, debug=debug)
            if rows: return rows
        except FeatureNotFound:
            continue
        except Exception as e:
            if debug: print(f"[디버그] 파싱 실패(parser={parser}): {e}")
            continue
    return []

# ---------------- 필터/그룹/CSV ----------------
JONGNO_KEYWORDS = [
    "종로구", "광화문", "광화문광장", "경복궁", "안국", "안국역", "인사동",
    "종각", "종로", "세종문화회관", "정부서울청사", "교보빌딩", "정곡빌딩",
    "사직로", "율곡로", "자하문로", "청와대로", "수송동", "신문로", "서린로"
]

def is_jongno_related(place: str, route: str) -> bool:
    blob = f"{place}\n{route}".lower()
    return any(kw.lower() in blob for kw in JONGNO_KEYWORDS)

def filter_for_jongno(rows: List[Dict]) -> List[Dict]:
    return [r for r in rows if is_jongno_related(r.get("장소",""), r.get("행진경로",""))]

def parse_time_range(t: str) -> Tuple[str, str]:
    t = clean_text(t)
    m = TIME_PAT_CELL.search(t)
    if not m: return "", ""
    sh, sm, eh, em = m.group(1), m.group(2), m.group(3), m.group(4)
    start = f"{int(sh):02d}:{int(sm):02d}"
    end = f"{int(eh):02d}:{int(em):02d}" if (eh and em) else ""
    return start, end

def unique_preserve_order(xs: List[str]) -> List[str]:
    seen, out = set(), []
    for x in xs:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def group_rows_by_time(rows: List[Dict]) -> List[Dict]:
    groups: Dict[Tuple[str,str], Dict] = {}
    for r in rows:
        start, end = parse_time_range(r.get("시간",""))
        place = clean_text(r.get("장소",""))
        remark = clean_text(r.get("행진경로",""))
        key = (start, end)
        if key not in groups:
            groups[key] = {"start_time": start, "end_time": end, "places": [], "remarks": []}
        if place:  groups[key]["places"].append(place)
        if remark: groups[key]["remarks"].append(remark)
    out: List[Dict] = []
    for g in groups.values():
        g["places"] = unique_preserve_order([p for p in g["places"] if p])
        g["remarks"] = unique_preserve_order([rk for rk in g["remarks"] if rk])
        out.append(g)
    out.sort(key=lambda g: (g["start_time"] or "99:99", g["end_time"] or "99:99"))
    return out

# ─────────────── 지오코딩 유틸(정규화/후보/컨텍스트) ───────────────
GU_PATTERN = re.compile(r"(종로구|중구|용산구|성동구|광진구|동대문구|중랑구|성북구|강북구|도봉구|노원구|은평구|서대문구|마포구|양천구|강서구|구로구|금천구|영등포구|동작구|관악구|서초구|강남구|송파구|강동구)")
POLICE_TO_GU = {
    "종로서": "종로구", "남대문서": "중구", "중부서": "중구", "용산서": "용산구",
    "서대문서": "서대문구", "마포서": "마포구", "영등포서": "영등포구", "동작서": "동작구",
    "관악서": "관악구", "금천서": "금천구", "구로서": "구로구", "강서서": "강서구",
    "양천서": "양천구", "강남서": "강남구", "서초서": "서초구", "송파서": "송파구",
    "강동서": "강동구", "동대문서": "동대문구", "성북서": "성북구", "노원서": "노원구",
    "도봉서": "도봉구", "강북서": "강북구", "성동서": "성동구", "광진서": "광진구", "은평서": "은평구",
}
def extract_gu(text: str) -> Optional[str]:
    if not text: return None
    m = GU_PATTERN.search(text)
    if m: return m.group(1)
    for k, gu in POLICE_TO_GU.items():
        if k in text: return gu
    return None

# 장소 정규화
def _to_ascii_digits(s: str) -> str:
    mapping = {ord('０'):'0',ord('１'):'1',ord('２'):'2',ord('３'):'3',ord('４'):'4',
               ord('５'):'5',ord('６'):'6',ord('７'):'7',ord('８'):'8',ord('９'):'9',ord('〇'):'0'}
    return s.translate(mapping)

def _insert_space_between_kor_engnum(s: str) -> str:
    s = re.sub(r'([가-힣])([A-Za-z0-9])', r'\1 \2', s)
    s = re.sub(r'([A-Za-z0-9])([가-힣])', r'\1 \2', s)
    return s

def normalize_tokens_basic(place: str) -> str:
    t = (place or "").strip()
    t = _to_ascii_digits(t)
    t = t.replace("出口", "출구").replace("出", "출구").replace("口", "출구")
    t = _insert_space_between_kor_engnum(t)
    t = re.sub(r'(\d+)\s*(?:번)?\s*(?:출|출구)\b', r'\1번 출구', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t

# 노이즈/날개/알리아스/맥락
WING_TOKENS = ["본관","별관","서관","동관","남관","북관","정문","후문","본동","신관"]
NOISE_TOKENS = [
    "앞","옆","맞은편","방면","방향","인근","주변","부근","일대","일원","부지","앞쪽","건너편",
    "횡단보도","교차로","사거리","오거리","분수대","계단","광장내","광장 내","인도","보도",
    "1개차로","2개차로","3개차로","4개차로","5개차로","개차로","차로"
]
def strip_parentheses_and_noise(s: str) -> str:
    s = re.sub(r"[()\[\]{}〈〉＜＞]", " ", s)
    s = re.sub(r"\b\d+\s*개?\s*차로\b", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def split_core_and_wing(s: str) -> Tuple[str, List[str]]:
    tokens = s.split()
    core_parts, wings = [], []
    for tk in tokens:
        if tk in WING_TOKENS: wings.append(tk)
        elif tk in NOISE_TOKENS: continue
        else: core_parts.append(tk)
    core = " ".join(core_parts).strip()
    return core, wings

def expand_wing_synonyms(wings: List[str]) -> List[str]:
    out = set()
    for w in wings:
        out.add(w)
        if w == "남관": out.update(["남문","남측 출입문"])
        elif w == "북관": out.update(["북문","북측 출입문"])
        elif w == "동관": out.update(["동문","동측 출입문"])
        elif w == "서관": out.update(["서문","서측 출입문"])
        elif w == "정문": out.update(["정문 출입구","정문 게이트"])
        elif w == "후문": out.update(["후문 출입구","후문 게이트"])
    return list(out)

def add_spacing_variants(name: str) -> List[str]:
    cands = {name}
    for kw in ["빌딩","타워","센터","플라자","문고","광장","사거리","교차로","주차장","별관"]:
        cands.add(name.replace(kw, f" {kw}"))
    return list(cands)

ALIAS_MAP: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"정\s*곡\s*빌\s*딩"), "정부서울청사 별관 정곡빌딩"),
    (re.compile(r"정부\s*서울\s*청사"), "정부서울청사"),
    (re.compile(r"교\s*보\s*빌\s*딩"), "교보생명빌딩 광화문"),
    (re.compile(r"교\s*보\s*문\s*고"), "교보문고 광화문점"),
    (re.compile(r"광\s*화\s*문\s*광\s*장"), "광화문광장"),
    (re.compile(r"세\s*종\s*문\s*화\s*회\s*관"), "세종문화회관"),
    (re.compile(r"경\s*복\s*궁"), "경복궁"),
    (re.compile(r"광\s*화\s*문\b"), "광화문"),
]
def apply_aliases(name: str) -> List[str]:
    outs = [name]
    for pat, rep in ALIAS_MAP:
        if pat.search(name):
            outs.append(rep)
    return unique_preserve_order([normalize_spaces(x) for x in outs if x])

CTX_PATTERNS = [
    r"[가-힣A-Za-z0-9]+(?:대로|로|길)\b",               # 세종대로, 사직로, 종로, 율곡로, 서린로 ...
    r"[가-힣A-Za-z0-9]+광장\b",                        # 광화문광장
    r"[가-힣A-Za-z0-9]+문\b",                          # 광화문
    r"[가-힣A-Za-z0-9]+궁\b",                          # 경복궁
    r"(?:정부서울청사|[가-힣A-Za-z0-9]+시청)\b",        # 정부서울청사, 시청
    r"[가-힣A-Za-z0-9]+역\b(?:\s*\d+\s*번\s*출구)?",     # 종각역 4번 출구
    r"[가-힣A-Za-z0-9]+사거리\b|[가-힣A-Za-z0-9]+교차로\b"
]
def extract_context_terms(text: str, limit: int = 3) -> List[str]:
    if not text: return []
    found = []
    for pat in CTX_PATTERNS:
        for m in re.finditer(pat, text):
            tok = normalize_spaces(m.group(0))
            if tok and tok not in found:
                found.append(tok)
    key_prio = ["광화문", "광화문광장", "세종대로", "사직로", "종로", "경복궁", "시청", "정부서울청사"]
    found.sort(key=lambda x: (0 if any(k in x for k in key_prio) else 1, len(x)))
    return found[:limit]

# 후보 질의 생성
def build_query_candidates(place: str, remarks_joined: str) -> Tuple[List[str], str, List[str]]:
    base0 = normalize_tokens_basic(place)
    base1 = strip_parentheses_and_noise(base0)

    core, wings = split_core_and_wing(base1)
    wing_syns = expand_wing_synonyms(wings)
    core_vars = add_spacing_variants(core) if core else []
    base_vars = add_spacing_variants(base1) if base1 else []

    alias_vars = []
    for nm in unique_preserve_order([core] + core_vars + [base1] + base_vars):
        alias_vars.extend(apply_aliases(nm))
    alias_vars = unique_preserve_order(alias_vars)

    gu = extract_gu(f"{place} {remarks_joined}")
    ctx_terms = extract_context_terms(remarks_joined)

    cand: List[str] = []
    seen = set()
    def add(q: str):
        q = normalize_spaces(q)
        if not q or q in seen: return
        seen.add(q); cand.append(q)

    # 1) 코어 우선
    for cv in (core_vars or []):
        add(cv)
        for w in wings: add(f"{cv} {w}")
        for w in wing_syns: add(f"{cv} {w}")

    # 2) 알리아스(강력) → 앞쪽
    for v in alias_vars:
        add(v)

    # 3) 베이스 변형
    if base1: add(base1)
    for v in base_vars: add(v)

    # 4) 맥락 결합 (강한 조합을 앞에)
    pref = []
    for q in list(cand):
        for ctx in ctx_terms:
            pref.append(f"{q} {ctx}")
    cand = unique_preserve_order(pref + cand)

    # 5) 서울/구 접두 (최우선)
    prefixes = []
    if gu: prefixes.append(f"서울 {gu}")
    prefixes.append("서울")
    front = []
    for pfx in prefixes:
        for q in cand[:20]:  # 상위 후보만 접두
            front.append(f"{pfx} {q}")

    final = unique_preserve_order(front + cand)[:50]
    return final, (core or ""), ctx_terms

# ----- 디버그용: VWorld 검색 결과 미리보기 -----
def _debug_preview_items(items: list, max_k: int = 3) -> str:
    outs = []
    for i, it in enumerate(items[:max_k], 1):
        title = str(it.get("title",""))
        addr  = f"{it.get('address','')} {it.get('road_address','')}".strip()
        pt    = it.get("point") or {}
        x, y  = pt.get("x"), pt.get("y")
        if (x is None or y is None) and isinstance((it.get("geometry") or {}).get("coordinates"), list):
            coords = it["geometry"]["coordinates"]
            if len(coords) >= 2:
                x, y = coords[0], coords[1]
        outs.append(f"  · #{i} title='{title}' / addr='{addr}' / (y,x)=({y},{x})")
    return "\n".join(outs) if outs else "  · (후보 없음)"

# PLACE 다중 페이지 조회
def _vworld_search_place(query: str, key: str, session: requests.Session, debug: bool=False) -> List[dict]:
    items_all = []
    for page in (1, 2):  # 1~2페이지만
        params = {
            "service": "search", "request": "search", "version": "2.0", "format": "json",
            "size": 30, "page": page, "type": "place", "key": key, "query": query
        }
        if debug:
            print(f"[VWorld PLACE] GET {VWORLD_SEARCH_URL} page={page} query='{query}'")
        r = session.get(VWORLD_SEARCH_URL, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        items = (((data or {}).get("response") or {}).get("result") or {}).get("items", [])
        if items:
            items_all.extend(items)
        if not items or len(items) < 30:
            break
    if debug:
        print(f"[VWorld PLACE]  → 후보 {len(items_all)}개\n{_debug_preview_items(items_all)}")
    return items_all

def _vworld_address_coord(addr: str, key: str, session: requests.Session, addr_type: str, debug: bool=False) -> Optional[Tuple[float, float]]:
    params = {"service":"address","request":"getCoord","version":"2.0","format":"json","crs":"EPSG:4326","type":addr_type,"address":addr,"key":key}
    if debug:
        print(f"[VWorld ADDR] GET {VWORLD_ADDR_URL} type={addr_type} addr='{addr}'")
    r = session.get(VWORLD_ADDR_URL, params=params, timeout=6)
    r.raise_for_status()
    data = r.json()
    res = (data.get("response") or {}).get("result")
    if isinstance(res, list):
        res = res[0] if res else None
    if not isinstance(res, dict):
        if debug: print("[VWorld ADDR]  → 결과 없음")
        return None
    pt = res.get("point") or {}
    x, y = pt.get("x"), pt.get("y")
    if x is None or y is None:
        if debug: print("[VWorld ADDR]  → 좌표 없음")
        return None
    if debug:
        print(f"[VWorld ADDR]  → (y,x)=({y},{x})")
    return (float(y), float(x))

# 스코어링 + BBOX 반영
def _choose_best_item(items: list, gu_hint: Optional[str], needle: Optional[str],
                      ctx_terms: List[str],
                      bbox: Optional[Tuple[float,float,float,float]] = None,
                      bbox_mode: str = "boost") -> Optional[dict]:
    if not items: return None

    def _norm(s: str) -> str:
        return re.sub(r"\s+", "", (s or "").lower())

    needle_n = _norm(needle or "")
    def core_match_score(title: str) -> int:
        if not needle_n: return 0
        t = _norm(title)
        if needle_n and needle_n == t:  # 완전 일치
            return 6
        if needle_n and needle_n in t:  # 부분 포함
            return 3
        return 0

    def get_xy(it: dict) -> Tuple[Optional[float], Optional[float]]:
        pt = it.get("point") or {}
        x, y = pt.get("x"), pt.get("y")
        if x is None or y is None:
            coords = ((it.get("geometry") or {}).get("coordinates"))
            if isinstance(coords, list) and len(coords) >= 2:
                x, y = coords[0], coords[1]
        return (float(x) if x is not None else None,
                float(y) if y is not None else None)

    def score(it: dict) -> int:
        s_all = json.dumps(it, ensure_ascii=False)
        s = s_all.lower()
        sc = 0
        if '서울' in s: sc += 3
        if gu_hint and gu_hint.lower() in s: sc += 4

        title = str(it.get('title',''))
        addr  = f"{it.get('address','')} {it.get('road_address','')}".lower()
        if "서울" in addr or "seoul" in addr: sc += 2
        else: sc -= 1

        sc += core_match_score(title)
        for ctx in ctx_terms:
            c = ctx.lower()
            if c in title.lower() or c in addr:
                sc += 2

        for k in ["광화문","세종대로","종로","정부서울청사","교보","세종문화회관","경복궁","안국","종각"]:
            if k in s: sc += 1

        # BBOX 가중치/페널티
        x, y = get_xy(it)
        if x is not None and y is not None and bbox:
            lat, lon = y, x
            inside = _in_bbox(lat, lon, bbox)
            if bbox_mode == "boost":
                sc += (5 if inside else -5)
            elif bbox_mode == "strict":
                sc += (100 if inside else -100)
        return sc

    ranked = sorted(items, key=score, reverse=True)

    # bbox 주어진 경우, 상위권에서 박스 내 첫 후보를 우선 리턴
    if bbox and bbox_mode in ("boost","strict"):
        for it in ranked[:20]:
            pt = it.get("point") or {}
            x, y = pt.get("x"), pt.get("y")
            if x is None or y is None:
                coords = ((it.get("geometry") or {}).get("coordinates"))
                if isinstance(coords, list) and len(coords) >= 2:
                    x, y = coords[0], coords[1]
            if x is not None and y is not None and _in_bbox(float(y), float(x), bbox):
                return it

    return ranked[0] if ranked else None

def geocode_vworld_candidates(queries: List[str], key: str, session: requests.Session,
                              gu_hint: Optional[str], needle: Optional[str],
                              ctx_terms: List[str],
                              bbox: Optional[Tuple[float,float,float,float]] = None,
                              bbox_mode: str = "boost",
                              debug: bool=False) -> Optional[Tuple[float,float,str]]:
    """
    질의 리스트를 순차 시도 → PLACE 스코어링 최적 후보 → 좌표 반환.
    실패시 ADDRESS(road→parcel) 시도. 성공 시 (lat,lon,사용쿼리) 반환.
    마지막으로 맥락 단독 재시도.
    """
    # 1) PLACE
    for q in queries:
        try:
            items = _vworld_search_place(q, key, session, debug=debug)
        except Exception as e:
            if debug: print(f"[VWorld PLACE] 오류: {e}")
            items = []
        if items:
            best = _choose_best_item(items, gu_hint, needle, ctx_terms, bbox=bbox, bbox_mode=bbox_mode) or items[0]
            pt = best.get("point") or {}
            x, y = pt.get("x"), pt.get("y")
            if x is None or y is None:
                coords = ((best.get("geometry") or {}).get("coordinates"))
                if isinstance(coords, list) and len(coords) >= 2:
                    x, y = coords[0], coords[1]
            if x is not None and y is not None:
                return (float(y), float(x), q)
        # 다음 후보로

    # 2) ADDRESS 보강 (bbox 검증 포함)
    addr_try = []
    for q in queries[:4]:
        if gu_hint:
            addr_try.append(f"서울 {gu_hint} {q}")
        addr_try.append(f"서울 {q}")
    addr_try += queries[:2]
    for a in addr_try:
        got = _vworld_address_coord(a, key, session, "road", debug=debug)
        if got:
            if (not bbox) or (bbox_mode == "boost" and _in_bbox(got[0], got[1], bbox)) or \
               (bbox_mode == "strict" and _in_bbox(got[0], got[1], bbox)):
                return (got[0], got[1], a)
        got = _vworld_address_coord(a, key, session, "parcel", debug=debug)
        if got:
            if (not bbox) or (bbox_mode == "boost" and _in_bbox(got[0], got[1], bbox)) or \
               (bbox_mode == "strict" and _in_bbox(got[0], got[1], bbox)):
                return (got[0], got[1], a)

    # 3) 최후 수단(맥락 단독)
    if ctx_terms:
        for ctx in ctx_terms:
            try:
                q2 = f"서울 {gu_hint or ''} {ctx}".strip()
                items = _vworld_search_place(q2, key, session, debug=debug)
            except Exception as e:
                if debug: print(f"[VWorld PLACE/CTX] 오류: {e}")
                items = []
            if items:
                best = _choose_best_item(items, gu_hint, needle, ctx_terms, bbox=bbox, bbox_mode=bbox_mode) or items[0]
                pt = best.get("point") or {}
                x, y = pt.get("x"), pt.get("y")
                if x is None or y is None:
                    coords = ((best.get("geometry") or {}).get("coordinates"))
                    if isinstance(coords, list) and len(coords) >= 2:
                        x, y = coords[0], coords[1]
                if x is not None and y is not None:
                    return (float(y), float(x), f"[CTX]{ctx}")

    return None

# --- 그룹 지오코딩 (강화판) ---
def geocode_grouped_inplace(grouped: List[Dict], vworld_key: str,
                            sleep_sec: float = 0.25,
                            bbox: Optional[Tuple[float,float,float,float]] = None,
                            bbox_mode: str = "boost",
                            debug: bool=False):
    session = requests.Session()
    cache: Dict[str, Optional[Tuple[float, float]]] = {}
    for g in grouped:
        places = g.get("places", []) or []
        remark_context = " | ".join(g.get("remarks", []) or [])
        gu_hint = extract_gu(f"{' '.join(places)} {remark_context}")
        lats: List[Optional[float]] = []; lons: List[Optional[float]] = []
        for p in places:
            queries, core_kw, ctx_terms = build_query_candidates(str(p or ""), remark_context)

            if debug:
                print("\n[지오코딩 시작]")
                print(f"  · 장소: '{p}'")
                print(f"  · 구 힌트: '{gu_hint or ''}', 코어: '{core_kw}', 맥락: {ctx_terms}")
                print(f"  · 질의 후보({len(queries)}): {queries[:12]}{' ...' if len(queries)>12 else ''}")

            hit = None; used_q = None
            # 캐시 우선
            for q in queries:
                if q in cache:
                    hit = cache[q]
                    if hit:
                        used_q = q
                        if debug:
                            print(f"[캐시적중] '{p}' → (lat,lon)={hit} via '{q}'")
                        break
            # API 호출
            if not hit:
                got = geocode_vworld_candidates(
                    queries, vworld_key, session,
                    gu_hint, core_kw or None, ctx_terms,
                    bbox=bbox, bbox_mode=bbox_mode, debug=debug
                )
                if got:
                    lat, lon, used_q = got
                    hit = (lat, lon)
                # 캐시 저장(앞쪽 후보 위주)
                for q in queries[:8]:
                    cache[q] = hit
                time.sleep(max(0.0, float(sleep_sec)))
            if debug:
                if hit: print(f"[지오코딩 성공] '{p}' → (lat,lon)={hit}  (사용질의='{used_q}')")
                else:   print(f"[지오코딩 실패] '{p}' (질의 {len(queries)}개 시도)")
            lats.append(hit[0] if hit else None)
            lons.append(hit[1] if hit else None)
        g["lats"] = lats; g["lons"] = lons

# ---------------- CSV 렌더링 ----------------
def records_to_csv_rows(ymd: Optional[Tuple[str, str, str]], grouped: List[Dict]) -> List[Dict]:
    if ymd is not None:
        Y, M, D = ymd
    else:
        Y = M = D = ""
    rows_csv: List[Dict] = []
    for g in grouped:
        places_json = json.dumps(g.get("places", []), ensure_ascii=False)
        lat_list = g.get("lats", []); lon_list = g.get("lons", [])
        lats_json = json.dumps(lat_list, ensure_ascii=False)
        lons_json = json.dumps(lon_list, ensure_ascii=False)
        remarks = " | ".join(g.get("remarks", []))
        rows_csv.append({
            "년": Y, "월": M, "일": D,
            "start_time": g.get("start_time",""),
            "end_time": g.get("end_time",""),
            "장소": places_json,
            "인원": "",
            "위도": lats_json,
            "경도": lons_json,
            "비고": remarks
        })
    return rows_csv

def save_csv_new_schema(records: List[Dict], out_path: str):
    fieldnames = ["년","월","일","start_time","end_time","장소","인원","위도","경도","비고"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

# ---------------- 메인 ----------------
def main():
    p = argparse.ArgumentParser(description="SPATIC 집회·통제정보 크롤러 (Selenium 목록 기반 / CSV / VWorld 지오코딩+BBOX 강화)")
    p.add_argument("--url", help="게시글 URL 직접 지정 (지정 시 mgrSeq/목록 무시)")
    p.add_argument("--mgr-seq", type=int, help="mgrSeq 직접 지정 (예: 1177)")
    p.add_argument("--out", default="집회_정보.csv", help="저장 CSV 경로 (기본: 집회_정보.csv)")
    p.add_argument("--debug", action="store_true", help="파싱/지오코딩 로그 출력")
    p.add_argument("--debug-html", action="store_true", help="원본 HTML을 debug/ 폴더에 저장")
    p.add_argument("--vworld-key", default=None, help="VWorld API Key (기본: 환경변수 VWORLD_KEY 또는 내장 기본키)")
    p.add_argument("--rate", type=float, default=0.25, help="지오코딩 호출 간 대기(초)")
    # BBOX 옵션
    p.add_argument("--bbox", choices=list(BBOXES.keys()), default="jj_loose",
                   help="후보 좌표 가중/필터에 사용할 BBOX (기본: jj_loose)")
    p.add_argument("--bbox-mode", choices=["boost","strict"], default="boost",
                   help="BBOX 적용 방식: boost=가중치, strict=박스 밖 후보 배제")
    args = p.parse_args()

    # 키 우선순위: --vworld-key > 환경변수 > DEFAULT_VWORLD_KEY
    try:
        env_key = __import__("os").environ.get("VWORLD_KEY")
    except Exception:
        env_key = None
    vworld_key = args.vworld_key or env_key or DEFAULT_VWORLD_KEY

    chosen_url: Optional[str] = None
    chosen_post: Optional[Dict] = None

    # 우선순위: --url > --mgr-seq > (Selenium) 목록 기반
    if args.url:
        chosen_url = args.url
        if args.debug: print(f"[정보] 요청 URL: {chosen_url} (URL 직접 지정)")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / "manual_url.html") if args.debug_html else None)
        ymd = None
    elif args.mgr_seq is not None:
        chosen_url = DETAIL_URL_FMT.format(mgrSeq=args.mgr_seq)
        if args.debug: print(f"[정보] 요청 URL: {chosen_url} (mgrSeq 직접 지정: {args.mgr_seq})")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / f"mgrseq_{args.mgr_seq}.html") if args.debug_html else None)
        ymd = None
    else:
        if not SELENIUM_AVAILABLE:
            print("[오류] Selenium을 사용할 수 없습니다. --mgr-seq 또는 --url 옵션을 지정해 주세요.")
            return
        posts = get_list_posts(debug=args.debug)
        if args.debug: print(f"[디버그] 목록 총 {len(posts)}건 수집")
        event_posts = filter_event_posts(posts, limit=10)
        if args.debug:
            for i, p_ in enumerate(event_posts, 1):
                print(f"[디버그] 이벤트[{i}] 번호={p_.get('number')} / 날짜={p_.get('date')} / 제목={p_.get('title')}")
        chosen_post = select_highest_post(event_posts)
        if not chosen_post:
            print("[오류] 목록 상위 10개에서 '행사 및 집회' 게시글을 찾지 못했습니다.")
            return
        chosen_url = DETAIL_URL_FMT.format(mgrSeq=chosen_post["number"])
        if args.debug:
            print(f"[정보] 목록 기반 선택 mgrSeq={chosen_post['number']} → {chosen_url} / 목록날짜='{chosen_post.get('date','')}'")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / f"list_{chosen_post['number']}.html") if args.debug_html else None)
        ymd = parse_list_date_to_ymd(chosen_post.get("date", ""))

    if not chosen_url:
        print("[오류] 유효한 게시글 URL을 결정하지 못했습니다.")
        return

    # 1) 종로 필터
    rows_filtered = filter_for_jongno(rows)
    # 2) 시간대 그룹핑
    grouped = group_rows_by_time(rows_filtered)

    # 3) 지오코딩
    bbox = BBOXES[args.bbox]
    bbox_mode = args.bbox_mode
    if vworld_key:
        if args.debug:
            tail = ('***' + vworld_key[-6:]) if len(vworld_key or '') > 6 else '***'
            print(f"[정보] VWorld 지오코딩 시행 (rate={args.rate}s, key={tail}, bbox={args.bbox}, mode={bbox_mode})")
        geocode_grouped_inplace(grouped, vworld_key, args.rate, bbox=bbox, bbox_mode=bbox_mode, debug=args.debug)
    else:
        if args.debug:
            print("[경고] VWorld 키가 없어 지오코딩을 건너뜁니다. --vworld-key 또는 환경변수 설정 필요.")

    # 4) CSV 저장
    save_dir = Path("data")
    save_dir.mkdir(parents=True, exist_ok=True)

    filename = Path(args.out).name   # 파일명만 추출
    out_path = save_dir / filename

    final_records = records_to_csv_rows(ymd, grouped)
    save_csv_new_schema(final_records, str(out_path))

    ymd_str = "-".join(ymd) if ymd and all(ymd) else ""
    print(f"[완료] {out_path} 저장 (총 {len(final_records)}건, 종로구 필터 적용 / 선택 URL={chosen_url}{' / 날짜=' + ymd_str if ymd_str else ''})")

if __name__ == "__main__":
    main()
